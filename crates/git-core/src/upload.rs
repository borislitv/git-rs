//! Authorized Git upload server built from maintained packet/object/pack codecs.
//! Transport ownership, authentication and the catalog lease stay in the adapter.

use crate::{
    CapacityBuffer, MAX_REF_BYTES, MAX_REFS, SnapshotSession, VerifiedObjectProvider,
    objects::{Result, invalid, metadata_header_end},
    storage::{ChargedVec, Set},
    write_pack::PackWriteLimits,
};
use gix_hash::ObjectId;
use gix_object::Kind;
use gix_packetline::{Channel, PacketLineRef, blocking_io::encode};
use std::io::{Read, Write};

mod shallow;

const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_ROUNDS: usize = 256;

/// Negotiated protocol version, never inferred from request body bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Version {
    V0,
    V1,
    V2,
}

impl Version {
    pub fn parse(header: &str) -> Result<Self> {
        match header {
            "" | "version=0" => Ok(Self::V0),
            "version=1" => Ok(Self::V1),
            "version=2" => Ok(Self::V2),
            _ => Err(invalid("unsupported Git upload protocol version")),
        }
    }
}

struct VisibleRef {
    name: CapacityBuffer,
    id: ObjectId,
    peeled: Option<ObjectId>,
}

/// A pinned authorized ref view. Arbitrary physical catalog objects do not become
/// legal wants. Have acknowledgments are restricted to its reachable commits.
pub struct UploadSession<P> {
    snapshot: SnapshotSession<P>,
    refs: ChargedVec<VisibleRef>,
    advertised: Set<ObjectId>,
    default_ref: CapacityBuffer,
    version: Version,
    visible_commits: Option<Set<ObjectId>>,
}

impl<P: VerifiedObjectProvider> UploadSession<P> {
    pub fn new<'a>(
        snapshot: SnapshotSession<P>,
        refs: impl IntoIterator<Item = (&'a [u8], ObjectId)>,
        default_ref: &[u8],
        version: Version,
    ) -> Result<Self> {
        if snapshot.identity().object_hash != gix_hash::Kind::Sha1 {
            return Err(invalid("upload server requires SHA-1 object format"));
        }
        validate_ref(default_ref)?;
        let mut visible = ChargedVec::new(snapshot.capacity.clone(), MAX_REFS)?;
        let mut advertised =
            Set::new(snapshot.capacity.clone(), snapshot.limits.max_graph_objects)?;
        let mut previous: Option<CapacityBuffer> = None;
        for (name, id) in refs {
            snapshot.control.check()?;
            validate_ref(name)?;
            if previous
                .as_ref()
                .is_some_and(|previous| previous.as_slice() >= name)
            {
                return Err(invalid("upload refs must have unique byte-sorted names"));
            }
            if name.starts_with(b"refs/heads/")
                && snapshot
                    .header(&id)?
                    .is_none_or(|header| header.kind != Kind::Commit)
            {
                return Err(invalid("upload branch target must be a physical commit"));
            }
            let peeled = peel(&snapshot, id)?;
            advertised.insert(id, &snapshot.control)?;
            if let Some(peeled) = peeled {
                advertised.insert(peeled, &snapshot.control)?;
            }
            previous = Some(copy(&snapshot, name)?);
            visible.push(
                VisibleRef {
                    name: copy(&snapshot, name)?,
                    id,
                    peeled,
                },
                &snapshot.control,
            )?;
        }
        let default_ref = copy(&snapshot, default_ref)?;
        Ok(Self {
            snapshot,
            refs: visible,
            advertised,
            default_ref,
            version,
            visible_commits: None,
        })
    }

    pub fn advertise(&self, smart_http: bool, output: &mut impl Write) -> Result<()> {
        self.snapshot.control.check()?;
        if self.version == Version::V2 {
            for line in [
                b"version 2\n".as_slice(),
                b"agent=git-rs\n",
                b"ls-refs=unborn\n",
                b"fetch=shallow wait-for-done\n",
                b"object-format=sha1\n",
            ] {
                encode::data_to_write(line, &mut *output)?;
            }
        } else {
            if smart_http {
                encode::data_to_write(b"# service=git-upload-pack\n", &mut *output)?;
                encode::flush_to_write(&mut *output)?;
            }
            if self.version == Version::V1 {
                encode::data_to_write(b"version 1\n", &mut *output)?;
            }
            let mut first = true;
            if let Some(head) = self.head() {
                self.advertise_ref(b"HEAD", head.id, &mut first, output)?;
            }
            for reference in self.refs.as_slice() {
                self.snapshot.control.check()?;
                self.advertise_ref(reference.name.as_slice(), reference.id, &mut first, output)?;
                if let Some(peeled) = reference.peeled {
                    let mut name = [0u8; MAX_REF_BYTES + 3];
                    let len = reference.name.as_slice().len();
                    name[..len].copy_from_slice(reference.name.as_slice());
                    name[len..len + 3].copy_from_slice(b"^{}");
                    self.advertise_ref(&name[..len + 3], peeled, &mut first, output)?;
                }
            }
            if first {
                self.advertise_ref(
                    b"capabilities^{}",
                    ObjectId::null(self.snapshot.identity().object_hash),
                    &mut first,
                    output,
                )?;
            }
        }
        encode::flush_to_write(&mut *output)?;
        output.flush()?;
        self.snapshot.control.check()?;
        Ok(())
    }

    fn advertise_ref(
        &self,
        name: &[u8],
        id: ObjectId,
        first: &mut bool,
        output: &mut impl Write,
    ) -> Result<()> {
        let mut line = [0u8; 4096];
        let mut cursor = std::io::Cursor::new(line.as_mut_slice());
        write!(cursor, "{id} ")?;
        cursor.write_all(name)?;
        if *first {
            cursor.write_all(b"\0multi_ack multi_ack_detailed thin-pack side-band side-band-64k ofs-delta shallow deepen-relative deepen-since deepen-not no-progress include-tag object-format=sha1 agent=git-rs symref=HEAD:")?;
            cursor.write_all(self.default_ref.as_slice())?;
            *first = false;
        }
        cursor.write_all(b"\n")?;
        let len = cursor.position() as usize;
        encode::data_to_write(&line[..len], output)?;
        Ok(())
    }

    /// A stateless HTTP call consumes exactly one bounded request. SSH retains
    /// interactive negotiation and v2 command reuse; it never drains stdin EOF
    /// after sending a complete pack or receiving an explicit empty request.
    pub fn run(
        &mut self,
        input: &mut impl Read,
        output: &mut impl Write,
        stateless: bool,
    ) -> Result<()> {
        let mut wire = Wire {
            bytes: self.snapshot.capacity.buffer(65_520)?,
            consumed: 0,
        };
        if self.version == Version::V2 {
            for _ in 0..MAX_ROUNDS {
                if !self.run_v2(&mut wire, input, output, stateless)? {
                    return Ok(());
                }
                if stateless {
                    return Ok(());
                }
            }
            Err(invalid("upload command-round limit exceeded"))
        } else {
            self.run_v1(&mut wire, input, output, stateless)
        }
    }

    fn run_v2(
        &mut self,
        wire: &mut Wire,
        input: &mut impl Read,
        output: &mut impl Write,
        stateless: bool,
    ) -> Result<bool> {
        let Some(packet) = wire.next(&self.snapshot, input)? else {
            return Ok(false);
        };
        if packet == PacketLineRef::Flush {
            if stateless {
                require_end(input)?;
            }
            return Ok(false);
        }
        let command = match text(packet)? {
            b"command=ls-refs" => false,
            b"command=fetch" => true,
            _ => return Err(invalid("unsupported upload v2 command")),
        };
        let mut caps = 0u8;
        loop {
            let packet = wire.required(&self.snapshot, input)?;
            if packet == PacketLineRef::Delimiter {
                break;
            }
            let line = text(packet)?;
            let bit = if line == b"object-format=sha1" {
                1
            } else if line.starts_with(b"agent=") && valid_agent(&line[6..]) {
                2
            } else {
                return Err(invalid("unadvertised upload v2 capability"));
            };
            if caps & bit != 0 {
                return Err(invalid("duplicate upload v2 capability"));
            }
            caps |= bit;
        }
        let mut complete = false;
        if !command {
            self.ls_refs(wire, input, output, stateless)?;
        } else {
            let mut request = Request::new(&self.snapshot)?;
            loop {
                let packet = wire.required(&self.snapshot, input)?;
                if packet == PacketLineRef::Flush {
                    break;
                }
                request.argument(self, text(packet)?, true)?;
            }
            if stateless {
                require_end(input)?;
            }
            self.authorize(&request)?;
            let common = self.common(&request)?;
            if !request.done {
                encode::data_to_write(b"acknowledgments\n", &mut *output)?;
                if common.as_slice().is_empty() {
                    encode::data_to_write(b"NAK\n", &mut *output)?;
                } else {
                    for id in common.as_slice() {
                        self.ack(*id, b"", output)?;
                    }
                }
                encode::flush_to_write(&mut *output)?;
            } else {
                let boundaries = shallow::compute(self, &request)?;
                if boundaries.changed() {
                    encode::data_to_write(b"shallow-info\n", &mut *output)?;
                    boundaries.write(self, output)?;
                    encode::delim_to_write(&mut *output)?;
                }
                encode::data_to_write(b"packfile\n", &mut *output)?;
                self.send_pack(
                    &request,
                    common.as_slice(),
                    boundaries.boundaries(),
                    65_515,
                    output,
                )?;
                complete = true;
            }
        }
        // Server responses terminate at their flush. The stateless remote-curl
        // helper supplies response-end to its caller; it is not an HTTP body packet.
        output.flush()?;
        Ok(!complete)
    }

    fn ls_refs(
        &self,
        wire: &mut Wire,
        input: &mut impl Read,
        output: &mut impl Write,
        stateless: bool,
    ) -> Result<()> {
        let mut symrefs = false;
        let mut peel_tags = false;
        let mut unborn = false;
        let mut prefixes = ChargedVec::new(self.snapshot.capacity.clone(), MAX_REFS)?;
        loop {
            let packet = wire.required(&self.snapshot, input)?;
            if packet == PacketLineRef::Flush {
                break;
            }
            match text(packet)? {
                b"symrefs" if !symrefs => symrefs = true,
                b"peel" if !peel_tags => peel_tags = true,
                b"unborn" if !unborn => unborn = true,
                prefix if prefix.starts_with(b"ref-prefix ") && prefix.len() <= 1035 => {
                    prefixes.push(copy(&self.snapshot, &prefix[11..])?, &self.snapshot.control)?;
                }
                _ => return Err(invalid("invalid ls-refs argument")),
            }
        }
        if stateless {
            require_end(input)?;
        }
        let matches = |name: &[u8]| {
            prefixes.as_slice().is_empty()
                || prefixes
                    .as_slice()
                    .iter()
                    .any(|prefix| name.starts_with(prefix.as_slice()))
        };
        if matches(b"HEAD") {
            if let Some(head) = self.head() {
                self.ls_ref(
                    b"HEAD",
                    Some(head.id),
                    symrefs,
                    if peel_tags { head.peeled } else { None },
                    output,
                )?;
            } else if unborn {
                self.ls_ref(b"HEAD", None, true, None, output)?;
            }
        }
        for reference in self.refs.as_slice() {
            self.snapshot.control.check()?;
            if matches(reference.name.as_slice()) {
                self.ls_ref(
                    reference.name.as_slice(),
                    Some(reference.id),
                    false,
                    if peel_tags { reference.peeled } else { None },
                    output,
                )?;
            }
        }
        encode::flush_to_write(output)?;
        Ok(())
    }

    fn ls_ref(
        &self,
        name: &[u8],
        id: Option<ObjectId>,
        symref: bool,
        peeled: Option<ObjectId>,
        output: &mut impl Write,
    ) -> Result<()> {
        let mut bytes = [0u8; 4096];
        let mut line = std::io::Cursor::new(bytes.as_mut_slice());
        if let Some(id) = id {
            write!(line, "{id} ")?;
        } else {
            line.write_all(b"unborn ")?;
        }
        line.write_all(name)?;
        if symref {
            line.write_all(b" symref-target:")?;
            line.write_all(self.default_ref.as_slice())?;
        }
        if let Some(id) = peeled {
            write!(line, " peeled:{id}")?;
        }
        line.write_all(b"\n")?;
        let len = line.position() as usize;
        encode::data_to_write(&bytes[..len], output)?;
        Ok(())
    }

    fn run_v1(
        &mut self,
        wire: &mut Wire,
        input: &mut impl Read,
        output: &mut impl Write,
        stateless: bool,
    ) -> Result<()> {
        let mut request = Request::new(&self.snapshot)?;
        loop {
            let Some(packet) = wire.next(&self.snapshot, input)? else {
                return Ok(());
            };
            if packet == PacketLineRef::Flush {
                break;
            }
            let line = text(packet)?;
            if request.wants.as_slice().is_empty() {
                let want = line.strip_prefix(b"want ").ok_or_else(|| {
                    invalid("upload first request must want an advertised object")
                })?;
                let (id, capabilities) = want
                    .split_at_checked(40)
                    .ok_or_else(|| invalid("invalid upload want length"))?;
                request.wants.push(parse_id(id)?, &self.snapshot.control)?;
                if !capabilities.is_empty() {
                    let capabilities = capabilities
                        .strip_prefix(b" ")
                        .ok_or_else(|| invalid("invalid upload capabilities separator"))?;
                    request.capabilities(capabilities)?;
                }
            } else {
                request.argument(self, line, false)?;
            }
        }
        if request.wants.as_slice().is_empty() {
            if stateless {
                require_end(input)?;
            }
            return Ok(());
        }
        self.authorize(&request)?;
        let boundaries = shallow::compute(self, &request)?;
        if request.deepening() {
            boundaries.write(self, output)?;
            encode::flush_to_write(&mut *output)?;
            output.flush()?;
        }
        let mut first_ack = None;
        let mut rounds = 0;
        loop {
            let packet = wire.required(&self.snapshot, input)?;
            if packet == PacketLineRef::Flush {
                rounds += 1;
                if rounds > MAX_ROUNDS {
                    return Err(invalid("upload negotiation-round limit exceeded"));
                }
                if stateless {
                    require_end(input)?;
                }
                if request.ack != Ack::Single || first_ack.is_none() {
                    encode::data_to_write(b"NAK\n", &mut *output)?;
                }
                output.flush()?;
                if stateless {
                    return Ok(());
                }
                continue;
            }
            let line = text(packet)?;
            if line == b"done" {
                if stateless {
                    require_end(input)?;
                }
                let common = self.common(&request)?;
                if let Some(id) = common.as_slice().last() {
                    if request.ack != Ack::Single || first_ack.is_none() {
                        self.ack(*id, b"", output)?;
                    }
                } else {
                    encode::data_to_write(b"NAK\n", &mut *output)?;
                }
                return self.send_pack(
                    &request,
                    common.as_slice(),
                    boundaries.boundaries(),
                    request.sideband,
                    output,
                );
            }
            let have = line
                .strip_prefix(b"have ")
                .ok_or_else(|| invalid("expected upload have or done"))?;
            let id = parse_id(have)?;
            request.haves.push(id, &self.snapshot.control)?;
            if self.is_common(&id)? {
                match request.ack {
                    Ack::Detailed => self.ack(id, b" common", output)?,
                    Ack::Multi => self.ack(id, b" continue", output)?,
                    Ack::Single if first_ack.is_none() => self.ack(id, b"", output)?,
                    Ack::Single => {}
                }
                first_ack = Some(id);
                output.flush()?;
            }
        }
    }

    fn authorize(&self, request: &Request) -> Result<()> {
        if request.wants.as_slice().is_empty() {
            return Err(invalid("fetch requires at least one want"));
        }
        for id in request.wants.as_slice() {
            self.snapshot.control.check()?;
            if !self.advertised.contains(id) {
                return Err(invalid("want was not an authorized advertised object"));
            }
        }
        Ok(())
    }

    fn is_common(&mut self, id: &ObjectId) -> Result<bool> {
        if self.visible_commits.is_none() {
            let reachable = self.snapshot.reachable_from_refs(
                self.refs
                    .as_slice()
                    .iter()
                    .map(|reference| (reference.name.as_slice(), reference.id)),
            )?;
            let mut visible = Set::new(
                self.snapshot.capacity.clone(),
                self.snapshot.limits.max_graph_objects,
            )?;
            for id in reachable.iter() {
                visible.insert(id, &self.snapshot.control)?;
            }
            self.visible_commits = Some(visible);
        }
        self.snapshot.control.check()?;
        Ok(self
            .visible_commits
            .as_ref()
            .is_some_and(|visible| visible.contains(id)))
    }

    fn common(&mut self, request: &Request) -> Result<ChargedVec<ObjectId>> {
        let mut seen = Set::new(
            self.snapshot.capacity.clone(),
            self.snapshot.limits.max_graph_objects,
        )?;
        let mut common = ChargedVec::new(
            self.snapshot.capacity.clone(),
            self.snapshot.limits.max_graph_objects,
        )?;
        for id in request.haves.as_slice() {
            if self.is_common(id)? && seen.insert(*id, &self.snapshot.control)? {
                common.push(*id, &self.snapshot.control)?;
            }
        }
        Ok(common)
    }

    fn send_pack(
        &self,
        request: &Request,
        common: &[ObjectId],
        boundaries: &[ObjectId],
        sideband: usize,
        output: &mut impl Write,
    ) -> Result<()> {
        let excluded = self.snapshot.select_objects(
            common.iter().copied(),
            None,
            request.shallow.as_slice(),
        )?;
        let mut selection = self.snapshot.select_objects(
            request.wants.as_slice().iter().copied(),
            Some(&excluded),
            boundaries,
        )?;
        if request.include_tag {
            let mut roots = ChargedVec::new(self.snapshot.capacity.clone(), MAX_REFS * 2)?;
            for id in request.wants.as_slice() {
                roots.push(*id, &self.snapshot.control)?;
            }
            for reference in self.refs.as_slice() {
                if reference.peeled.is_some_and(|id| selection.contains(&id)) {
                    roots.push(reference.id, &self.snapshot.control)?;
                }
            }
            selection = self.snapshot.select_objects(
                roots.as_slice().iter().copied(),
                Some(&excluded),
                boundaries,
            )?;
        }
        if sideband == 0 {
            self.snapshot
                .write_pack(&selection, output, PackWriteLimits::default())?;
        } else {
            let mut bands = Sideband {
                output: &mut *output,
                max: sideband,
            };
            self.snapshot
                .write_pack(&selection, &mut bands, PackWriteLimits::default())?;
            encode::flush_to_write(&mut *bands.output)?;
        }
        output.flush()?;
        Ok(())
    }

    fn ack(&self, id: ObjectId, suffix: &[u8], output: &mut impl Write) -> Result<()> {
        self.snapshot.control.check()?;
        let mut data = [0u8; 80];
        let mut line = std::io::Cursor::new(data.as_mut_slice());
        write!(line, "ACK {id}")?;
        line.write_all(suffix)?;
        line.write_all(b"\n")?;
        let len = line.position() as usize;
        encode::data_to_write(&data[..len], output)?;
        Ok(())
    }

    fn head(&self) -> Option<&VisibleRef> {
        self.refs
            .as_slice()
            .iter()
            .find(|reference| reference.name.as_slice() == self.default_ref.as_slice())
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Ack {
    Single,
    Multi,
    Detailed,
}

struct Request {
    wants: ChargedVec<ObjectId>,
    haves: ChargedVec<ObjectId>,
    shallow: ChargedVec<ObjectId>,
    deepen_not: ChargedVec<ObjectId>,
    depth: Option<usize>,
    since: Option<i64>,
    relative: bool,
    include_tag: bool,
    done: bool,
    sideband: usize,
    ack: Ack,
}

impl Request {
    fn new<P: VerifiedObjectProvider>(snapshot: &SnapshotSession<P>) -> Result<Self> {
        Ok(Self {
            // Native clients can request every catalog ref and the separately
            // advertised HEAD without deduplicating identical object IDs.
            wants: ChargedVec::new(snapshot.capacity.clone(), MAX_REFS + 1)?,
            haves: ChargedVec::new(snapshot.capacity.clone(), snapshot.limits.max_graph_objects)?,
            shallow: ChargedVec::new(snapshot.capacity.clone(), snapshot.limits.max_graph_objects)?,
            deepen_not: ChargedVec::new(snapshot.capacity.clone(), MAX_REFS)?,
            depth: None,
            since: None,
            relative: false,
            include_tag: false,
            done: false,
            sideband: 0,
            ack: Ack::Single,
        })
    }

    fn argument<P: VerifiedObjectProvider>(
        &mut self,
        session: &UploadSession<P>,
        line: &[u8],
        v2: bool,
    ) -> Result<()> {
        let control = &session.snapshot.control;
        if let Some(id) = line.strip_prefix(b"want ") {
            self.wants.push(parse_id(id)?, control)?;
        } else if v2 && line.starts_with(b"have ") {
            self.haves.push(parse_id(&line[5..])?, control)?;
        } else if let Some(id) = line.strip_prefix(b"shallow ") {
            self.shallow.push(parse_id(id)?, control)?;
        } else if let Some(depth) = line.strip_prefix(b"deepen ") {
            if self.depth.is_some()
                || self.since.is_some()
                || !self.deepen_not.as_slice().is_empty()
            {
                return Err(invalid("incompatible or duplicate deepen requests"));
            }
            self.depth = Some(number(depth)?);
        } else if let Some(since) = line.strip_prefix(b"deepen-since ") {
            if self.depth.is_some() || self.since.is_some() {
                return Err(invalid("incompatible or duplicate deepen-since request"));
            }
            self.since = Some(i64::try_from(number(since)?)?);
        } else if let Some(name) = line.strip_prefix(b"deepen-not ") {
            if self.depth.is_some() {
                return Err(invalid("deepen-not cannot be combined with deepen"));
            }
            let mut found = None;
            for reference in session.refs.as_slice() {
                let full = reference.name.as_slice();
                if full == name
                    || full.strip_prefix(b"refs/heads/") == Some(name)
                    || full.strip_prefix(b"refs/tags/") == Some(name)
                    || name == b"HEAD" && full == session.default_ref.as_slice()
                {
                    if found.is_some() {
                        return Err(invalid("ambiguous deepen-not ref"));
                    }
                    found = Some(reference.peeled.unwrap_or(reference.id));
                }
            }
            self.deepen_not.push(
                found.ok_or_else(|| invalid("deepen-not ref is not advertised"))?,
                control,
            )?;
        } else {
            match line {
                b"deepen-relative" if !self.relative => self.relative = true,
                b"include-tag" if v2 && !self.include_tag => self.include_tag = true,
                b"done" if v2 && !self.done => self.done = true,
                b"thin-pack" | b"ofs-delta" | b"no-progress" | b"wait-for-done" if v2 => {}
                _ => return Err(invalid("unsupported or misplaced upload argument")),
            }
        }
        Ok(())
    }

    fn capabilities(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > 4096 {
            return Err(invalid("upload capability byte limit exceeded"));
        }
        let mut seen = 0u32;
        for capability in bytes.split(|byte| *byte == b' ') {
            let (bit, ack, band) = match capability {
                b"multi_ack" => (1, Some(Ack::Multi), 0),
                b"multi_ack_detailed" => (2, Some(Ack::Detailed), 0),
                b"side-band" => (4, None, 995),
                b"side-band-64k" => (8, None, 65_515),
                b"thin-pack" => (16, None, 0),
                b"ofs-delta" => (32, None, 0),
                b"shallow" => (64, None, 0),
                b"deepen-relative" => (128, None, 0),
                b"deepen-since" => (256, None, 0),
                b"deepen-not" => (512, None, 0),
                b"no-progress" => (1024, None, 0),
                b"include-tag" => (2048, None, 0),
                b"object-format=sha1" => (4096, None, 0),
                agent if agent.starts_with(b"agent=") && valid_agent(&agent[6..]) => {
                    (8192, None, 0)
                }
                _ => return Err(invalid("unadvertised upload capability")),
            };
            if seen & bit != 0 {
                return Err(invalid("duplicate upload capability"));
            }
            seen |= bit;
            if let Some(ack) = ack {
                if self.ack != Ack::Single {
                    return Err(invalid("conflicting upload ACK capabilities"));
                }
                self.ack = ack;
            }
            if band != 0 {
                if self.sideband != 0 {
                    return Err(invalid("conflicting upload sideband capabilities"));
                }
                self.sideband = band;
            }
            if capability == b"include-tag" {
                self.include_tag = true;
            }
            if capability == b"deepen-relative" {
                self.relative = true;
            }
        }
        Ok(())
    }

    fn deepening(&self) -> bool {
        self.depth.is_some() || self.since.is_some() || !self.deepen_not.as_slice().is_empty()
    }
}

struct Wire {
    bytes: CapacityBuffer,
    consumed: usize,
}

impl Wire {
    fn next<'a, P: VerifiedObjectProvider>(
        &'a mut self,
        snapshot: &SnapshotSession<P>,
        input: &mut impl Read,
    ) -> Result<Option<PacketLineRef<'a>>> {
        snapshot.control.check()?;
        self.bytes.resize(4, 0)?;
        if input.read(&mut self.bytes.as_mut_slice()[..1])? == 0 {
            return Ok(None);
        }
        input.read_exact(&mut self.bytes.as_mut_slice()[1..4])?;
        let length = match gix_packetline::decode::hex_prefix(self.bytes.as_slice())? {
            gix_packetline::decode::PacketLineOrWantedSize::Line(_) => 0,
            gix_packetline::decode::PacketLineOrWantedSize::Wanted(length) => usize::from(length),
        };
        if length > 65_516 {
            return Err(invalid("oversized upload packet"));
        }
        self.consumed = self
            .consumed
            .checked_add(length + 4)
            .filter(|total| *total <= MAX_REQUEST_BYTES)
            .ok_or_else(|| invalid("upload request byte limit exceeded"))?;
        self.bytes.resize(length + 4, 0)?;
        for chunk in self.bytes.as_mut_slice()[4..].chunks_mut(32 * 1024) {
            snapshot.control.check()?;
            input.read_exact(chunk)?;
        }
        snapshot.control.check()?;
        Ok(Some(gix_packetline::decode(self.bytes.as_slice())?))
    }
    fn required<'a, P: VerifiedObjectProvider>(
        &'a mut self,
        snapshot: &SnapshotSession<P>,
        input: &mut impl Read,
    ) -> Result<PacketLineRef<'a>> {
        self.next(snapshot, input)?
            .ok_or_else(|| invalid("truncated upload request"))
    }
}

struct Sideband<'a, W> {
    output: &'a mut W,
    max: usize,
}
impl<W: Write> Write for Sideband<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let len = bytes.len().min(self.max);
        encode::band_to_write(Channel::Data, &bytes[..len], &mut *self.output)?;
        Ok(len)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.output.flush()
    }
}

fn text(packet: PacketLineRef<'_>) -> Result<&[u8]> {
    let PacketLineRef::Data(line) = packet else {
        return Err(invalid("unexpected upload packet delimiter"));
    };
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    if line.iter().any(|byte| *byte < 32 || *byte == 127) {
        return Err(invalid("control byte in upload argument"));
    }
    Ok(line)
}
fn require_end(input: &mut impl Read) -> Result<()> {
    if input.read(&mut [0u8; 1])? != 0 {
        return Err(invalid("trailing upload HTTP request data"));
    }
    Ok(())
}
fn parse_id(bytes: &[u8]) -> Result<ObjectId> {
    let id = ObjectId::from_hex(bytes)?;
    if id.is_null() || id.kind() != gix_hash::Kind::Sha1 {
        return Err(invalid("invalid upload SHA-1 object ID"));
    }
    Ok(id)
}
fn number(bytes: &[u8]) -> Result<usize> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return Err(invalid("invalid upload depth/timestamp"));
    }
    let value = std::str::from_utf8(bytes)?.parse::<usize>()?;
    if value == 0 || value > i32::MAX as usize {
        return Err(invalid("upload depth/timestamp out of range"));
    }
    Ok(value)
}
fn valid_agent(bytes: &[u8]) -> bool {
    !bytes.is_empty() && bytes.len() <= 128 && bytes.iter().all(|byte| (33..127).contains(byte))
}
fn validate_ref(name: &[u8]) -> Result<()> {
    if name.len() > MAX_REF_BYTES || !name.starts_with(b"refs/") {
        return Err(invalid("invalid upload ref namespace"));
    }
    gix_validate::reference::name(name.into())?;
    Ok(())
}
fn copy<P: VerifiedObjectProvider>(
    snapshot: &SnapshotSession<P>,
    bytes: &[u8],
) -> Result<CapacityBuffer> {
    let mut output = snapshot.capacity.buffer(bytes.len())?;
    output.extend_controlled(bytes, bytes.len(), &snapshot.control)?;
    Ok(output)
}
fn peel<P: VerifiedObjectProvider>(
    snapshot: &SnapshotSession<P>,
    original: ObjectId,
) -> Result<Option<ObjectId>> {
    let mut id = original;
    let mut seen = Set::new(snapshot.capacity.clone(), snapshot.limits.max_graph_objects)?;
    loop {
        snapshot.control.check()?;
        if !seen.insert(id, &snapshot.control)? {
            return Err(invalid("upload tag cycle"));
        }
        let header = snapshot
            .header(&id)?
            .ok_or_else(|| invalid("missing upload ref target"))?;
        if header.kind != Kind::Tag {
            return Ok((id != original).then_some(id));
        }
        let tag = snapshot.required(&id, Kind::Tag)?;
        let end = metadata_header_end(
            tag.data.as_slice(),
            snapshot.limits.max_metadata_bytes,
            &snapshot.control,
        )?;
        let tag = gix_object::TagRef::from_bytes(&tag.data.as_slice()[..end], id.kind())?;
        let target = tag.target();
        if snapshot
            .header(&target)?
            .is_none_or(|header| header.kind != tag.target_kind)
        {
            return Err(invalid("upload tag target kind mismatch"));
        }
        id = target;
    }
}
