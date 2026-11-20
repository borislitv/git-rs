use super::storage::{ChargedVec, Set};
use crate::objects::{Result, invalid};
use crate::{CapacityBudget, CapacityBuffer, Control};
use gix_hash::ObjectId;
use gix_object::bstr::ByteSlice;
use gix_packetline::{PacketLineRef, blocking_io::StreamingPeekableIter};
use sha2::{Digest, Sha256};
use std::io::Read;

#[derive(Clone, Copy, Debug)]
pub struct CommandLimits {
    pub max_commands: usize,
    pub max_command_bytes: usize,
    pub max_ref_bytes: usize,
    pub max_capability_bytes: usize,
    pub max_capabilities: usize,
}

impl Default for CommandLimits {
    fn default() -> Self {
        Self {
            max_commands: 10_000,
            max_command_bytes: 12 * 1024 * 1024,
            max_ref_bytes: 1024,
            max_capability_bytes: 4096,
            max_capabilities: 32,
        }
    }
}

/// Only implemented server capabilities. Root advertisement must use precisely
/// this set (with its own agent value), not capabilities from a native backend.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Capabilities {
    bits: u16,
}

impl Capabilities {
    pub const ADVERTISED: &'static [u8] =
        b"report-status side-band-64k delete-refs ofs-delta atomic object-format=sha1 agent=git-rs";
    pub fn report_status(self) -> bool {
        self.bits & 1 != 0
    }
    pub fn sideband(self) -> bool {
        self.bits & 2 != 0
    }
    pub fn atomic(self) -> bool {
        self.bits & 16 != 0
    }
    fn parse(bytes: &[u8], limits: CommandLimits) -> Result<Self> {
        if bytes.len() > limits.max_capability_bytes {
            return Err(invalid("receive capability byte limit exceeded"));
        }
        // send-pack prefixes the first capability with SP after the command NUL.
        // Preserve raw byte accounting; do not trim controls, repeated/trailing
        // separators, or silently normalize arbitrary malformed token lists.
        let bytes = bytes.strip_prefix(b" ").unwrap_or(bytes);
        let mut value = Self::default();
        if bytes.is_empty() {
            return Ok(value);
        }
        for (count, token) in bytes.split(|byte| *byte == b' ').enumerate() {
            if count >= limits.max_capabilities
                || token.is_empty()
                || token.iter().any(|byte| !(33..=126).contains(byte))
            {
                return Err(invalid("invalid or excessive receive capabilities"));
            }
            let bit = match token {
                b"report-status" => 1,
                b"side-band-64k" => 2,
                b"delete-refs" => 4,
                b"ofs-delta" => 8,
                b"atomic" => 16,
                b"object-format=sha1" => 32,
                agent if agent.starts_with(b"agent=") && (7..=128).contains(&agent.len()) => 64,
                _ => return Err(invalid("unadvertised receive capability")),
            };
            if value.bits & bit != 0 {
                return Err(invalid("duplicate receive capability"));
            }
            value.bits |= bit;
        }
        Ok(value)
    }
}

pub struct Command {
    old: ObjectId,
    new: ObjectId,
    name: CapacityBuffer,
}

impl Command {
    pub fn old(&self) -> ObjectId {
        self.old
    }
    // `old`/`new` are Git wire field names, not a constructor pair.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(&self) -> ObjectId {
        self.new
    }
    pub fn name(&self) -> &[u8] {
        self.name.as_slice()
    }
}

/// Ordered, immutable, byte-preserving command section. No public mutation,
/// subset constructor or conversion from an unchecked command vector exists.
pub struct CommandBatch {
    commands: ChargedVec<Command>,
    capabilities: Capabilities,
    raw_capabilities: CapacityBuffer,
    pub(super) budget: CapacityBudget,
}

/// The protocol's command flush without commands. This token is not semantic,
/// HTTP EOF or publication authority and cannot be constructed by a caller.
pub struct Noop {
    _private: (),
}

/// A successful empty exchange is distinct from cancellation and a change batch.
pub enum CommandSection {
    Noop(Noop),
    Commands(CommandBatch),
}

impl CommandSection {
    pub fn read(
        input: &mut impl Read,
        control: &Control,
        budget: CapacityBudget,
        limits: CommandLimits,
    ) -> Result<Self> {
        let batch = CommandBatch::read_allow_empty(input, control, budget, limits)?;
        Ok(if batch.commands().is_empty() {
            Self::Noop(Noop { _private: () })
        } else {
            Self::Commands(batch)
        })
    }
}

impl CommandBatch {
    /// Read through the required command flush only; raw pack bytes are left in
    /// `input`. The caller owns blocking-I/O deadlines/shutdown. Neither flush nor
    /// returning this batch proves completion of the rest of the request body.
    pub fn read(
        input: &mut impl Read,
        control: &Control,
        budget: CapacityBudget,
        limits: CommandLimits,
    ) -> Result<Self> {
        match CommandSection::read(input, control, budget, limits)? {
            CommandSection::Commands(batch) => Ok(batch),
            CommandSection::Noop(_) => Err(invalid("change batch must not be empty")),
        }
    }

    fn read_allow_empty(
        input: &mut impl Read,
        control: &Control,
        budget: CapacityBudget,
        limits: CommandLimits,
    ) -> Result<Self> {
        control.check()?;
        if limits.max_commands == 0
            || limits.max_commands > 10_000
            || limits.max_ref_bytes == 0
            || limits.max_ref_bytes > 1024
            || limits.max_capability_bytes > 4096
            || limits.max_capabilities > 32
            || limits.max_command_bytes == 0
        {
            return Err(invalid("invalid receive command limits"));
        }
        // StreamingPeekableIter allocates one fixed 65,520-byte data buffer.
        // Do not use peek_line(), which allocates a second buffer.
        let _packet_capacity = budget.reservation(65_520)?;
        let mut packets = StreamingPeekableIter::new(input, &[PacketLineRef::Flush], false);
        let mut commands = ChargedVec::new(budget.clone(), limits.max_commands)?;
        let mut names = Set::new(budget.clone(), limits.max_commands)?;
        let mut capabilities = Capabilities::default();
        let mut raw_capabilities = budget.buffer(0)?;
        let mut total = 0usize;
        while let Some(packet) = packets.read_line() {
            control.check()?;
            let PacketLineRef::Data(bytes) = packet?? else {
                return Err(invalid("unexpected receive packet delimiter"));
            };
            total = total
                .checked_add(bytes.len() + 4)
                .filter(|total| *total <= limits.max_command_bytes)
                .ok_or_else(|| invalid("receive command byte limit exceeded"))?;
            let bytes = bytes.strip_suffix(b"\n").unwrap_or(bytes);
            let line = if let Some(nul) = bytes.iter().position(|byte| *byte == 0) {
                if !commands.as_slice().is_empty() {
                    return Err(invalid("capabilities permitted on first command only"));
                }
                let raw = &bytes[nul + 1..];
                capabilities = Capabilities::parse(raw, limits)?;
                raw_capabilities = budget.buffer(raw.len())?;
                raw_capabilities.resize(raw.len(), 0)?;
                raw_capabilities.as_mut_slice().copy_from_slice(raw);
                &bytes[..nul]
            } else {
                bytes
            };
            if line.len() < 83 || line[40] != b' ' || line[81] != b' ' {
                return Err(invalid("invalid receive command"));
            }
            let old = ObjectId::from_hex(&line[..40])?;
            let new = ObjectId::from_hex(&line[41..81])?;
            let name = &line[82..];
            if name.len() > limits.max_ref_bytes
                || !name.starts_with(b"refs/")
                || name.iter().any(|byte| *byte <= b' ' || *byte == 127)
                || old.is_null() && new.is_null()
            {
                return Err(invalid("invalid receive ref or zero-to-zero command"));
            }
            gix_validate::reference::name(name.as_bstr())?;
            // Fixed-size cryptographic keys bound name-index storage; collision
            // can only reject a batch, never merge/drop/authorize a command.
            let digest: [u8; 32] = Sha256::digest(name).into();
            if !names.insert(digest, control)? {
                return Err(invalid("duplicate receive ref command"));
            }
            let mut retained_name = budget.buffer(name.len())?;
            retained_name.resize(name.len(), 0)?;
            retained_name.as_mut_slice().copy_from_slice(name);
            commands.push(
                Command {
                    old,
                    new,
                    name: retained_name,
                },
                control,
            )?;
        }
        if packets.stopped_at() != Some(PacketLineRef::Flush) {
            return Err(invalid("missing receive command list or flush"));
        }
        control.check()?;
        Ok(Self {
            commands,
            capabilities,
            raw_capabilities,
            budget,
        })
    }
    pub fn commands(&self) -> &[Command] {
        self.commands.as_slice()
    }
    pub fn capabilities(&self) -> Capabilities {
        self.capabilities
    }
    pub fn raw_capabilities(&self) -> &[u8] {
        self.raw_capabilities.as_slice()
    }
    pub fn delete_only(&self) -> bool {
        self.commands()
            .iter()
            .all(|command| command.new().is_null())
    }
}
