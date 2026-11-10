//! Raw-object, request-controlled tree and rewrite traversal.
use std::{cell::RefCell, collections::VecDeque};

use bstr::{BStr, ByteSlice};
use gix_error::{ErrorExt, Result, message};
use gix_object::TreeRefIter;

use super::{Action, ChangeRef, Options};
use crate::{
    blob::control::{self, BYTE_CHUNK, Check, Checkpoint, Stage},
    rewrites,
    tree::{self, Visit},
};

type Shared<'a, 'b> = RefCell<&'a mut Check<'b>>;

/// Like the ordinary tree/rewrite diff, but only raw immutable resources may be
/// used. All callbacks and the object provider must themselves honour control.
/// Partial emissions must be discarded if this function returns an error.
/// Root iterators and trees returned by the provider must have had their record
/// sizes validated before the ordinary object parser is invoked.
#[expect(clippy::too_many_arguments)]
pub fn diff_with_control(
    lhs: TreeRefIter<'_>,
    rhs: TreeRefIter<'_>,
    resource_cache: &mut crate::blob::Platform,
    state: &mut tree::State,
    objects: &impl gix_object::FindObjectOrHeader,
    mut for_each: impl FnMut(ChangeRef<'_>) -> Result<Action>,
    options: Options,
    check: &mut Check<'_>,
) -> std::result::Result<Option<rewrites::Outcome>, tree::Error> {
    resource_cache.validate_controlled_raw()?;
    let shared = RefCell::new(check);
    let mut tracked = options.rewrites.map(rewrites::Tracker::new);
    let result = walk(
        lhs,
        rhs,
        state,
        objects,
        options.location,
        &shared,
        &mut |change, path| match tracked.as_mut() {
            Some(tracked) => {
                match tracked.push_change_with_control(change, path, &mut |e| (**shared.borrow_mut())(e))? {
                    Some(change) => for_each(attach(change, path)),
                    None => Ok(Action::Continue(())),
                }
            }
            None => for_each(attach(change, path)),
        },
    );
    let mut callback_error = None;
    let result = result.and_then(|()| {
        let Some(tracked) = tracked.as_mut() else {
            return Ok(None);
        };
        tracked
            .emit_with_control(
                |dest, source| {
                    let change = match source {
                        Some(source) => {
                            use crate::rewrites::tracker::Change as _;
                            let (oid, mode) = dest.change.oid_and_entry_mode();
                            ChangeRef::Rewrite {
                                source_location: source.location,
                                source_entry_mode: source.entry_mode,
                                source_id: source.id,
                                source_relation: source.change.relation(),
                                entry_mode: mode,
                                id: oid.to_owned(),
                                relation: dest.change.relation(),
                                diff: source.diff,
                                location: dest.location,
                                copy: source.kind == rewrites::tracker::visit::SourceKind::Copy,
                            }
                        }
                        None => attach(dest.change, dest.location),
                    };
                    // Preserve callback failures rather than turning them into an
                    // intentional early-stop action.
                    match for_each(change) {
                        Ok(action) => action,
                        Err(error) => {
                            callback_error = Some(error);
                            Action::Break(())
                        }
                    }
                },
                resource_cache,
                objects,
                |push: &mut dyn FnMut(tree::visit::Change, &BStr) -> Result| {
                    // Diffing an empty tree traverses the source breadth-first, like
                    // the legacy exhaustive-source collector. Its hash is immaterial.
                    let empty = TreeRefIter::from_bytes(&[], gix_hash::Kind::Sha1);
                    walk(
                        empty,
                        lhs,
                        &mut tree::State::default(),
                        objects,
                        options.location,
                        &shared,
                        &mut |change, path| {
                            if let tree::visit::Change::Addition { entry_mode, oid, .. } = change
                                && entry_mode.is_blob()
                            {
                                push(
                                    tree::visit::Change::Modification {
                                        previous_entry_mode: entry_mode,
                                        previous_oid: gix_hash::ObjectId::null(oid.kind()),
                                        entry_mode,
                                        oid,
                                    },
                                    path,
                                )?;
                            }
                            Ok(Action::Continue(()))
                        },
                    )
                    .map_err(|e| e.raise())
                },
                &mut |e| (**shared.borrow_mut())(e),
            )
            .map(Some)
            .map_err(tree::Error::from)
    });
    if let Some(tracked) = tracked.as_mut() {
        tracked.clear_with_control(&mut |e| (**shared.borrow_mut())(e));
    }
    resource_cache.clear_controlled_raw(&mut |e| (**shared.borrow_mut())(e));
    match callback_error {
        Some(error) => Err(tree::Error::Failure(error)),
        None => result,
    }
}

fn attach(change: tree::visit::Change, location: &BStr) -> ChangeRef<'_> {
    match change {
        tree::visit::Change::Addition {
            entry_mode,
            oid,
            relation,
        } => ChangeRef::Addition {
            location,
            relation,
            entry_mode,
            id: oid,
        },
        tree::visit::Change::Deletion {
            entry_mode,
            oid,
            relation,
        } => ChangeRef::Deletion {
            location,
            relation,
            entry_mode,
            id: oid,
        },
        tree::visit::Change::Modification {
            previous_entry_mode,
            previous_oid,
            entry_mode,
            oid,
        } => ChangeRef::Modification {
            location,
            previous_entry_mode,
            entry_mode,
            previous_id: previous_oid,
            id: oid,
        },
    }
}

fn walk(
    lhs: TreeRefIter<'_>,
    rhs: TreeRefIter<'_>,
    state: &mut tree::State,
    objects: &impl gix_object::Find,
    location: Option<tree::recorder::Location>,
    shared: &Shared<'_, '_>,
    visit: &mut dyn FnMut(tree::visit::Change, &BStr) -> Result<Action>,
) -> std::result::Result<(), tree::Error> {
    let mut delegate = Delegate {
        shared,
        visit,
        location,
        path: Vec::new(),
        paths: VecDeque::new(),
        queued_bytes: 0,
        error: None,
    };
    let result = crate::tree(lhs, rhs, state, objects, &mut delegate);
    // Every queued path owns an allocation. Finish cleanup even after stop.
    while delegate.paths.pop_front().is_some() {
        let _ = (**shared.borrow_mut())(Checkpoint {
            stage: Stage::Cleanup,
            processed: delegate.paths.len(),
        });
    }
    match delegate.error {
        Some(error) => Err(tree::Error::Failure(error)),
        None => result,
    }
}

struct Delegate<'a, 'b, 'c> {
    shared: &'a Shared<'b, 'c>,
    visit: &'a mut dyn FnMut(tree::visit::Change, &BStr) -> Result<Action>,
    location: Option<tree::recorder::Location>,
    path: Vec<u8>,
    paths: VecDeque<Vec<u8>>,
    queued_bytes: usize,
    error: Option<gix_error::Error>,
}

impl Delegate<'_, '_, '_> {
    fn push(&mut self, component: &BStr, enqueue: bool) -> Result {
        if self.error.is_some() {
            return Ok(());
        }
        let Some(location) = self.location else {
            return Ok(());
        };
        if location == tree::recorder::Location::FileName {
            self.path.clear();
        }
        let slash = usize::from(!self.path.is_empty() && !component.is_empty());
        let len = self.path.len().saturating_add(slash).saturating_add(component.len());
        if len > 65536 {
            return Err(message("controlled tree path exceeds 64 KiB metadata limit").into());
        }
        let mut check = self.shared.borrow_mut();
        control::try_reserve(&mut self.path, slash + component.len(), &mut **check).map_err(|e| e.raise())?;
        if slash != 0 {
            self.path.push(b'/');
        }
        for chunk in component.chunks(BYTE_CHUNK) {
            self.path.extend_from_slice(chunk);
            check(Checkpoint {
                stage: Stage::Copy,
                processed: self.path.len(),
            })
            .map_err(|e| e.raise())?;
        }
        if enqueue && location == tree::recorder::Location::Path {
            if self.queued_bytes.saturating_add(len) > control::MAX_ALLOCATION_BYTES {
                return Err(message("controlled path queue exceeds capacity limit").into());
            }
            let mut path = Vec::new();
            control::try_reserve(&mut path, len, &mut **check).map_err(|e| e.raise())?;
            for chunk in self.path.chunks(BYTE_CHUNK) {
                path.extend_from_slice(chunk);
                check(Checkpoint {
                    stage: Stage::Copy,
                    processed: path.len(),
                })
                .map_err(|e| e.raise())?;
            }
            if self.paths.len() == self.paths.capacity() {
                let capacity = self.paths.capacity().saturating_mul(2).max(8);
                control::admit_bytes(capacity.saturating_mul(std::mem::size_of::<Vec<u8>>()), &mut **check)
                    .map_err(|e| e.raise())?;
                self.paths
                    .try_reserve_exact(capacity - self.paths.len())
                    .map_err(|_| message("controlled tree path queue allocation failed").raise())?;
            }
            self.queued_bytes += path.len();
            self.paths.push_back(path);
        }
        Ok(())
    }
}

impl Visit for Delegate<'_, '_, '_> {
    fn admit(&mut self, bytes: usize) -> std::result::Result<(), tree::Error> {
        self.check()?;
        control::admit_bytes(bytes, &mut **self.shared.borrow_mut()).map_err(|e| tree::Error::Failure(e.raise()))
    }
    fn check(&mut self) -> std::result::Result<(), tree::Error> {
        if let Some(error) = self.error.take() {
            return Err(tree::Error::Failure(error));
        }
        (**self.shared.borrow_mut())(Checkpoint {
            stage: Stage::Graph,
            processed: 1,
        })
        .map_err(|e| tree::Error::Failure(e.raise()))
    }
    fn pop_front_tracked_path_and_set_current(&mut self) {
        if self.error.is_some() {
            return;
        }
        if self.location == Some(tree::recorder::Location::Path)
            && let Some(path) = self.paths.pop_front()
        {
            self.queued_bytes -= path.len();
            self.path = path;
        }
    }
    fn push_back_tracked_path_component(&mut self, component: &BStr) {
        if let Err(error) = self.push(component, true) {
            self.error = Some(error);
        }
    }
    fn push_path_component(&mut self, component: &BStr) {
        if let Err(error) = self.push(component, false) {
            self.error = Some(error);
        }
    }
    fn pop_path_component(&mut self) {
        if self.error.is_some() || self.location != Some(tree::recorder::Location::Path) {
            return;
        }
        let len = self.path.len();
        for (n, chunk) in self.path.rchunks(BYTE_CHUNK).enumerate() {
            let slash = chunk.iter().rposition(|&b| b == b'/');
            if let Err(error) = (**self.shared.borrow_mut())(Checkpoint {
                stage: Stage::Graph,
                processed: n * BYTE_CHUNK + chunk.len(),
            }) {
                self.error = Some(error.raise());
                return;
            }
            if let Some(slash) = slash {
                self.path.truncate(len - n * BYTE_CHUNK - chunk.len() + slash);
                return;
            }
        }
        self.path.clear();
    }
    fn visit(&mut self, change: tree::visit::Change) -> tree::visit::Action {
        if self.error.is_some() {
            return Action::Break(());
        }
        match (self.visit)(change, self.path.as_bstr()) {
            Ok(action) => action,
            Err(error) => {
                self.error = Some(error);
                Action::Break(())
            }
        }
    }
}
