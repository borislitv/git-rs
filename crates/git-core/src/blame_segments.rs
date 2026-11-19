//! Job-local blame segments. Scheduling, source authority and fallback stay with the caller.
use super::*;

pub use gix_blame::segments::Endpoint;

/// Opaque summary returned by a worker using the same held source and job resources.
pub struct BlameSegment {
    inner: Option<gix_blame::segments::Summary>,
    control: Control,
    capacity: CapacityBudget,
    scopes: completed::Scopes,
}

/// Completed local prefix for a warm run. It has no wire format, no import or
/// public summary constructor. Accepts only segments freshly computed by append.
#[derive(Default)]
pub struct LocalSegmentRun {
    prefix: Option<BlameSegment>,
}

impl LocalSegmentRun {
    pub fn append<P: VerifiedObjectProvider>(
        &mut self,
        session: &SnapshotSession<P>,
        request: BlameRequest<'_>,
        endpoint: Option<Endpoint>,
        options: &BlameOptions,
    ) -> Result<bool, BlameError> {
        let prefix = self.prefix.take();
        let scopes = completed::scopes(session, options)?;
        if prefix
            .as_ref()
            .is_some_and(|piece| !session.accepts_local_prefix(piece, &scopes))
        {
            return Ok(false);
        }
        let Some(piece) = session.summarize_blame_segment(request, endpoint, options)? else {
            return Ok(false);
        };
        self.prefix = match prefix {
            Some(prefix) => session.compose_segments(&prefix, &piece, scopes)?,
            None => Some(piece),
        };
        Ok(self.prefix.is_some())
    }

    pub fn finish<P: VerifiedObjectProvider>(
        self,
        session: &SnapshotSession<P>,
        request: BlameRequest<'_>,
        options: &BlameOptions,
    ) -> Result<Option<CompleteBlame>, BlameError> {
        let Some(mut prefix) = self.prefix else {
            return Ok(None);
        };
        let scopes = completed::scopes(session, options)?;
        if !session.accepts_local_prefix(&prefix, &scopes) {
            return Ok(None);
        }
        // Only the completed summary moves; original allocation tokens keep
        // their controls. The final session owns any new work/metadata.
        let inner = prefix.inner.take().expect("live local summary");
        session.finish_blame_segments(session.segment_owner(inner, scopes), request, options)
    }
}

impl BlameSegment {
    fn inner(&self) -> &gix_blame::segments::Summary {
        self.inner.as_ref().expect("live summary")
    }
}

impl Drop for BlameSegment {
    fn drop(&mut self) {
        if let Some(summary) = self.inner.take() {
            summary.clear_with_control(&mut |_| self.control.check().map_err(|_| Cancelled));
        }
    }
}

impl<P: VerifiedObjectProvider> SnapshotSession<P> {
    /// Compute independently using a worker-local session over the same held
    /// snapshot. `None` means that the caller must use ordinary blame.
    pub fn summarize_blame_segment(
        &self,
        request: BlameRequest<'_>,
        endpoint: Option<Endpoint>,
        options: &BlameOptions,
    ) -> Result<Option<BlameSegment>, BlameError> {
        self.summarize_blame_segment_observed(request, endpoint, options, None)
    }

    pub fn summarize_blame_segment_observed(
        &self,
        request: BlameRequest<'_>,
        endpoint: Option<Endpoint>,
        options: &BlameOptions,
        progress: Option<&BlameProgress>,
    ) -> Result<Option<BlameSegment>, BlameError> {
        self.validate_blame_request(request)?;
        self.blame_target(request)?;
        let scopes = completed::scopes(self, options)?;
        let cause = Arc::new(Cause::default());
        let allocator = self.segment_allocator(&cause);
        let _platform_metadata = self.capacity.reservation(PLATFORM_BOOTSTRAP_BYTES)?;
        let mut raw_metadata = RawBlobMetadata {
            tokens: ChargedVec::new(
                self.capacity.clone(),
                isize::MAX as usize / std::mem::size_of::<CapacityReservation>(),
            )
            .map_err(|error| source_error(error.as_ref()))?,
            control: self.control.clone(),
        };
        let mut platform = raw_platform(self.identity().object_hash);
        platform
            .validate_controlled_raw()
            .map_err(|_| BlameError::UnsupportedContract)?;
        let source = FindBridge {
            session: self,
            cause: cause.clone(),
            progress,
        };
        let mut raw = |id: &oid| source.raw_blob(id, &mut raw_metadata);
        let max_commits = self.limits.max_graph_objects.min(MAX_COMMITS);
        let mut poll = blame_cpu_poll(&self.control);
        let mut check = |event: gix_diff::blob::control::Checkpoint| {
            if event.stage == Stage::Commit
                && let Some(progress) = progress
            {
                let mut counters = progress.snapshot();
                counters.commit_watermark = event.processed;
                progress.counters.set(counters);
            }
            poll(event.stage)
                .map_err(BlameError::from)
                .and_then(|()| {
                    if event.stage == Stage::Commit && event.processed >= max_commits {
                        Err(BlameError::WorkLimit)
                    } else {
                        Ok(())
                    }
                })
                .map_err(|err| {
                    cause.record(err);
                    Cancelled
                })
        };
        let result = gix_blame::segments::summarize(
            &source,
            request.commit.to_owned(),
            endpoint,
            &mut platform,
            request.path.into(),
            options.vendor_options(),
            options.tuning,
            allocator,
            &mut check,
            scopes.immutable,
            Some(&mut raw),
        )
        .map_err(|_| {
            cause
                .get()
                .unwrap_or(BlameError::Corrupt("blame segment failed"))
        })?;
        self.control.check()?;
        Ok(result.map(|inner| self.segment_owner(inner, scopes)))
    }

    /// Compose snapshots most-recent first. No object IO is performed.
    pub fn compose_blame_segments(
        &self,
        younger: &BlameSegment,
        older: &BlameSegment,
        options: &BlameOptions,
    ) -> Result<Option<BlameSegment>, BlameError> {
        let scopes = completed::scopes(self, options)?;
        self.control.check()?;
        if !self.accepts_segment(younger, &scopes) || !self.accepts_segment(older, &scopes) {
            return Ok(None);
        }
        self.compose_segments(younger, older, scopes)
    }

    fn compose_segments(
        &self,
        younger: &BlameSegment,
        older: &BlameSegment,
        scopes: completed::Scopes,
    ) -> Result<Option<BlameSegment>, BlameError> {
        let cause = Arc::new(Cause::default());
        let result = gix_blame::segments::compose(
            younger.inner(),
            older.inner(),
            self.segment_allocator(&cause),
            &mut |_| {
                self.control.check().map_err(|err| {
                    cause.record(err.into());
                    Cancelled
                })
            },
        )
        .map_err(|_| {
            cause
                .get()
                .unwrap_or(BlameError::Corrupt("blame segment composition failed"))
        })?;
        self.control.check()?;
        Ok(result.map(|inner| self.segment_owner(inner, scopes)))
    }

    /// Finalize a terminal summary with metadata read from the same live verified
    /// source. Rendering uses the ordinary complete blame porcelain API.
    pub fn finish_blame_segments(
        &self,
        mut segment: BlameSegment,
        request: BlameRequest<'_>,
        options: &BlameOptions,
    ) -> Result<Option<CompleteBlame>, BlameError> {
        self.validate_blame_request(request)?;
        let scopes = completed::scopes(self, options)?;
        let mut path_check = Ok(());
        if !self.accepts_segment(&segment, &scopes)
            || &*segment.inner().start().commit != request.commit
            || segment.inner().endpoint().is_some()
        {
            return Ok(None);
        }
        let matches_path = segment.inner().matches_path(request.path, &mut |_| {
            self.control.check().map_err(|error| {
                path_check = Err(BlameError::from(error));
                Cancelled
            })
        });
        path_check?;
        if !matches_path.map_err(|_| BlameError::Corrupt("cannot compare blame segment path"))? {
            return Ok(None);
        }
        self.control.check()?;
        let admitted = segment
            .inner
            .take()
            .expect("live summary")
            .into_complete()
            .expect("terminal summary");
        let max = self.limits.max_graph_objects.min(MAX_COMMITS);
        if admitted.outcome().statistics.commits_traversed > max {
            return Err(BlameError::WorkLimit);
        }
        let mut complete = CompleteBlame {
            admitted,
            target: request.commit.to_owned(),
            path_digest: Sha256::digest(request.path).into(),
            live_scope: scopes.live,
            completed_use: CompletedUse::NotRequested,
            metadata: Metadata::new(0, &self.capacity, &self.control)?,
            control: self.control.clone(),
            capacity: self.capacity.clone(),
        };
        let trace = complete.trace();
        if complete.blob().len() > MAX_BLOB_BYTES {
            return Err(BlameError::BlobTooLarge);
        }
        let mut metadata = Metadata::new(trace.len().min(max), &self.capacity, &self.control)?;
        for (index, row) in trace.iter().enumerate() {
            self.control.check()?;
            if metadata.get(&row.commit_id).is_none() {
                if metadata.table.len() >= max {
                    return Err(BlameError::WorkLimit);
                }
                let fields = crate::porcelain::commit_metadata(self, &row.commit_id)?;
                metadata.insert(row.commit_id, index, fields);
            }
        }
        complete.metadata = metadata;
        self.control.check()?;
        Ok(Some(complete))
    }

    fn segment_allocator(&self, cause: &Arc<Cause>) -> Arc<BlameAllocator> {
        Arc::new(BlameAllocator {
            budget: self.capacity.clone(),
            control: self.control.clone(),
            cause: cause.clone(),
        })
    }

    fn segment_owner(
        &self,
        inner: gix_blame::segments::Summary,
        scopes: completed::Scopes,
    ) -> BlameSegment {
        BlameSegment {
            inner: Some(inner),
            control: self.control.clone(),
            capacity: self.capacity.clone(),
            scopes,
        }
    }

    fn accepts_segment(&self, segment: &BlameSegment, scopes: &completed::Scopes) -> bool {
        self.accepts_local_prefix(segment, scopes)
            && segment.control.deadline() == self.control.deadline()
            && std::ptr::eq(
                segment.control.cancellation_flag(),
                self.control.cancellation_flag(),
            )
    }

    fn accepts_local_prefix(&self, segment: &BlameSegment, scopes: &completed::Scopes) -> bool {
        segment.scopes.live == scopes.live
            && segment.scopes.immutable == scopes.immutable
            && segment.capacity.shares_with(&self.capacity)
    }
}
