//! Durable source jobs. Kubernetes candidacy never substitutes for these
//! DB-clock leadership/job predicates or for the repository's source barriers.

use crate::{
    Result, invalid,
    metadata::{self, bytes, text},
    process,
};
use std::{
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

pub(crate) struct Authority {
    pub owner: String,
    pub campaign: String,
    pub fence: u64,
    deadline: Mutex<Instant>,
    lost: AtomicBool,
    work: Mutex<Option<(Weak<process::Operation>, Weak<Job>)>>,
}

impl Authority {
    pub fn new(owner: String, campaign: String, fence: u64, deadline: Instant) -> Arc<Self> {
        Arc::new(Self {
            owner,
            campaign,
            fence,
            deadline: Mutex::new(deadline),
            lost: AtomicBool::new(false),
            work: Mutex::new(None),
        })
    }
    pub fn check(&self) -> Result<()> {
        if self.lost.load(Ordering::Acquire)
            || Instant::now()
                >= *self
                    .deadline
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            self.lose();
            return Err(invalid("maintenance authority expired or uncertain"));
        }
        let job = self
            .work
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|(_, job)| job.upgrade());
        if job.is_some_and(|job| {
            !job.completed.load(Ordering::Acquire)
                && Instant::now()
                    >= *job
                        .deadline
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
        }) {
            self.lose();
            return Err(invalid("maintenance job lease expired locally"));
        }
        Ok(())
    }
    pub fn renewed(&self, deadline: Instant) -> Result<()> {
        self.check()?;
        let mut current = self
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if Instant::now() >= *current || Instant::now() >= deadline {
            drop(current);
            self.lose();
            return Err(invalid("late maintenance renewal cannot revive authority"));
        }
        *current = deadline;
        Ok(())
    }
    pub fn constrain(&self, deadline: Instant) {
        {
            let mut current = self
                .deadline
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *current = (*current).min(deadline);
        }
        let _expired = self.check();
    }
    pub fn attach(&self, operation: &Arc<process::Operation>, job: &Arc<Job>) -> Result<()> {
        self.check()?;
        *self
            .work
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some((Arc::downgrade(operation), Arc::downgrade(job)));
        if self.check().is_err() {
            operation.cancel();
            return Err(invalid("maintenance authority lost before dispatch"));
        }
        Ok(())
    }
    pub fn lose(&self) {
        self.lost.store(true, Ordering::Release);
        if let Some(work) = self
            .work
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|(operation, _)| operation.upgrade())
        {
            work.cancel();
        }
    }
    pub fn args(&self) -> String {
        format!(
            "{},{}::uuid,{}",
            text(&self.owner),
            text(&self.campaign),
            self.fence
        )
    }
}

pub(crate) struct Job {
    pub id: String,
    pub repo: String,
    pub kind: String,
    pub token: String,
    pub generation: u64,
    pub attempts: u32,
    pub payload: Vec<u8>,
    pub authority: Arc<Authority>,
    deadline: Mutex<Instant>,
    completed: AtomicBool,
}

impl Job {
    pub fn check_local(&self) -> Result<()> {
        self.authority.check()?;
        if Instant::now()
            >= *self
                .deadline
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            self.authority.lose();
            return Err(invalid("maintenance job lease expired locally"));
        }
        Ok(())
    }
    pub fn args(&self) -> String {
        format!(
            "{},{}::uuid,{}::uuid,{}",
            self.authority.args(),
            text(&self.id),
            text(&self.token),
            self.generation
        )
    }
    pub fn check(&self) -> Result<()> {
        self.check_local()?;
        metadata::sql(&format!(
            "BEGIN; SELECT (maintenance_assert_job({})).id; COMMIT;",
            self.args()
        ))?;
        self.check_local()
    }
    pub fn heartbeat(&self, seconds: u64) -> Result<()> {
        self.authority.check()?;
        if self.completed.load(Ordering::Acquire) {
            return Ok(());
        }
        self.check_local()?;
        let started = Instant::now();
        if let Err(error) = metadata::sql(&format!(
            "BEGIN; SET LOCAL synchronous_commit=on; SELECT maintenance_heartbeat({},{}); COMMIT;",
            self.args(),
            seconds
        )) {
            // A publication may atomically finish the job while its controller
            // is renewing. This read only suppresses further heartbeats; the
            // worker still owes full input/output journal confirmation.
            let done = metadata::sql(&format!(
                "SELECT 1 FROM maintenance_jobs j JOIN maintenance_operations o ON o.job_id=j.id WHERE j.id={}::uuid AND j.state='succeeded' AND o.operation_id={}::uuid AND o.claim_generation={} AND o.leader_fence={};",
                text(&self.id),
                text(&self.token),
                self.generation,
                self.authority.fence
            ));
            if done.is_ok_and(|row| row.trim() == "1") {
                self.completed.store(true, Ordering::Release);
                return Ok(());
            }
            self.authority.lose();
            return Err(error);
        }
        let deadline = started + Duration::from_secs(seconds.saturating_sub(2));
        let mut current = self
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if Instant::now() >= *current || Instant::now() >= deadline {
            drop(current);
            self.authority.lose();
            return Err(invalid("late job renewal cannot revive its lease"));
        }
        *current = deadline;
        drop(current);
        self.check_local()
    }
    pub fn finish(&self, outcome: &str, code: Option<&str>) -> Result<()> {
        self.check_local()?;
        let delay = (5u64.saturating_mul(1u64 << self.attempts.saturating_sub(1).min(6))).min(240);
        let mut random = [0u8; 1];
        use std::io::Read;
        std::fs::File::open("/dev/urandom")?.read_exact(&mut random)?;
        let delay = (delay + u64::from(random[0]) % (delay / 4 + 1)).min(300);
        let code = code.map(text).unwrap_or_else(|| "NULL".into());
        if metadata::sql(&format!("BEGIN; SET LOCAL synchronous_commit=on; SELECT maintenance_finish({},{},{code},NULL,{delay}); COMMIT;", self.args(), text(outcome))).is_err() {
            let confirmed = process::Operation::new(5).enter(|| metadata::sql(&format!(
                "BEGIN; SET LOCAL synchronous_commit=on; SELECT maintenance_confirm_finish({}::uuid,{}::uuid,{},{},{},{code},NULL); COMMIT;",
                text(&self.id), text(&self.token), self.generation, self.authority.fence, text(outcome))))?;
            if confirmed.trim()!=outcome && !(outcome=="retry" && confirmed.trim()=="dead") {
                return Err(invalid("maintenance finish rejected or outcome unknown"));
            }
        }
        self.completed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) fn claim(authority: Arc<Authority>, seconds: u64) -> Result<Option<Arc<Job>>> {
    authority.check()?;
    if !(10..=120).contains(&seconds) {
        return Err(invalid("invalid job lease"));
    }
    let started = Instant::now();
    let token = crate::scratch::identifier()?;
    let columns = "j.id,j.repo_id,r.slug,j.kind,replace(j.owner_token::text,'-',''),j.claim_generation,j.attempts,encode(j.payload,'hex')";
    let query = format!(
        "BEGIN; SET LOCAL synchronous_commit=on; SELECT {columns} FROM maintenance_claim({},{}::uuid,{seconds}) j JOIN repositories r ON r.id=j.repo_id; COMMIT;",
        authority.args(),
        text(&token)
    );
    let rows = match metadata::sql(&query) {
        Ok(rows) => rows,
        Err(_) => {
            // A lost claim COMMIT must not dispatch a second attempt. Confirm the
            // exact freshly generated token with a WAL-producing heartbeat.
            let found = metadata::sql(&format!(
                "SELECT {columns} FROM maintenance_jobs j JOIN repositories r ON r.id=j.repo_id WHERE j.owner_token={}::uuid AND j.leader_fence={} AND j.state='running';",
                text(&token),
                authority.fence
            ))?;
            if found.trim().is_empty() {
                authority.lose();
                return Err(invalid("maintenance claim outcome unconfirmed"));
            }
            found
        }
    };
    authority.check()?;
    if rows.trim().is_empty() {
        return Ok(None);
    }
    // Empty payload is a meaningful final field. Strip only the row terminator,
    // not its trailing tab (or whitespace in a slug accepted by old metadata).
    let row = rows.strip_suffix('\n').unwrap_or(&rows);
    if row.contains('\n') {
        return Err(invalid("multiple maintenance claim rows"));
    }
    let fields = row.split('\t').collect::<Vec<_>>();
    let [
        id,
        repo,
        _slug,
        kind,
        actual_token,
        generation,
        attempts,
        payload,
    ] = fields.as_slice()
    else {
        return Err(invalid("invalid maintenance claim framing"));
    };
    if *actual_token != token {
        return Err(invalid("maintenance claim token mismatch"));
    }
    let job = Arc::new(Job {
        id: (*id).into(),
        repo: (*repo).into(),
        kind: (*kind).into(),
        token,
        generation: generation.parse()?,
        attempts: attempts.parse()?,
        payload: crate::protocol::unhex(payload.as_bytes())?,
        authority,
        deadline: Mutex::new(started + Duration::from_secs(seconds - 2)),
        completed: AtomicBool::new(false),
    });
    job.heartbeat(seconds)?;
    Ok(Some(job))
}

pub struct Status {
    pub id: String,
    pub state: String,
    pub error_code: Option<String>,
}

fn decode_status(row: &str) -> Result<Status> {
    let fields = row.trim_end_matches('\n').split('\t').collect::<Vec<_>>();
    let [id, state, code] = fields.as_slice() else {
        return Err(invalid("invalid maintenance status framing"));
    };
    if id.is_empty()
        || !matches!(
            *state,
            "queued" | "running" | "retry" | "succeeded" | "dead"
        )
    {
        return Err(invalid("maintenance status unavailable"));
    }
    Ok(Status {
        id: (*id).into(),
        state: (*state).into(),
        error_code: (!code.is_empty()).then(|| (*code).into()),
    })
}

/// Owner authorization and durable enqueue are independent of the worker's
/// Kubernetes credentials and lifetime. A cancelled requester does not own a job.
pub fn request_cleanup(slug: &str, principal: &str) -> Result<Status> {
    crate::auth::repository(slug)?;
    crate::auth::principal(principal)?;
    let request = crate::scratch::identifier()?;
    let key = text(&format!("cleanup:{request}"));
    let query = format!(
        "BEGIN; SET LOCAL synchronous_commit=on; DO $b$ DECLARE repo uuid; requested uuid; BEGIN SELECT r.id INTO STRICT repo FROM repositories r JOIN permissions p ON p.repo_id=r.id WHERE r.slug={} AND p.principal={} AND p.role='owner' FOR UPDATE OF r; requested:=maintenance_enqueue(repo,'orphan_scan',{key},''::bytea,10); UPDATE maintenance_jobs SET superseded_by=requested WHERE repo_id=repo AND kind IN ('orphan_scan','retire_gc') AND state='dead' AND superseded_by IS NULL; END $b$; UPDATE maintenance_jobs j SET updated_at=j.updated_at WHERE j.job_key={key} AND j.kind='orphan_scan' AND j.repo_id=(SELECT id FROM repositories WHERE slug={}) RETURNING j.id,j.state,coalesce(j.error_code,''); COMMIT;",
        text(slug),
        text(principal),
        text(slug)
    );
    // Coalesces the same request after unknown COMMIT. The no-op UPDATE is a
    // synchronous WAL confirmation, not a second independently generated job.
    let row = metadata::sql(&query).or_else(|_| {
        process::check_operation()?;
        metadata::sql(&query)
    })?;
    decode_status(&row)
}

pub fn status(id: &str) -> Result<Status> {
    decode_status(&metadata::sql(&format!(
        "SELECT id,state,coalesce(error_code,'') FROM maintenance_jobs WHERE id={}::uuid;",
        text(id)
    ))?)
}

pub(crate) fn scan(
    authority: &Authority,
    batch: usize,
    seconds: u64,
    budget_millis: u64,
) -> Result<()> {
    authority.check()?;
    if !(1..=1000).contains(&batch)
        || !(1..=3600).contains(&seconds)
        || !(1..=1000).contains(&budget_millis)
    {
        return Err(invalid("invalid maintenance scanner bounds"));
    }
    metadata::sql(&format!(
        "BEGIN; SET LOCAL synchronous_commit=on; SET LOCAL statement_timeout='{budget_millis}ms'; SELECT maintenance_scan({},{batch},{seconds},{budget_millis}); COMMIT;",
        authority.args()
    ))?;
    authority.check()
}

pub(crate) fn refresh(job: &Job, generation: u64) -> Result<()> {
    job.authority.check()?;
    metadata::sql(&format!(
        "BEGIN; SET LOCAL synchronous_commit=on; SELECT (maintenance_assert_job({})).id; SELECT maintenance_enqueue({}::uuid,'compact',{},{}); COMMIT;",
        job.args(),
        text(&job.repo),
        text(&format!("compact:{}:{generation}:v1", job.repo)),
        bytes(generation.to_string().as_bytes())
    ))?;
    Ok(())
}

pub fn wait(id: &str, seconds: u64) -> Result<Status> {
    if seconds > 300 {
        return Err(invalid("maintenance wait must be at most 300 seconds"));
    }
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        let state = status(id)?;
        if matches!(state.state.as_str(), "succeeded" | "dead") || Instant::now() >= deadline {
            return Ok(state);
        }
        process::check_operation()?;
        std::thread::sleep(Duration::from_millis(100));
    }
}
