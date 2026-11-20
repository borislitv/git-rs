//! Kubernetes supplies advisory candidacy. Only the independently confirmed
//! PostgreSQL fence authorizes source-queue/catalog mutations.

use super::queue::Authority;
use crate::{
    Result, invalid,
    metadata::{self, text},
    process,
};
use k8s_openapi::api::coordination::v1::Lease;
use kube_client::{Api, Client};
use kubert::{
    LeaseManager,
    lease::{Claim, ClaimParams},
};
use std::{
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

#[derive(Clone, Debug)]
pub struct LeaderConfig {
    pub lease_name: String,
    pub namespace: String,
    pub pod_uid: String,
    pub lease_seconds: u64,
    pub renew_grace_seconds: u64,
    pub safety_seconds: u64,
    pub pg_lease_seconds: u64,
    pub heartbeat_seconds: u64,
}

fn number(name: &str, default: u64) -> Result<u64> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| invalid(&format!("{name} must be an unsigned integer"))),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(_) => Err(invalid(&format!("{name} must be UTF-8"))),
    }
}

impl LeaderConfig {
    pub fn from_env() -> Result<Self> {
        let config = Self {
            lease_name: std::env::var("GIT_RS_LEADER_LEASE_NAME")?,
            namespace: std::env::var("GIT_RS_LEADER_LEASE_NAMESPACE")?,
            pod_uid: std::env::var("GIT_RS_POD_UID")?,
            lease_seconds: number("GIT_RS_LEADER_LEASE_SECONDS", 30)?,
            renew_grace_seconds: number("GIT_RS_LEADER_RENEW_GRACE_SECONDS", 15)?,
            safety_seconds: number("GIT_RS_LEADER_EXPIRY_SAFETY_SECONDS", 2)?,
            pg_lease_seconds: number("GIT_RS_LEADER_PG_FENCE_LEASE_SECONDS", 20)?,
            heartbeat_seconds: number("GIT_RS_LEADER_PG_FENCE_HEARTBEAT_SECONDS", 5)?,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        fn dns(value: &str, max: usize) -> bool {
            !value.is_empty()
                && value.len() <= max
                && value.split('.').all(|label| {
                    !label.is_empty()
                        && label.len() <= 63
                        && !label.starts_with('-')
                        && !label.ends_with('-')
                        && label
                            .bytes()
                            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                })
        }
        if !dns(&self.lease_name, 253)
            || !dns(&self.namespace, 63)
            || self.namespace.contains('.')
            || self.pod_uid.is_empty()
            || self.pod_uid.len() > 128
            || !self
                .pod_uid
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(invalid("invalid named-Lease or pod identity configuration"));
        }
        // kubert bounds the WHOLE CAS operation by ten seconds; SQL calls below
        // have independent five-second absolute limits. Leave an explicit margin.
        if !(20..=300).contains(&self.lease_seconds)
            || !(1..=10).contains(&self.safety_seconds)
            || !(1..=300).contains(&self.renew_grace_seconds)
            || self.renew_grace_seconds <= 10 + self.safety_seconds
            || self.renew_grace_seconds + 10 + self.safety_seconds >= self.lease_seconds
            || !(10..=120).contains(&self.pg_lease_seconds)
            || !(1..=30).contains(&self.heartbeat_seconds)
            || self.heartbeat_seconds * 2 + 5 + self.safety_seconds >= self.pg_lease_seconds
        {
            return Err(invalid("leader lease/heartbeat margins are insufficient"));
        }
        Ok(())
    }
}

struct Observation {
    claim: Option<Arc<Claim>>,
    healthy: bool,
    authority: Weak<Authority>,
}

struct Shared {
    owner: String,
    safety: Duration,
    observation: Mutex<Observation>,
    stopping: AtomicBool,
    finished: AtomicBool,
}

impl Shared {
    fn bound(&self, claim: &Claim) -> Option<Instant> {
        let observed = Instant::now();
        let remaining = claim.remaining().checked_sub(self.safety)?;
        if remaining.is_zero() {
            return None;
        }
        observed.checked_add(remaining)
    }

    fn publish(&self, claim: Option<Arc<Claim>>, healthy: bool) {
        let mut state = self
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.claim = claim;
        state.healthy = healthy;
        self.constrain_authority(&state);
    }

    fn constrain_authority(&self, state: &Observation) {
        if let Some(authority) = state.authority.upgrade() {
            match state
                .claim
                .as_ref()
                .filter(|claim| state.healthy && claim.holder == self.owner)
                .and_then(|claim| self.bound(claim))
            {
                Some(until) => authority.constrain(until),
                None => authority.lose(),
            }
        }
    }

    fn candidate(&self) -> Option<Instant> {
        if self.stopping.load(Ordering::Acquire) {
            return None;
        }
        let state = self
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.healthy {
            return None;
        }
        state
            .claim
            .as_ref()
            .filter(|claim| claim.holder == self.owner)
            .and_then(|claim| self.bound(claim))
    }

    fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
        if let Some(authority) = self
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .authority
            .upgrade()
        {
            authority.lose();
        }
    }
}

enum Command {
    Confirm(mpsc::SyncSender<Result<Instant>>),
}

/// A separately owned, bounded lease task. No Kubernetes client is initialized by
/// frontend serving or by ordinary owner-authorized administrative enqueue.
pub struct Election {
    config: LeaderConfig,
    shared: Arc<Shared>,
    commands: tokio::sync::mpsc::Sender<Command>,
    thread: Option<JoinHandle<()>>,
}

impl Election {
    pub fn start(config: LeaderConfig) -> Result<Self> {
        Self::start_inner(config, None)
    }

    /// Explicit client boundary for real-API and fault-gateway qualification. It
    /// still uses kubert's named resourceVersion CAS; it cannot bypass PG fencing.
    pub fn with_client(config: LeaderConfig, client: Client) -> Result<Self> {
        Self::start_inner(config, Some(client))
    }

    fn start_inner(config: LeaderConfig, client: Option<Client>) -> Result<Self> {
        config.validate()?;
        let owner = format!("{}/{}", config.pod_uid, crate::scratch::identifier()?);
        let shared = Arc::new(Shared {
            owner,
            safety: Duration::from_secs(config.safety_seconds),
            observation: Mutex::new(Observation {
                claim: None,
                healthy: false,
                authority: Weak::new(),
            }),
            stopping: AtomicBool::new(false),
            finished: AtomicBool::new(false),
        });
        let (commands, receiver) = tokio::sync::mpsc::channel(1);
        let (state, settings) = (shared.clone(), config.clone());
        let thread = std::thread::Builder::new()
            .name("git-rs-election".into())
            .spawn(move || {
                let result = (|| -> Result<()> {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .max_blocking_threads(2)
                        .enable_all()
                        .build()?;
                    runtime.block_on(run_election(state.clone(), settings, client, receiver))
                })();
                if result.is_err() {
                    state.publish(None, false);
                    state.stop();
                }
            })?;
        Ok(Self {
            config,
            shared,
            commands,
            thread: Some(thread),
        })
    }

    pub fn is_candidate(&self) -> bool {
        self.shared.candidate().is_some()
    }
    pub fn stopping(&self) -> bool {
        self.shared.stopping.load(Ordering::Acquire)
    }
    pub fn request_stop(&self) {
        self.shared.stop();
    }
    pub(crate) fn view(&self) -> ElectionView {
        ElectionView(self.shared.clone())
    }

    /// Eligible standby readiness is independent of holder identity and queue
    /// emptiness. A closed task or expired observation is never ready.
    pub fn eligible(&self) -> bool {
        if self.stopping() {
            return false;
        }
        let state = self
            .shared
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.healthy
            && state
                .claim
                .as_ref()
                .and_then(|claim| self.shared.bound(claim))
                .is_some()
    }

    pub fn confirm(&self) -> Result<Instant> {
        if !self.is_candidate() {
            return Err(invalid("Kubernetes candidacy unavailable"));
        }
        let (reply, response) = mpsc::sync_channel(1);
        self.commands
            .try_send(Command::Confirm(reply))
            .map_err(|_| invalid("election confirmation busy or stopped"))?;
        let confirmed = response
            .recv_timeout(Duration::from_secs(11))
            .map_err(|_| invalid("election confirmation deadline exceeded"))??;
        let current = self
            .shared
            .candidate()
            .ok_or_else(|| invalid("Kubernetes candidacy lost during confirmation"))?;
        if Instant::now() >= confirmed {
            return Err(invalid("late Kubernetes confirmation"));
        }
        Ok(current.min(confirmed))
    }
}

#[derive(Clone)]
pub(crate) struct ElectionView(Arc<Shared>);
impl ElectionView {
    pub fn stopping(&self) -> bool {
        self.0.stopping.load(Ordering::Acquire)
    }
    pub fn stop(&self) {
        self.0.stop();
    }
    pub fn eligible(&self) -> bool {
        if self.stopping() {
            return false;
        }
        let state = self
            .0
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.healthy
            && state
                .claim
                .as_ref()
                .and_then(|claim| self.0.bound(claim))
                .is_some()
    }
    pub fn fence(&self) -> Option<u64> {
        self.0.candidate()?;
        self.0
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .authority
            .upgrade()
            .filter(|authority| authority.check().is_ok())
            .map(|authority| authority.fence)
    }
}

impl Drop for Election {
    fn drop(&mut self) {
        self.shared.stop();
        // Controller releases this owner only AFTER all work has stopped. A
        // healthy task may now vacate; uncertain/failed tasks simply expire.
        self.shared.finished.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _joined = thread.join();
        }
    }
}

async fn run_election(
    shared: Arc<Shared>,
    config: LeaderConfig,
    client: Option<Client>,
    mut commands: tokio::sync::mpsc::Receiver<Command>,
) -> Result<()> {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let signals = shared.clone();
    let signal_task = tokio::spawn(async move {
        tokio::select! { _ = term.recv() => {}, _ = interrupt.recv() => {} }
        signals.stop();
    });
    let deadlines = shared.clone();
    let expiry_task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            if deadlines.finished.load(Ordering::Acquire) {
                break;
            }
            // Confirmation and initialization may await a whole bounded API
            // operation. Their waits must not suspend the actual work's local
            // leader/job deadline checks or its cancellation notification.
            let state = deadlines
                .observation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            deadlines.constrain_authority(&state);
        }
    });
    while !shared.finished.load(Ordering::Acquire) {
        if shared.stopping.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        let attempt = async {
            // Production never infers kubeconfig or executes credential plugins.
            let client = match client.as_ref() {
                Some(client) => client.clone(),
                None => Client::try_from(kube_client::Config::incluster()?)?,
            };
            let api = Api::<Lease>::namespaced(client, &config.namespace);
            let manager = LeaseManager::init(api.clone(), &config.lease_name).await?
                .with_field_manager("git-rs-source-maintenance");
            let verifier = LeaseManager::init(api, &config.lease_name).await?;
            let params = ClaimParams { lease_duration: Duration::from_secs(config.lease_seconds),
                renew_grace_period: Duration::from_secs(config.renew_grace_seconds) };
            let (mut receiver, mut task) = manager.spawn(&shared.owner, params).await?;
            let mut tick = tokio::time::interval(Duration::from_millis(100));
            let result = loop {
                tokio::select! {
                    biased;
                    changed = receiver.changed() => {
                        if changed.is_err() { break Err(invalid("election task ended")); }
                        shared.publish(Some(receiver.borrow_and_update().clone()), true);
                    }
                    command = commands.recv() => match command {
                        Some(Command::Confirm(reply)) => {
                            let verified = verifier.sync().await;
                            let answer = match verified {
                                Ok(Some(claim)) if claim.holder==shared.owner => shared.bound(&claim)
                                    .ok_or_else(|| invalid("Kubernetes confirmation expired")),
                                _ => Err(invalid("Kubernetes confirmation unavailable")),
                            };
                            let failed = answer.is_err();
                            let _sent = reply.send(answer);
                            if failed { break Err(invalid("Kubernetes confirmation failed")); }
                        }
                        None => break Err(invalid("election controller ended")),
                    },
                    _ = tick.tick() => {
                        if shared.finished.load(Ordering::Acquire) { break Ok(()); }
                        shared.publish(Some(receiver.borrow().clone()), true);
                    }
                }
            };
            shared.publish(None, false);
            if shared.finished.load(Ordering::Acquire) {
                drop(receiver);
                if tokio::time::timeout(Duration::from_secs(11), &mut task).await.is_err() {
                    task.abort();
                    let _stopped = task.await;
                }
            } else {
                // Never voluntarily release before the independently owned work
                // has drained. Aborting a failed campaign leaves safe expiry.
                task.abort();
                let _stopped = task.await;
            }
            result
        }.await;
        shared.publish(None, false);
        if attempt.is_err() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    signal_task.abort();
    let _stopped = signal_task.await;
    expiry_task.abort();
    let _stopped = expiry_task.await;
    Ok(())
}

/// One process/campaign's PostgreSQL authority. The election task and actual CPU
/// runtime are owned outside jobs, so dropping a job never joins its own worker.
pub struct LeaderSession {
    election: Election,
    authority: Option<Arc<Authority>>,
}

impl LeaderSession {
    pub fn new(config: LeaderConfig) -> Result<Self> {
        Ok(Self {
            election: Election::start(config)?,
            authority: None,
        })
    }
    pub fn election(&self) -> &Election {
        &self.election
    }
    pub(crate) fn authority(&self) -> Option<Arc<Authority>> {
        self.authority
            .as_ref()
            .filter(|authority| authority.check().is_ok())
            .cloned()
    }

    pub fn acquire_fence(&mut self) -> Result<bool> {
        if self.authority().is_some() {
            return Ok(true);
        }
        if !self.election.is_candidate() {
            return Ok(false);
        }
        let _before = self.election.confirm()?;
        let campaign = crate::scratch::identifier()?;
        let owner = &self.election.shared.owner;
        let started = Instant::now();
        let query = format!(
            "BEGIN; SET LOCAL synchronous_commit=on; SELECT maintenance_acquire({},{}::uuid,{}); COMMIT;",
            text(owner),
            text(&campaign),
            self.election.config.pg_lease_seconds
        );
        let row = process::Operation::new(5)
            .enter(|| metadata::sql(&query))
            .or_else(|_| {
                // The same live campaign confirms the unknown acquisition with a
                // WAL-producing counter update. SQL rejects an expired campaign.
                process::Operation::new(5).enter(|| metadata::sql(&query))
            })?;
        if row.trim().is_empty() {
            return Ok(false);
        }
        let fence: u64 = row.trim().parse()?;
        if fence == 0 {
            return Err(invalid("invalid maintenance fence"));
        }
        let candidate = self.election.confirm()?;
        let deadline = (started
            + Duration::from_secs(
                self.election.config.pg_lease_seconds - self.election.config.safety_seconds,
            ))
        .min(candidate);
        if Instant::now() >= deadline {
            return Err(invalid("maintenance fence acquired too late"));
        }
        let authority = Authority::new(owner.clone(), campaign, fence, deadline);
        self.election
            .shared
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .authority = Arc::downgrade(&authority);
        authority.constrain(
            self.election
                .shared
                .candidate()
                .ok_or_else(|| invalid("candidacy lost after fence acquisition"))?,
        );
        authority.check()?;
        self.authority = Some(authority);
        Ok(true)
    }

    pub fn renew_fence(&mut self) -> Result<()> {
        let authority = self
            .authority()
            .ok_or_else(|| invalid("maintenance fence unavailable"))?;
        let started = Instant::now();
        let result = (|| {
            self.election
                .shared
                .candidate()
                .ok_or_else(|| invalid("Kubernetes candidacy lost"))?;
            process::Operation::new(5).enter(|| metadata::sql(&format!(
                "BEGIN; SET LOCAL synchronous_commit=on; SELECT maintenance_renew({},{}); COMMIT;",
                authority.args(), self.election.config.pg_lease_seconds)))?;
            let candidate = self
                .election
                .shared
                .candidate()
                .ok_or_else(|| invalid("Kubernetes candidacy lost during renewal"))?;
            authority.renewed(
                (started
                    + Duration::from_secs(
                        self.election.config.pg_lease_seconds - self.election.config.safety_seconds,
                    ))
                .min(candidate),
            )
        })();
        if result.is_err() {
            authority.lose();
        }
        result
    }

    pub fn release_fence(&mut self) -> Result<()> {
        let Some(authority) = self.authority.take() else {
            return Ok(());
        };
        authority.lose();
        process::Operation::new(5).enter(|| {
            metadata::sql(&format!(
                "BEGIN; SET LOCAL synchronous_commit=on; SELECT maintenance_release({}); COMMIT;",
                authority.args()
            ))
        })?;
        Ok(())
    }
}

impl Drop for LeaderSession {
    fn drop(&mut self) {
        let _released = self.release_fence();
    }
}
