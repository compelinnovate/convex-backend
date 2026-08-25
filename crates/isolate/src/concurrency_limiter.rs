use std::{
    array,
    collections::VecDeque,
    mem,
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};

use common::{
    runtime::Runtime,
    types::ActiveJavascriptClass,
};
use fastrace::{
    func_path,
    Span,
};
use futures::Future;
use parking_lot::Mutex;
use slab::Slab;
use tokio::sync::oneshot;

use crate::metrics::{
    concurrency_permit_acquire_timer,
    decrement_active_javascript_occupancy,
    decrement_active_javascript_waiters,
    increment_active_javascript_occupancy,
    increment_active_javascript_waiters,
    initialize_active_javascript_metrics,
    log_concurrency_permit_used,
};

/// Whether active JavaScript is starting or resuming after an asynchronous
/// wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConcurrencyPermitPhase {
    Initial,
    Resume,
}

/// Limits how many isolate threads can actively run JavaScript at the same
/// time.
///
/// When class minimums are configured, dependency work runs first because it
/// releases an isolate-holding ancestor. Protected and degradable work each
/// receive a non-preemptive service floor under contention and borrow all
/// capacity the other class does not need. Elastic occupancy is balanced
/// between the two classes, and resumptions precede initial starts within the
/// selected class. Without class minimums, the limiter retains its phase-only
/// compatibility policy.
#[derive(Clone, Debug)]
pub struct ConcurrencyLimiter {
    inner: Arc<ConcurrencyLimiterInner>,
}

#[derive(Debug)]
struct ConcurrencyLimiterInner {
    tracker: Mutex<ActivePermitsTracker>,
    max_permits: usize,
    protected_minimum: usize,
    degradable_minimum: usize,
}

#[derive(Debug)]
enum WaiterState {
    Waiting(oneshot::Sender<()>),
    Granted,
}

#[derive(Debug)]
struct Waiter {
    queue: WaiterQueue,
    state: WaiterState,
}

#[derive(Debug)]
struct ActivePermit {
    client_id: Arc<String>,
    started: Instant,
    class: ActiveJavascriptClass,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WaiterQueue {
    DependencyResume,
    DependencyInitial,
    ProtectedResume,
    ProtectedInitial,
    DegradableResume,
    DegradableInitial,
}

impl WaiterQueue {
    const COUNT: usize = 6;

    fn new(class: ActiveJavascriptClass, phase: ConcurrencyPermitPhase) -> Self {
        match (class, phase) {
            (ActiveJavascriptClass::Dependency, ConcurrencyPermitPhase::Resume) => {
                Self::DependencyResume
            },
            (ActiveJavascriptClass::Dependency, ConcurrencyPermitPhase::Initial) => {
                Self::DependencyInitial
            },
            (ActiveJavascriptClass::Protected, ConcurrencyPermitPhase::Resume) => {
                Self::ProtectedResume
            },
            (ActiveJavascriptClass::Protected, ConcurrencyPermitPhase::Initial) => {
                Self::ProtectedInitial
            },
            (ActiveJavascriptClass::Degradable, ConcurrencyPermitPhase::Resume) => {
                Self::DegradableResume
            },
            (ActiveJavascriptClass::Degradable, ConcurrencyPermitPhase::Initial) => {
                Self::DegradableInitial
            },
        }
    }

    fn index(self) -> usize {
        match self {
            Self::DependencyResume => 0,
            Self::DependencyInitial => 1,
            Self::ProtectedResume => 2,
            Self::ProtectedInitial => 3,
            Self::DegradableResume => 4,
            Self::DegradableInitial => 5,
        }
    }

    fn class(self) -> ActiveJavascriptClass {
        match self {
            Self::DependencyResume | Self::DependencyInitial => ActiveJavascriptClass::Dependency,
            Self::ProtectedResume | Self::ProtectedInitial => ActiveJavascriptClass::Protected,
            Self::DegradableResume | Self::DegradableInitial => ActiveJavascriptClass::Degradable,
        }
    }

    fn phase(self) -> &'static str {
        match self {
            Self::DependencyResume | Self::ProtectedResume | Self::DegradableResume => "resume",
            Self::DependencyInitial | Self::ProtectedInitial | Self::DegradableInitial => "initial",
        }
    }
}

#[derive(Debug)]
struct ActivePermitsTracker {
    active_permits: Slab<ActivePermit>,
    active_by_class: [usize; 3],
    granted_by_class: [usize; 3],
    waiters: Slab<Waiter>,
    queues: [VecDeque<usize>; WaiterQueue::COUNT],
    next_tied_class: ActiveJavascriptClass,
}

impl ConcurrencyLimiter {
    pub fn new(max_concurrency: usize) -> Self {
        Self::new_with_class_minimums(max_concurrency, 0, 0)
    }

    pub fn new_with_class_minimums(
        max_concurrency: usize,
        protected_minimum: usize,
        degradable_minimum: usize,
    ) -> Self {
        assert!(
            max_concurrency > 0,
            "max_concurrency must be greater than zero"
        );
        assert_eq!(
            protected_minimum == 0,
            degradable_minimum == 0,
            "protected and degradable minimums must be enabled together"
        );
        assert!(
            protected_minimum == 0 || max_concurrency != usize::MAX,
            "active-JavaScript class minimums require finite total capacity"
        );
        assert!(
            protected_minimum
                .checked_add(degradable_minimum)
                .is_some_and(|sum| sum <= max_concurrency),
            "active-JavaScript class minimums exceed total capacity"
        );
        let limiter = Self {
            inner: Arc::new(ConcurrencyLimiterInner {
                tracker: Mutex::new(ActivePermitsTracker {
                    active_permits: Slab::new(),
                    active_by_class: [0; 3],
                    granted_by_class: [0; 3],
                    waiters: Slab::new(),
                    queues: array::from_fn(|_| VecDeque::new()),
                    next_tied_class: ActiveJavascriptClass::Degradable,
                }),
                max_permits: max_concurrency,
                protected_minimum,
                degradable_minimum,
            }),
        };
        initialize_active_javascript_metrics(
            if max_concurrency == usize::MAX {
                0
            } else {
                max_concurrency
            },
            protected_minimum,
            degradable_minimum,
        );
        limiter
    }

    pub fn unlimited() -> Self {
        Self::new(usize::MAX)
    }

    pub fn active_permits(&self) -> usize {
        self.inner.tracker.lock().active_permits.len()
    }

    #[cfg(test)]
    pub(crate) fn waiting_permits(
        &self,
        class: ActiveJavascriptClass,
        phase: ConcurrencyPermitPhase,
    ) -> usize {
        let queue = WaiterQueue::new(class, phase);
        self.inner.tracker.lock().queues[queue.index()].len()
    }

    pub fn max_permits(&self) -> Option<usize> {
        if self.inner.max_permits == usize::MAX {
            None
        } else {
            Some(self.inner.max_permits)
        }
    }

    pub(crate) fn class_aware_admission_enabled(&self) -> bool {
        self.inner.protected_minimum > 0
    }

    // If a client uses a thread for too long. We still want to log periodically.
    pub fn go_log<RT: Runtime>(
        &self,
        rt: RT,
        frequency: Duration,
    ) -> impl Future<Output = ()> + use<RT> {
        let inner = self.inner.clone();
        async move {
            loop {
                rt.wait(frequency).await;
                let current_permits = inner.tracker.lock().reset_start_time();
                for (client_id, start_time) in current_permits {
                    if start_time.elapsed() >= frequency {
                        tracing::warn!(
                            "{client_id} held concurrency semaphore for more than {frequency:?}"
                        );
                    }
                    log_concurrency_permit_used(client_id, start_time.elapsed());
                }
            }
        }
    }

    /// Compatibility entry point for callers without an explicit service class.
    pub async fn acquire(&self, client_id: Arc<String>, high_priority: bool) -> ConcurrencyPermit {
        self.acquire_with_class(
            client_id,
            ActiveJavascriptClass::Protected,
            if high_priority {
                ConcurrencyPermitPhase::Resume
            } else {
                ConcurrencyPermitPhase::Initial
            },
        )
        .await
    }

    pub(crate) async fn acquire_with_class(
        &self,
        client_id: Arc<String>,
        class: ActiveJavascriptClass,
        phase: ConcurrencyPermitPhase,
    ) -> ConcurrencyPermit {
        // Zero minimums disable class-aware admission completely. Collapse all
        // declarations so the compatibility contract remains phase-only and
        // no class changes ordering without an explicit opt-in.
        let class = if !self.class_aware_admission_enabled() {
            ActiveJavascriptClass::Protected
        } else {
            class
        };
        let queue = WaiterQueue::new(class, phase);
        let timer = concurrency_permit_acquire_timer(class, queue.phase());
        let immediate_permit_id = {
            let mut tracker = self.inner.tracker.lock();
            // Avoid waiter machinery on the uncontended path, but never let a
            // new arrival barge ahead of an already queued request.
            if tracker.total_occupancy() < self.inner.max_permits
                && tracker.queues.iter().all(VecDeque::is_empty)
            {
                increment_active_javascript_occupancy(class);
                Some(tracker.register(client_id.clone(), class))
            } else {
                None
            }
        };
        if let Some(permit_id) = immediate_permit_id {
            timer.finish(true);
            return ConcurrencyPermit {
                permit_id,
                limiter: self.clone(),
                client_id,
                class,
            };
        }

        let (sender, receiver) = oneshot::channel();
        let waiter_id = {
            let mut tracker = self.inner.tracker.lock();
            let waiter_id = tracker.waiters.insert(Waiter {
                queue,
                state: WaiterState::Waiting(sender),
            });
            tracker.queues[queue.index()].push_back(waiter_id);
            increment_active_javascript_waiters(class, queue.phase());
            tracker.dispatch(&self.inner);
            waiter_id
        };
        let mut guard = WaiterGuard {
            limiter: self.clone(),
            waiter_id: Some(waiter_id),
            receiver,
        };
        let _span = Span::enter_with_local_parent(func_path!());
        (&mut guard.receiver)
            .await
            .expect("active-JavaScript waiter disappeared before its grant");

        let permit_id = {
            let mut tracker = self.inner.tracker.lock();
            // The acquisition now owns the grant. Disarm cancellation at the
            // same transition before removing the waiter from its slab.
            let waiter_id = guard
                .waiter_id
                .take()
                .expect("active-JavaScript waiter guard was already disarmed");
            let waiter = tracker.waiters.remove(waiter_id);
            assert!(
                matches!(waiter.state, WaiterState::Granted),
                "active-JavaScript waiter awoke without a grant"
            );
            let class_index = class_index(class);
            tracker.granted_by_class[class_index] = tracker.granted_by_class[class_index]
                .checked_sub(1)
                .expect("active-JavaScript granted count underflow");
            tracker.register(client_id.clone(), class)
        };
        timer.finish(true);
        ConcurrencyPermit {
            permit_id,
            limiter: self.clone(),
            client_id,
            class,
        }
    }

    pub(crate) async fn acquire_internal_dependency(
        &self,
        client_id: Arc<String>,
    ) -> ConcurrencyPermit {
        // In compatibility mode, direct internal callbacks use the resume
        // phase so they precede external initial starts. With service floors
        // enabled, the dependency class supplies that priority and the
        // callback remains an initial start.
        let phase = if !self.class_aware_admission_enabled() {
            ConcurrencyPermitPhase::Resume
        } else {
            ConcurrencyPermitPhase::Initial
        };
        self.acquire_with_class(client_id, ActiveJavascriptClass::Dependency, phase)
            .await
    }

    fn cancel_waiter(&self, waiter_id: usize) {
        let mut tracker = self.inner.tracker.lock();
        let waiter = tracker.waiters.remove(waiter_id);
        match waiter.state {
            WaiterState::Waiting(_) => {
                let queue = &mut tracker.queues[waiter.queue.index()];
                let position = queue
                    .iter()
                    .position(|queued_id| *queued_id == waiter_id)
                    .expect("waiting active-JavaScript waiter missing from its queue");
                queue.remove(position);
                decrement_active_javascript_waiters(waiter.queue.class(), waiter.queue.phase());
            },
            WaiterState::Granted => {
                let class_index = class_index(waiter.queue.class());
                tracker.granted_by_class[class_index] = tracker.granted_by_class[class_index]
                    .checked_sub(1)
                    .expect("active-JavaScript granted count underflow");
                decrement_active_javascript_occupancy(waiter.queue.class());
            },
        }
        tracker.dispatch(&self.inner);
    }
}

struct WaiterGuard {
    limiter: ConcurrencyLimiter,
    waiter_id: Option<usize>,
    // `Drop` runs before fields are destroyed, so cancellation removes a
    // waiting or granted entry while its receiver can still accept a grant.
    receiver: oneshot::Receiver<()>,
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        if let Some(waiter_id) = self.waiter_id.take() {
            self.limiter.cancel_waiter(waiter_id);
        }
    }
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
struct PermitId(usize);

fn class_index(class: ActiveJavascriptClass) -> usize {
    match class {
        ActiveJavascriptClass::Dependency => 0,
        ActiveJavascriptClass::Protected => 1,
        ActiveJavascriptClass::Degradable => 2,
    }
}

impl ActivePermitsTracker {
    fn register(&mut self, client_id: Arc<String>, class: ActiveJavascriptClass) -> PermitId {
        self.active_by_class[class_index(class)] += 1;
        PermitId(self.active_permits.insert(ActivePermit {
            client_id,
            started: Instant::now(),
            class,
        }))
    }

    fn deregister(&mut self, id: PermitId) -> (Duration, ActiveJavascriptClass) {
        let permit = self.active_permits.remove(id.0);
        let class_index = class_index(permit.class);
        self.active_by_class[class_index] = self.active_by_class[class_index]
            .checked_sub(1)
            .expect("active-JavaScript class count underflow");
        decrement_active_javascript_occupancy(permit.class);
        (permit.started.elapsed(), permit.class)
    }

    fn total_occupancy(&self) -> usize {
        self.active_permits.len() + self.granted_by_class.iter().sum::<usize>()
    }

    fn class_occupancy(&self, class: ActiveJavascriptClass) -> usize {
        let index = class_index(class);
        self.active_by_class[index] + self.granted_by_class[index]
    }

    fn class_has_waiter(&self, class: ActiveJavascriptClass) -> bool {
        match class {
            ActiveJavascriptClass::Dependency => {
                !self.queues[WaiterQueue::DependencyResume.index()].is_empty()
                    || !self.queues[WaiterQueue::DependencyInitial.index()].is_empty()
            },
            ActiveJavascriptClass::Protected => {
                !self.queues[WaiterQueue::ProtectedResume.index()].is_empty()
                    || !self.queues[WaiterQueue::ProtectedInitial.index()].is_empty()
            },
            ActiveJavascriptClass::Degradable => {
                !self.queues[WaiterQueue::DegradableResume.index()].is_empty()
                    || !self.queues[WaiterQueue::DegradableInitial.index()].is_empty()
            },
        }
    }

    fn select_class(&mut self, inner: &ConcurrencyLimiterInner) -> Option<ActiveJavascriptClass> {
        if inner.protected_minimum == 0 {
            // `acquire_with_class` collapses every declaration before queueing
            // in compatibility mode. Keep that the only compatibility rule so
            // future call paths cannot silently introduce class priority.
            assert!(
                !self.class_has_waiter(ActiveJavascriptClass::Dependency)
                    && !self.class_has_waiter(ActiveJavascriptClass::Degradable),
                "class-specific active-JavaScript waiter escaped compatibility collapse"
            );
            return self
                .class_has_waiter(ActiveJavascriptClass::Protected)
                .then_some(ActiveJavascriptClass::Protected);
        }

        if self.class_has_waiter(ActiveJavascriptClass::Dependency) {
            return Some(ActiveJavascriptClass::Dependency);
        }

        let protected_waiting = self.class_has_waiter(ActiveJavascriptClass::Protected);
        let degradable_waiting = self.class_has_waiter(ActiveJavascriptClass::Degradable);
        match (protected_waiting, degradable_waiting) {
            (false, false) => return None,
            (true, false) => return Some(ActiveJavascriptClass::Protected),
            (false, true) => return Some(ActiveJavascriptClass::Degradable),
            (true, true) => {},
        }

        let protected_occupancy = self.class_occupancy(ActiveJavascriptClass::Protected);
        let degradable_occupancy = self.class_occupancy(ActiveJavascriptClass::Degradable);
        let protected_below = protected_occupancy < inner.protected_minimum;
        let degradable_below = degradable_occupancy < inner.degradable_minimum;
        match (protected_below, degradable_below) {
            (true, false) => Some(ActiveJavascriptClass::Protected),
            (false, true) => Some(ActiveJavascriptClass::Degradable),
            (true, true) => {
                // Compare satisfaction ratios without floating point. Filling
                // floors proportionally prevents the larger floor from delaying
                // all progress in the smaller class during a cold burst.
                let protected_scaled =
                    protected_occupancy as u128 * inner.degradable_minimum as u128;
                let degradable_scaled =
                    degradable_occupancy as u128 * inner.protected_minimum as u128;
                if protected_scaled < degradable_scaled {
                    Some(ActiveJavascriptClass::Protected)
                } else if degradable_scaled < protected_scaled {
                    Some(ActiveJavascriptClass::Degradable)
                } else {
                    Some(self.take_next_tied_class())
                }
            },
            (false, false) => {
                let protected_elastic = protected_occupancy - inner.protected_minimum;
                let degradable_elastic = degradable_occupancy - inner.degradable_minimum;
                // Balance elastic occupancy, not only the handoff sequence.
                // Otherwise a class that initially borrowed the whole gate can
                // lose every elastic slot as its incumbents finish, even while
                // both classes remain continuously runnable.
                if protected_elastic < degradable_elastic {
                    Some(ActiveJavascriptClass::Protected)
                } else if degradable_elastic < protected_elastic {
                    Some(ActiveJavascriptClass::Degradable)
                } else {
                    Some(self.take_next_tied_class())
                }
            },
        }
    }

    fn take_next_tied_class(&mut self) -> ActiveJavascriptClass {
        let selected = self.next_tied_class;
        self.next_tied_class = match selected {
            ActiveJavascriptClass::Protected => ActiveJavascriptClass::Degradable,
            ActiveJavascriptClass::Degradable => ActiveJavascriptClass::Protected,
            ActiveJavascriptClass::Dependency => {
                panic!("dependency cannot be the next tied active-JavaScript class")
            },
        };
        selected
    }

    fn pop_waiter(&mut self, class: ActiveJavascriptClass) -> Option<usize> {
        let queues = match class {
            ActiveJavascriptClass::Dependency => [
                WaiterQueue::DependencyResume,
                WaiterQueue::DependencyInitial,
            ],
            ActiveJavascriptClass::Protected => {
                [WaiterQueue::ProtectedResume, WaiterQueue::ProtectedInitial]
            },
            ActiveJavascriptClass::Degradable => [
                WaiterQueue::DegradableResume,
                WaiterQueue::DegradableInitial,
            ],
        };
        for queue in queues {
            if let Some(waiter_id) = self.queues[queue.index()].pop_front() {
                return Some(waiter_id);
            }
        }
        None
    }

    fn dispatch(&mut self, inner: &ConcurrencyLimiterInner) {
        while self.total_occupancy() < inner.max_permits {
            let Some(class) = self.select_class(inner) else {
                return;
            };
            let waiter_id = self
                .pop_waiter(class)
                .expect("selected active-JavaScript class has no waiter");
            let waiter = self
                .waiters
                .get_mut(waiter_id)
                .expect("queued active-JavaScript waiter is missing");
            let sender = match mem::replace(&mut waiter.state, WaiterState::Granted) {
                WaiterState::Waiting(sender) => sender,
                WaiterState::Granted => {
                    panic!("granted active-JavaScript waiter remained queued")
                },
            };
            self.granted_by_class[class_index(class)] += 1;
            decrement_active_javascript_waiters(class, waiter.queue.phase());
            increment_active_javascript_occupancy(class);
            sender
                .send(())
                .expect("active-JavaScript receiver closed before waiter cancellation");
        }
    }

    fn reset_start_time(&mut self) -> Vec<(Arc<String>, Instant)> {
        let now = Instant::now();
        self.active_permits
            .iter_mut()
            .map(|(_, permit)| {
                (
                    permit.client_id.clone(),
                    mem::replace(&mut permit.started, now),
                )
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct ConcurrencyPermit {
    permit_id: PermitId,
    limiter: ConcurrencyLimiter,
    client_id: Arc<String>,
    class: ActiveJavascriptClass,
}

impl ConcurrencyPermit {
    pub async fn with_suspend<'a, T>(
        self,
        f: impl Future<Output = T> + 'a,
    ) -> (T, ConcurrencyPermit) {
        let regain = self.suspend();
        let result = f.await;
        let permit = regain.acquire().await;
        (result, permit)
    }

    pub fn suspend(self) -> SuspendedPermit {
        let client_id = self.client_id.clone();
        let limiter = self.limiter.clone();
        let class = self.class;
        // The service class follows the execution across every async wait. A
        // resume classified as a fresh protected start can starve an admitted
        // degradable tree after the tree has already consumed lifetime capacity.
        SuspendedPermit {
            client_id,
            limiter,
            class,
        }
    }

    pub fn limiter(&self) -> &ConcurrencyLimiter {
        &self.limiter
    }
}

impl Drop for ConcurrencyPermit {
    fn drop(&mut self) {
        let mut tracker = self.limiter.inner.tracker.lock();
        let (duration, class) = tracker.deregister(self.permit_id);
        assert_eq!(class, self.class, "active-JavaScript permit class drifted");
        tracker.dispatch(&self.limiter.inner);
        drop(tracker);
        log_concurrency_permit_used(self.client_id.clone(), duration);
    }
}

pub struct SuspendedPermit {
    limiter: ConcurrencyLimiter,
    client_id: Arc<String>,
    class: ActiveJavascriptClass,
}

impl SuspendedPermit {
    pub async fn acquire(self) -> ConcurrencyPermit {
        self.limiter
            .acquire_with_class(self.client_id, self.class, ConcurrencyPermitPhase::Resume)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        sync::Arc,
        task::Poll,
    };

    use common::types::ActiveJavascriptClass;
    use futures::{
        poll,
        FutureExt,
    };

    use super::{
        ConcurrencyLimiter,
        ConcurrencyPermit,
        ConcurrencyPermitPhase,
    };

    fn acquire<'a>(
        limiter: &'a ConcurrencyLimiter,
        name: impl Into<String>,
        class: ActiveJavascriptClass,
        phase: ConcurrencyPermitPhase,
    ) -> impl Future<Output = ConcurrencyPermit> + 'a {
        let name = name.into();
        limiter.acquire_with_class(Arc::new(name), class, phase)
    }

    #[tokio::test]
    async fn resumption_overtakes_initial_start_without_class_minimums() {
        let limiter = ConcurrencyLimiter::new(1);
        let initial_permit = limiter.acquire(Arc::new("initial".to_owned()), false).await;
        let mut initial_waiter =
            Box::pin(limiter.acquire(Arc::new("initial-waiter".to_owned()), false));
        assert!(matches!(poll!(initial_waiter.as_mut()), Poll::Pending));
        let mut resume_waiter =
            Box::pin(limiter.acquire(Arc::new("resume-waiter".to_owned()), true));
        assert!(matches!(poll!(resume_waiter.as_mut()), Poll::Pending));

        drop(initial_permit);

        let Poll::Ready(resume_permit) = poll!(resume_waiter.as_mut()) else {
            panic!("resume waiter was not notified first");
        };
        assert!(matches!(poll!(initial_waiter.as_mut()), Poll::Pending));
        drop(resume_permit);
        let Poll::Ready(initial_waiter_permit) = poll!(initial_waiter.as_mut()) else {
            panic!("initial waiter was not notified after the resume permit dropped");
        };
        drop(initial_waiter_permit);
    }

    #[test]
    #[should_panic(expected = "active-JavaScript class minimums require finite total capacity")]
    fn class_minimums_require_finite_capacity() {
        let _ = ConcurrencyLimiter::new_with_class_minimums(usize::MAX, 1, 1);
    }

    #[tokio::test]
    async fn compatibility_mode_collapses_classes_and_preserves_phase_fifo() {
        let limiter = ConcurrencyLimiter::new(1);
        let held = limiter.acquire(Arc::new("held".to_owned()), false).await;
        let mut first_initial = Box::pin(acquire(
            &limiter,
            "first-initial",
            ActiveJavascriptClass::Dependency,
            ConcurrencyPermitPhase::Initial,
        ));
        let mut second_initial = Box::pin(acquire(
            &limiter,
            "second-initial",
            ActiveJavascriptClass::Degradable,
            ConcurrencyPermitPhase::Initial,
        ));
        let mut first_resume = Box::pin(acquire(
            &limiter,
            "first-resume",
            ActiveJavascriptClass::Degradable,
            ConcurrencyPermitPhase::Resume,
        ));
        let mut second_resume = Box::pin(acquire(
            &limiter,
            "second-resume",
            ActiveJavascriptClass::Dependency,
            ConcurrencyPermitPhase::Resume,
        ));
        assert!(matches!(poll!(first_initial.as_mut()), Poll::Pending));
        assert!(matches!(poll!(second_initial.as_mut()), Poll::Pending));
        assert!(matches!(poll!(first_resume.as_mut()), Poll::Pending));
        assert!(matches!(poll!(second_resume.as_mut()), Poll::Pending));

        drop(held);
        let Poll::Ready(first_resume_permit) = poll!(first_resume.as_mut()) else {
            panic!("first resumption was not granted first");
        };
        assert_eq!(first_resume_permit.class, ActiveJavascriptClass::Protected);
        assert!(matches!(poll!(second_resume.as_mut()), Poll::Pending));
        assert!(matches!(poll!(first_initial.as_mut()), Poll::Pending));
        drop(first_resume_permit);

        let Poll::Ready(second_resume_permit) = poll!(second_resume.as_mut()) else {
            panic!("resumptions did not remain FIFO");
        };
        assert_eq!(second_resume_permit.class, ActiveJavascriptClass::Protected);
        assert!(matches!(poll!(first_initial.as_mut()), Poll::Pending));
        drop(second_resume_permit);

        let Poll::Ready(first_initial_permit) = poll!(first_initial.as_mut()) else {
            panic!("first initial start did not follow resumptions");
        };
        assert_eq!(first_initial_permit.class, ActiveJavascriptClass::Protected);
        assert!(matches!(poll!(second_initial.as_mut()), Poll::Pending));
        drop(first_initial_permit);

        let Poll::Ready(second_initial_permit) = poll!(second_initial.as_mut()) else {
            panic!("initial starts did not remain FIFO");
        };
        assert_eq!(
            second_initial_permit.class,
            ActiveJavascriptClass::Protected
        );
        drop(second_initial_permit);
    }

    #[tokio::test]
    async fn dependency_precedes_both_service_classes() {
        let limiter = ConcurrencyLimiter::new_with_class_minimums(2, 1, 1);
        let held = limiter.acquire(Arc::new("held".to_owned()), false).await;
        let second_held = limiter
            .acquire(Arc::new("second-held".to_owned()), false)
            .await;
        let mut protected = Box::pin(acquire(
            &limiter,
            "protected",
            ActiveJavascriptClass::Protected,
            ConcurrencyPermitPhase::Resume,
        ));
        let mut degradable = Box::pin(acquire(
            &limiter,
            "degradable",
            ActiveJavascriptClass::Degradable,
            ConcurrencyPermitPhase::Resume,
        ));
        let mut dependency = Box::pin(acquire(
            &limiter,
            "dependency",
            ActiveJavascriptClass::Dependency,
            ConcurrencyPermitPhase::Initial,
        ));
        assert!(protected.as_mut().now_or_never().is_none());
        assert!(degradable.as_mut().now_or_never().is_none());
        assert!(dependency.as_mut().now_or_never().is_none());

        drop(held);
        let dependency_permit = dependency.await;
        assert!(protected.as_mut().now_or_never().is_none());
        assert!(degradable.as_mut().now_or_never().is_none());
        drop(dependency_permit);
        drop(second_held);
    }

    #[tokio::test]
    async fn degradable_uses_ordinary_admission_without_class_minimums() {
        let limiter = ConcurrencyLimiter::new(1);
        let permit = acquire(
            &limiter,
            "degradable",
            ActiveJavascriptClass::Degradable,
            ConcurrencyPermitPhase::Initial,
        )
        .await;
        assert_eq!(permit.class, ActiveJavascriptClass::Protected);
    }

    #[tokio::test]
    async fn resumption_precedes_initial_start_within_a_class() {
        let limiter = ConcurrencyLimiter::new_with_class_minimums(2, 1, 1);
        let first = limiter.acquire(Arc::new("first".to_owned()), false).await;
        let second = limiter.acquire(Arc::new("second".to_owned()), false).await;
        let mut initial = Box::pin(acquire(
            &limiter,
            "initial",
            ActiveJavascriptClass::Degradable,
            ConcurrencyPermitPhase::Initial,
        ));
        let mut resume = Box::pin(acquire(
            &limiter,
            "resume",
            ActiveJavascriptClass::Degradable,
            ConcurrencyPermitPhase::Resume,
        ));
        assert!(matches!(poll!(initial.as_mut()), Poll::Pending));
        assert!(matches!(poll!(resume.as_mut()), Poll::Pending));

        drop(first);
        let Poll::Ready(resumed) = poll!(resume.as_mut()) else {
            panic!("degradable resumption did not precede its initial start");
        };
        assert!(matches!(poll!(initial.as_mut()), Poll::Pending));
        drop(resumed);
        drop(second);
    }

    #[tokio::test]
    async fn class_minimums_are_work_conserving_and_balance_elastic_occupancy() {
        let limiter = ConcurrencyLimiter::new_with_class_minimums(6, 1, 3);
        let held: Vec<_> = futures::future::join_all(
            (0..6).map(|index| limiter.acquire(Arc::new(format!("held-{index}")), false)),
        )
        .await;
        let mut protected: Vec<_> = (0..6)
            .map(|index| {
                Box::pin(acquire(
                    &limiter,
                    format!("protected-{index}"),
                    ActiveJavascriptClass::Protected,
                    ConcurrencyPermitPhase::Initial,
                ))
            })
            .collect();
        let mut degradable: Vec<_> = (0..6)
            .map(|index| {
                Box::pin(acquire(
                    &limiter,
                    format!("degradable-{index}"),
                    ActiveJavascriptClass::Degradable,
                    ConcurrencyPermitPhase::Initial,
                ))
            })
            .collect();
        for waiter in protected.iter_mut().chain(degradable.iter_mut()) {
            assert!(waiter.as_mut().now_or_never().is_none());
        }

        drop(held);
        let mut protected_permits = Vec::new();
        let mut degradable_permits = Vec::new();
        for waiter in &mut protected {
            if let Some(permit) = waiter.as_mut().now_or_never() {
                protected_permits.push(permit);
            }
        }
        for waiter in &mut degradable {
            if let Some(permit) = waiter.as_mut().now_or_never() {
                degradable_permits.push(permit);
            }
        }
        assert_eq!((protected_permits.len(), degradable_permits.len()), (2, 4));
        drop(protected_permits);
        drop(degradable_permits);
    }

    #[tokio::test]
    async fn exact_floor_and_elastic_ties_alternate() {
        let limiter = ConcurrencyLimiter::new_with_class_minimums(6, 1, 1);
        let mut dependencies = Vec::new();
        for index in 0..6 {
            dependencies.push(
                acquire(
                    &limiter,
                    format!("dependency-{index}"),
                    ActiveJavascriptClass::Dependency,
                    ConcurrencyPermitPhase::Initial,
                )
                .await,
            );
        }
        let mut first_protected = Box::pin(acquire(
            &limiter,
            "first-protected",
            ActiveJavascriptClass::Protected,
            ConcurrencyPermitPhase::Initial,
        ));
        let mut second_protected = Box::pin(acquire(
            &limiter,
            "second-protected",
            ActiveJavascriptClass::Protected,
            ConcurrencyPermitPhase::Initial,
        ));
        let mut first_degradable = Box::pin(acquire(
            &limiter,
            "first-degradable",
            ActiveJavascriptClass::Degradable,
            ConcurrencyPermitPhase::Initial,
        ));
        let mut second_degradable = Box::pin(acquire(
            &limiter,
            "second-degradable",
            ActiveJavascriptClass::Degradable,
            ConcurrencyPermitPhase::Initial,
        ));
        assert!(matches!(poll!(first_protected.as_mut()), Poll::Pending));
        assert!(matches!(poll!(second_protected.as_mut()), Poll::Pending));
        assert!(matches!(poll!(first_degradable.as_mut()), Poll::Pending));
        assert!(matches!(poll!(second_degradable.as_mut()), Poll::Pending));

        drop(dependencies.pop());
        let Poll::Ready(first_degradable_permit) = poll!(first_degradable.as_mut()) else {
            panic!("underfilled-floor tie did not choose degradable");
        };

        drop(dependencies.pop());
        let Poll::Ready(first_protected_permit) = poll!(first_protected.as_mut()) else {
            panic!("protected floor did not receive the next grant");
        };

        drop(dependencies.pop());
        let Poll::Ready(second_protected_permit) = poll!(second_protected.as_mut()) else {
            panic!("elastic tie did not alternate to protected");
        };

        drop(dependencies.pop());
        let Poll::Ready(second_degradable_permit) = poll!(second_degradable.as_mut()) else {
            panic!("elastic occupancy did not rebalance toward degradable");
        };

        drop(first_degradable_permit);
        drop(first_protected_permit);
        drop(second_protected_permit);
        drop(second_degradable_permit);
    }

    #[tokio::test]
    async fn cancelling_waiting_requests_preserves_capacity() {
        let limiter = ConcurrencyLimiter::new_with_class_minimums(2, 1, 1);
        let first = limiter.acquire(Arc::new("first".to_owned()), false).await;
        let second = limiter.acquire(Arc::new("second".to_owned()), false).await;
        let mut cancelled = Box::pin(acquire(
            &limiter,
            "cancelled",
            ActiveJavascriptClass::Degradable,
            ConcurrencyPermitPhase::Resume,
        ));
        assert!(cancelled.as_mut().now_or_never().is_none());
        drop(cancelled);
        drop(first);
        drop(second);

        let replacement = acquire(
            &limiter,
            "replacement",
            ActiveJavascriptClass::Protected,
            ConcurrencyPermitPhase::Initial,
        )
        .await;
        drop(replacement);
        assert_eq!(limiter.active_permits(), 0);
    }

    #[tokio::test]
    async fn cancelling_a_granted_request_transfers_capacity() {
        let limiter = ConcurrencyLimiter::new(1);
        let held = limiter.acquire(Arc::new("held".to_owned()), false).await;
        let mut cancelled = Box::pin(limiter.acquire(Arc::new("cancelled".to_owned()), true));
        let mut replacement = Box::pin(limiter.acquire(Arc::new("replacement".to_owned()), true));
        assert!(matches!(poll!(cancelled.as_mut()), Poll::Pending));
        assert!(matches!(poll!(replacement.as_mut()), Poll::Pending));

        drop(held);
        drop(cancelled);
        let Poll::Ready(replacement) = poll!(replacement.as_mut()) else {
            panic!("cancelled grant did not transfer active-JavaScript capacity");
        };
        drop(replacement);
        assert_eq!(limiter.active_permits(), 0);
    }
}
