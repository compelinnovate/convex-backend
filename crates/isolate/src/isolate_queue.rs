use std::{
    collections::VecDeque,
    future::poll_fn,
    pin::Pin,
    sync::Arc,
    task::{
        Context,
        Poll,
    },
    time::Duration,
};

use common::runtime::Runtime;
use event_listener::Event;
use futures::{
    future::FusedFuture,
    Future,
};
use parking_lot::Mutex;

use crate::metrics;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IsolateQueueLane {
    Dependency,
    ControlPlane,
    IndependentAction,
    Ordinary,
}

impl IsolateQueueLane {
    const ALL: [Self; 4] = [
        Self::Dependency,
        Self::ControlPlane,
        Self::IndependentAction,
        Self::Ordinary,
    ];

    fn index(self) -> usize {
        match self {
            Self::Dependency => 0,
            Self::ControlPlane => 1,
            Self::IndependentAction => 2,
            Self::Ordinary => 3,
        }
    }

    pub(crate) fn as_label(self) -> &'static str {
        match self {
            Self::Dependency => "dependency",
            Self::ControlPlane => "control_plane",
            Self::IndependentAction => "independent_action",
            Self::Ordinary => "ordinary",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IsolateQueueIneligibilityReason {
    PhysicalTotal,
    SharedBase,
    PerClientTotal,
    PerClientBase,
    IndependentActionCap,
    ActiveJavascriptClassPending,
}

impl IsolateQueueIneligibilityReason {
    const ALL: [Self; 6] = [
        Self::PhysicalTotal,
        Self::SharedBase,
        Self::PerClientTotal,
        Self::PerClientBase,
        Self::IndependentActionCap,
        Self::ActiveJavascriptClassPending,
    ];

    fn index(self) -> usize {
        match self {
            Self::PhysicalTotal => 0,
            Self::SharedBase => 1,
            Self::PerClientTotal => 2,
            Self::PerClientBase => 3,
            Self::IndependentActionCap => 4,
            Self::ActiveJavascriptClassPending => 5,
        }
    }

    fn as_label(self) -> &'static str {
        match self {
            Self::PhysicalTotal => "physical_total",
            Self::SharedBase => "shared_base",
            Self::PerClientTotal => "per_client_total",
            Self::PerClientBase => "per_client_base",
            Self::IndependentActionCap => "independent_action_cap",
            Self::ActiveJavascriptClassPending => "active_javascript_class_pending",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct IsolateQueueEligibility {
    pub(crate) physical_total: bool,
    pub(crate) shared_base: bool,
    pub(crate) per_client_total: bool,
    pub(crate) per_client_base: bool,
    pub(crate) independent_action_cap: bool,
    pub(crate) active_javascript_class_pending: bool,
}

impl IsolateQueueEligibility {
    #[cfg(test)]
    pub(crate) fn eligible() -> Self {
        Self::default()
    }

    pub(crate) fn is_eligible(self) -> bool {
        self == Self::default()
    }

    fn contains(self, reason: IsolateQueueIneligibilityReason) -> bool {
        match reason {
            IsolateQueueIneligibilityReason::PhysicalTotal => self.physical_total,
            IsolateQueueIneligibilityReason::SharedBase => self.shared_base,
            IsolateQueueIneligibilityReason::PerClientTotal => self.per_client_total,
            IsolateQueueIneligibilityReason::PerClientBase => self.per_client_base,
            IsolateQueueIneligibilityReason::IndependentActionCap => self.independent_action_cap,
            IsolateQueueIneligibilityReason::ActiveJavascriptClassPending => {
                self.active_javascript_class_pending
            },
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct IsolateQueueConfig {
    target: Duration,
    interval: Duration,
    shed_threshold: Duration,
    hard_max_age: Duration,
    control_plane_hard_max_age: Duration,
    pub(crate) control_plane_capacity: usize,
}

impl IsolateQueueConfig {
    pub(crate) fn new(
        target: Duration,
        interval: Duration,
        shed_threshold: Duration,
        hard_max_age: Duration,
        control_plane_hard_max_age: Duration,
        control_plane_capacity: usize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !target.is_zero(),
            "isolate queue delay target must be greater than zero"
        );
        anyhow::ensure!(
            !interval.is_zero(),
            "isolate queue delay interval must be greater than zero"
        );
        anyhow::ensure!(
            shed_threshold > target,
            "isolate queue delay shed threshold must be greater than the delay target"
        );
        anyhow::ensure!(
            hard_max_age > shed_threshold,
            "isolate queue hard maximum age must be greater than the delay shed threshold"
        );
        anyhow::ensure!(
            !control_plane_hard_max_age.is_zero(),
            "isolate control-plane queue hard maximum age must be greater than zero"
        );
        anyhow::ensure!(
            control_plane_capacity > 0,
            "isolate control-plane queue capacity must be greater than zero"
        );
        // Integer parsing alone does not guarantee that the runtime can arm
        // an Instant-based timer for the configured duration.
        let now = tokio::time::Instant::now();
        anyhow::ensure!(
            now.checked_add(interval).is_some(),
            "isolate queue delay interval is too large"
        );
        anyhow::ensure!(
            now.checked_add(shed_threshold).is_some(),
            "isolate queue delay shed threshold is too large"
        );
        anyhow::ensure!(
            now.checked_add(hard_max_age).is_some(),
            "isolate queue hard maximum age is too large"
        );
        anyhow::ensure!(
            now.checked_add(control_plane_hard_max_age).is_some(),
            "isolate control-plane queue hard maximum age is too large"
        );
        Ok(Self {
            target,
            interval,
            shed_threshold,
            hard_max_age,
            control_plane_hard_max_age,
            control_plane_capacity,
        })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ControllerTransitions {
    entered: bool,
    cleared: bool,
}

impl ControllerTransitions {
    fn merge(&mut self, other: Self) {
        self.entered |= other.entered;
        self.cleared |= other.cleared;
    }
}

#[derive(Clone, Copy, Debug)]
struct LaneDelayController {
    interval_end: tokio::time::Instant,
    min_sojourn: Option<Duration>,
    sample_count: usize,
    overloaded: bool,
}

impl LaneDelayController {
    fn new(now: tokio::time::Instant, interval: Duration) -> Self {
        Self {
            interval_end: now + interval,
            min_sojourn: None,
            sample_count: 0,
            overloaded: false,
        }
    }

    fn observe(
        &mut self,
        now: tokio::time::Instant,
        sojourn: Duration,
        config: IsolateQueueConfig,
    ) -> ControllerTransitions {
        let mut transitions = ControllerTransitions::default();
        if now >= self.interval_end {
            let previous_overloaded = self.overloaded;
            if self.sample_count >= 2 && self.min_sojourn.is_some_and(|d| d > config.target) {
                self.overloaded = true;
            } else if self.sample_count > 0 && self.min_sojourn.is_some_and(|d| d <= config.target)
            {
                self.overloaded = false;
            }
            transitions.entered = !previous_overloaded && self.overloaded;
            transitions.cleared = previous_overloaded && !self.overloaded;
            // The current sample starts a fresh complete interval. Empty
            // intervals do not synthesize a minimum or change overload state.
            self.interval_end = now + config.interval;
            self.min_sojourn = None;
            self.sample_count = 0;
        }
        self.min_sojourn = Some(
            self.min_sojourn
                .map_or(sojourn, |minimum| minimum.min(sojourn)),
        );
        self.sample_count = self
            .sample_count
            .checked_add(1)
            .expect("isolate queue lane sample count overflow");
        transitions
    }

    fn reset(&mut self, now: tokio::time::Instant, interval: Duration) -> ControllerTransitions {
        let cleared = self.overloaded;
        *self = Self::new(now, interval);
        ControllerTransitions {
            entered: false,
            cleared,
        }
    }
}

struct QueueEntry<T> {
    item: T,
    lane: IsolateQueueLane,
    enqueued_at: tokio::time::Instant,
    hard_deadline: tokio::time::Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IsolateQueueSendError {
    QueueFull,
    LaneFull,
    SchedulerClosed,
}

impl IsolateQueueSendError {
    pub(crate) fn as_label(self) -> &'static str {
        match self {
            Self::QueueFull => "queue_full",
            Self::LaneFull => "lane_full",
            Self::SchedulerClosed => "scheduler_closed",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IsolateQueueRejection {
    HardExpired,
    DelayControlShed,
}

impl IsolateQueueRejection {
    fn as_label(self) -> &'static str {
        match self {
            Self::HardExpired => "hard_expired",
            Self::DelayControlShed => "delay_control_shed",
        }
    }
}

pub(crate) struct IsolateQueueOutput<T> {
    pub(crate) item: T,
    pub(crate) rejection: Option<IsolateQueueRejection>,
    /// Deadline that still bounds initial active-permit acquisition after a
    /// successful queue selection and before worker assignment.
    pub(crate) permit_deadline: Option<tokio::time::Instant>,
    /// Lane sojourn to publish after the scheduler successfully hands the
    /// request to a worker. Queue rejection paths never carry this sample.
    pub(crate) dispatch_sojourn: Option<(IsolateQueueLane, Duration)>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct IneligibleCounts([[usize; 6]; 4]);

impl IneligibleCounts {
    fn increment(&mut self, lane: IsolateQueueLane, eligibility: IsolateQueueEligibility) {
        for reason in IsolateQueueIneligibilityReason::ALL {
            if eligibility.contains(reason) {
                self.0[lane.index()][reason.index()] += 1;
            }
        }
    }

    fn get(self, lane: IsolateQueueLane, reason: IsolateQueueIneligibilityReason) -> usize {
        self.0[lane.index()][reason.index()]
    }
}

struct QueuePop<T> {
    output: IsolateQueueOutput<T>,
    lane: IsolateQueueLane,
    transitions: ControllerTransitions,
}

struct SelectionAttempt<T> {
    selected: Option<QueuePop<T>>,
    ineligible: IneligibleCounts,
}

struct IsolateDelayQueue<T> {
    buffer: VecDeque<QueueEntry<T>>,
    lane_depths: [usize; 4],
    capacity: usize,
    capacity_with_reserve: usize,
    config: IsolateQueueConfig,
    controllers: [LaneDelayController; 4],
}

impl<T> IsolateDelayQueue<T> {
    fn new(
        now: tokio::time::Instant,
        capacity: usize,
        reserved_capacity: usize,
        config: IsolateQueueConfig,
    ) -> Self {
        let capacity_with_reserve = capacity
            .checked_add(reserved_capacity)
            .expect("isolate queue capacity overflow");
        Self {
            buffer: VecDeque::new(),
            lane_depths: [0; 4],
            capacity,
            capacity_with_reserve,
            config,
            controllers: [LaneDelayController::new(now, config.interval); 4],
        }
    }

    fn push(
        &mut self,
        item: T,
        lane: IsolateQueueLane,
        now: tokio::time::Instant,
    ) -> Result<bool, (IsolateQueueSendError, T)> {
        // The scheduler derives the lane once from request properties. Using
        // that same value as the reserve authority prevents admission and
        // delay-control classification from drifting apart.
        let capacity = if lane == IsolateQueueLane::Dependency {
            self.capacity_with_reserve
        } else {
            self.capacity
        };
        if self.buffer.len() >= capacity {
            // Return ownership so the sender can drop arbitrary request
            // resources after releasing the queue mutex.
            return Err((IsolateQueueSendError::QueueFull, item));
        }
        if lane == IsolateQueueLane::ControlPlane
            && self.lane_depths[lane.index()] >= self.config.control_plane_capacity
        {
            return Err((IsolateQueueSendError::LaneFull, item));
        }
        if self.lane_depths[lane.index()] == 0 {
            self.controllers[lane.index()] = LaneDelayController::new(now, self.config.interval);
        }
        let used_reserved_capacity = self.buffer.len() >= self.capacity;
        assert!(
            !used_reserved_capacity || lane == IsolateQueueLane::Dependency,
            "only dependency requests may use isolate queue reserve"
        );
        self.lane_depths[lane.index()] = self.lane_depths[lane.index()]
            .checked_add(1)
            .expect("isolate queue lane depth overflow");
        let hard_max_age = if lane == IsolateQueueLane::ControlPlane {
            self.config.control_plane_hard_max_age
        } else {
            self.config.hard_max_age
        };
        let hard_deadline = now
            .checked_add(hard_max_age)
            .expect("validated isolate queue hard maximum age must fit the runtime timer");
        self.buffer.push_back(QueueEntry {
            item,
            lane,
            enqueued_at: now,
            hard_deadline,
        });
        Ok(used_reserved_capacity)
    }

    fn remove(&mut self, index: usize) -> QueueEntry<T> {
        let entry = self
            .buffer
            .remove(index)
            .expect("selected isolate queue entry must exist");
        self.lane_depths[entry.lane.index()] = self.lane_depths[entry.lane.index()]
            .checked_sub(1)
            .expect("isolate queue lane depth underflow");
        entry
    }

    fn pop_selecting(
        &mut self,
        now: tokio::time::Instant,
        select: &mut impl FnMut(&T) -> IsolateQueueEligibility,
    ) -> SelectionAttempt<T> {
        if let Some(expired) = self.pop_expired(now) {
            return SelectionAttempt {
                selected: Some(expired),
                ineligible: self.ineligible_counts(now, &mut *select),
            };
        }

        let mut ineligible = IneligibleCounts::default();
        let mut selected_index = None;
        for (index, entry) in self.buffer.iter().enumerate() {
            let eligibility = select(&entry.item);
            if eligibility.is_eligible() {
                if selected_index.is_none() {
                    selected_index = Some(index);
                }
            } else {
                ineligible.increment(entry.lane, eligibility);
            }
        }
        let Some(selected_index) = selected_index else {
            return SelectionAttempt {
                selected: None,
                ineligible,
            };
        };

        let entry = self.remove(selected_index);
        let sojourn = now.saturating_duration_since(entry.enqueued_at);
        let controller = &mut self.controllers[entry.lane.index()];
        let mut transitions = controller.observe(now, sojourn, self.config);
        // Lane overload only enables shedding; the selected request's own age
        // decides it. A blocked older peer must not cause a younger eligible
        // request to be shed. Dependencies and control-plane work bypass
        // adaptive shedding.
        let should_shed = !matches!(
            entry.lane,
            IsolateQueueLane::Dependency | IsolateQueueLane::ControlPlane
        ) && controller.overloaded
            && sojourn > self.config.shed_threshold;
        transitions.merge(self.reset_drained_lane(entry.lane, now));
        let rejection = should_shed.then_some(IsolateQueueRejection::DelayControlShed);
        SelectionAttempt {
            selected: Some(QueuePop {
                output: IsolateQueueOutput {
                    item: entry.item,
                    rejection,
                    permit_deadline: rejection.is_none().then_some(entry.hard_deadline),
                    dispatch_sojourn: rejection.is_none().then_some((entry.lane, sojourn)),
                },
                lane: entry.lane,
                transitions,
            }),
            ineligible,
        }
    }

    fn pop_expired(&mut self, now: tokio::time::Instant) -> Option<QueuePop<T>> {
        let expired_index = self
            .buffer
            .iter()
            .position(|entry| now >= entry.hard_deadline)?;
        let entry = self.remove(expired_index);
        let transitions = self.reset_drained_lane(entry.lane, now);
        Some(QueuePop {
            output: IsolateQueueOutput {
                item: entry.item,
                rejection: Some(IsolateQueueRejection::HardExpired),
                permit_deadline: None,
                dispatch_sojourn: None,
            },
            lane: entry.lane,
            transitions,
        })
    }

    fn ineligible_counts(
        &self,
        now: tokio::time::Instant,
        select: &mut impl FnMut(&T) -> IsolateQueueEligibility,
    ) -> IneligibleCounts {
        let mut ineligible = IneligibleCounts::default();
        for entry in &self.buffer {
            // Due entries will be removed on subsequent receives; they are
            // expired, not blocked by scheduler eligibility.
            if now >= entry.hard_deadline {
                continue;
            }
            let eligibility = select(&entry.item);
            if !eligibility.is_eligible() {
                ineligible.increment(entry.lane, eligibility);
            }
        }
        ineligible
    }

    fn reset_drained_lane(
        &mut self,
        lane: IsolateQueueLane,
        now: tokio::time::Instant,
    ) -> ControllerTransitions {
        if self.lane_depths[lane.index()] > 0 {
            ControllerTransitions::default()
        } else {
            self.controllers[lane.index()].reset(now, self.config.interval)
        }
    }

    fn next_expiration(&self) -> Option<tokio::time::Instant> {
        self.buffer.iter().map(|entry| entry.hard_deadline).min()
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn drain(
        &mut self,
        now: tokio::time::Instant,
    ) -> (VecDeque<QueueEntry<T>>, [ControllerTransitions; 4]) {
        let queued = std::mem::take(&mut self.buffer);
        self.lane_depths = [0; 4];
        let transitions = IsolateQueueLane::ALL
            .map(|lane| self.controllers[lane.index()].reset(now, self.config.interval));
        (queued, transitions)
    }

    fn depth(&self, lane: IsolateQueueLane) -> usize {
        self.lane_depths[lane.index()]
    }

    fn oldest_age(&self, lane: IsolateQueueLane, now: tokio::time::Instant) -> Duration {
        self.buffer
            .iter()
            .filter(|entry| entry.lane == lane)
            .map(|entry| now.saturating_duration_since(entry.enqueued_at))
            .max()
            .unwrap_or_default()
    }
}

struct Inner<RT: Runtime, T> {
    rt: RT,
    pool_name: &'static str,
    queue: IsolateDelayQueue<T>,
    event: Event,
    expired_event: Event,
    senders: usize,
    receivers: usize,
    reported_ineligible: Option<IneligibleCounts>,
}

impl<RT: Runtime, T> Inner<RT, T> {
    fn log_queue_state(&self) {
        let now = self.rt.monotonic_now();
        for lane in IsolateQueueLane::ALL {
            metrics::log_isolate_queue_depth(
                self.pool_name,
                lane.as_label(),
                self.queue.depth(lane),
            );
            metrics::log_isolate_queue_oldest_age(
                self.pool_name,
                lane.as_label(),
                self.queue.oldest_age(lane, now),
            );
            metrics::log_isolate_queue_overloaded(
                self.pool_name,
                lane.as_label(),
                self.queue.controllers[lane.index()].overloaded,
            );
        }
    }

    fn log_lane_mutation(&self, lane: IsolateQueueLane) {
        let depth = self.queue.depth(lane);
        metrics::log_isolate_queue_depth(self.pool_name, lane.as_label(), depth);
        // Oldest age advances without queue mutations and is refreshed by the
        // scheduler's periodic report. Clear it immediately on drain so a
        // stopped scheduler cannot leave a stale positive age behind.
        if depth == 0 {
            metrics::log_isolate_queue_oldest_age(self.pool_name, lane.as_label(), Duration::ZERO);
        }
        metrics::log_isolate_queue_overloaded(
            self.pool_name,
            lane.as_label(),
            self.queue.controllers[lane.index()].overloaded,
        );
    }

    fn log_ineligible(&mut self, counts: IneligibleCounts) {
        let previous = self.reported_ineligible;
        for lane in IsolateQueueLane::ALL {
            for reason in IsolateQueueIneligibilityReason::ALL {
                let count = counts.get(lane, reason);
                if previous.is_none_or(|previous| previous.get(lane, reason) != count) {
                    metrics::log_isolate_queue_ineligible(
                        self.pool_name,
                        lane.as_label(),
                        reason.as_label(),
                        count,
                    );
                }
            }
        }
        self.reported_ineligible = Some(counts);
    }

    fn log_pop(&self, pop: &QueuePop<T>) {
        if pop.transitions.entered {
            metrics::log_isolate_queue_overload_transition(
                self.pool_name,
                pop.lane.as_label(),
                "entered",
            );
        }
        if pop.transitions.cleared {
            metrics::log_isolate_queue_overload_transition(
                self.pool_name,
                pop.lane.as_label(),
                "cleared",
            );
        }
        if let Some(rejection) = pop.output.rejection {
            metrics::log_isolate_queue_rejection(
                self.pool_name,
                pop.lane.as_label(),
                rejection.as_label(),
            );
        }
    }
}

pub(crate) fn new_isolate_queue<RT: Runtime, T>(
    rt: RT,
    pool_name: &'static str,
    capacity: usize,
    reserved_capacity: usize,
    config: IsolateQueueConfig,
) -> (IsolateQueueSender<RT, T>, IsolateQueueReceiver<RT, T>) {
    let now = rt.monotonic_now();
    let inner = Arc::new(Mutex::new(Inner {
        rt,
        pool_name,
        queue: IsolateDelayQueue::new(now, capacity, reserved_capacity, config),
        event: Event::new(),
        expired_event: Event::new(),
        senders: 1,
        receivers: 1,
        reported_ineligible: None,
    }));
    {
        let mut inner = inner.lock();
        inner.log_queue_state();
        inner.log_ineligible(IneligibleCounts::default());
        metrics::log_isolate_queue_policy(inner.pool_name, "lane_delay_control");
        metrics::log_isolate_queue_config(inner.pool_name, "target_millis", config.target);
        metrics::log_isolate_queue_config(inner.pool_name, "interval_millis", config.interval);
        metrics::log_isolate_queue_config(
            inner.pool_name,
            "shed_threshold_millis",
            config.shed_threshold,
        );
        metrics::log_isolate_queue_config(
            inner.pool_name,
            "hard_max_age_millis",
            config.hard_max_age,
        );
        metrics::log_isolate_queue_config(
            inner.pool_name,
            "control_plane_hard_max_age_millis",
            config.control_plane_hard_max_age,
        );
    }
    (
        IsolateQueueSender {
            inner: inner.clone(),
        },
        IsolateQueueReceiver {
            inner,
            listener: None,
            expiration_wait: None,
        },
    )
}

pub(crate) struct IsolateQueueSender<RT: Runtime, T> {
    inner: Arc<Mutex<Inner<RT, T>>>,
}

impl<RT: Runtime, T> Clone for IsolateQueueSender<RT, T> {
    fn clone(&self) -> Self {
        let mut inner = self.inner.lock();
        inner.senders = inner
            .senders
            .checked_add(1)
            .expect("IsolateQueueSender count overflow");
        drop(inner);
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<RT: Runtime, T> Drop for IsolateQueueSender<RT, T> {
    fn drop(&mut self) {
        let mut inner = self.inner.lock();
        inner.senders = inner
            .senders
            .checked_sub(1)
            .expect("IsolateQueueSender count underflow");
        if inner.senders == 0 {
            inner.event.notify(usize::MAX);
            inner.expired_event.notify(usize::MAX);
        }
    }
}

impl<RT: Runtime, T> IsolateQueueSender<RT, T> {
    pub(crate) fn try_send(
        &self,
        item: T,
        lane: IsolateQueueLane,
    ) -> Result<bool, IsolateQueueSendError> {
        let mut inner = self.inner.lock();
        if inner.receivers == 0 {
            let pool_name = inner.pool_name;
            drop(inner);
            metrics::log_isolate_queue_rejection(
                pool_name,
                lane.as_label(),
                IsolateQueueSendError::SchedulerClosed.as_label(),
            );
            drop(item);
            return Err(IsolateQueueSendError::SchedulerClosed);
        }
        let now = inner.rt.monotonic_now();
        let result = inner.queue.push(item, lane, now);
        match result {
            Ok(used_reserved_capacity) => {
                inner.log_lane_mutation(lane);
                inner.event.notify_additional(1);
                // Expiry companions do not consume send notifications, so all
                // of them must reconsider their earliest deadline.
                inner.expired_event.notify(usize::MAX);
                Ok(used_reserved_capacity)
            },
            Err((error, item)) => {
                let pool_name = inner.pool_name;
                drop(inner);
                metrics::log_isolate_queue_rejection(pool_name, lane.as_label(), error.as_label());
                drop(item);
                Err(error)
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.inner.lock().receivers == 0
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().queue.buffer.len()
    }
}

pub(crate) struct IsolateQueueReceiver<RT: Runtime, T> {
    inner: Arc<Mutex<Inner<RT, T>>>,
    listener: Option<event_listener::EventListener>,
    expiration_wait: Option<(
        tokio::time::Instant,
        Pin<Box<dyn FusedFuture<Output = ()> + Send + 'static>>,
    )>,
}

/// A non-consuming companion to the scheduler's main receiver that removes
/// only hard-expired entries. It does not keep admission open after the main
/// receiver is dropped.
pub(crate) struct IsolateQueueExpiredReceiver<RT: Runtime, T> {
    inner: Arc<Mutex<Inner<RT, T>>>,
    listener: Option<event_listener::EventListener>,
    expiration_wait: Option<(
        tokio::time::Instant,
        Pin<Box<dyn FusedFuture<Output = ()> + Send + 'static>>,
    )>,
}

/// Reports scrape-facing queue state without borrowing the receiver future.
/// The scheduler polls a receive and the metrics timer in the same `select!`,
/// so metrics must share only the locked queue state, not receiver-local wake
/// registration.
pub(crate) struct IsolateQueueMetricsReporter<RT: Runtime, T> {
    inner: Arc<Mutex<Inner<RT, T>>>,
}

impl<RT: Runtime, T> IsolateQueueMetricsReporter<RT, T> {
    pub(crate) fn report(&self) {
        self.inner.lock().log_queue_state();
    }
}

impl<RT: Runtime, T> Clone for IsolateQueueReceiver<RT, T> {
    fn clone(&self) -> Self {
        let mut inner = self.inner.lock();
        inner.receivers = inner
            .receivers
            .checked_add(1)
            .expect("IsolateQueueReceiver count overflow");
        drop(inner);
        Self {
            inner: self.inner.clone(),
            listener: None,
            expiration_wait: None,
        }
    }
}

impl<RT: Runtime, T> Drop for IsolateQueueReceiver<RT, T> {
    fn drop(&mut self) {
        let queued = {
            let mut inner = self.inner.lock();
            inner.receivers = inner
                .receivers
                .checked_sub(1)
                .expect("IsolateQueueReceiver count underflow");
            if inner.receivers == 0 {
                let now = inner.rt.monotonic_now();
                let (queued, transitions) = inner.queue.drain(now);
                for (lane, transition) in IsolateQueueLane::ALL.into_iter().zip(transitions) {
                    if transition.cleared {
                        metrics::log_isolate_queue_overload_transition(
                            inner.pool_name,
                            lane.as_label(),
                            "cleared",
                        );
                    }
                }
                inner.log_queue_state();
                inner.log_ineligible(IneligibleCounts::default());
                inner.event.notify(usize::MAX);
                inner.expired_event.notify(usize::MAX);
                Some(queued)
            } else {
                None
            }
        };
        // Entries can own arbitrary drop implementations. Do not run them while
        // holding the queue mutex.
        drop(queued);
    }
}

impl<RT: Runtime, T> IsolateQueueReceiver<RT, T> {
    fn register(
        listener: &mut Option<event_listener::EventListener>,
        inner: &mut Inner<RT, T>,
        cx: &mut Context<'_>,
    ) -> Poll<!> {
        loop {
            match Pin::new(listener.get_or_insert_with(|| inner.event.listen())).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => {
                    listener.take();
                },
            }
        }
    }

    fn poll_expiration_deadline(
        expiration_wait: &mut Option<(
            tokio::time::Instant,
            Pin<Box<dyn FusedFuture<Output = ()> + Send + 'static>>,
        )>,
        rt: &RT,
        deadline: Option<tokio::time::Instant>,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        let Some(deadline) = deadline else {
            expiration_wait.take();
            return Poll::Pending;
        };
        if expiration_wait
            .as_ref()
            .map(|(current_deadline, _)| *current_deadline)
            != Some(deadline)
        {
            let duration = deadline.saturating_duration_since(rt.monotonic_now());
            *expiration_wait = Some((deadline, rt.wait(duration)));
        }
        let (_, wait) = expiration_wait
            .as_mut()
            .expect("isolate queue expiration wait must exist for a deadline");
        if wait.as_mut().poll(cx).is_ready() {
            expiration_wait.take();
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    fn poll_next_selecting(
        &mut self,
        cx: &mut Context<'_>,
        select: &mut impl FnMut(&T) -> IsolateQueueEligibility,
    ) -> Poll<Option<IsolateQueueOutput<T>>> {
        loop {
            let s = &mut *self;
            let mut inner = s.inner.lock();
            let now = inner.rt.monotonic_now();
            let attempt = inner.queue.pop_selecting(now, &mut *select);
            inner.log_ineligible(attempt.ineligible);
            if let Some(pop) = attempt.selected {
                let expiration_wait = s.expiration_wait.take();
                inner.log_pop(&pop);
                inner.log_lane_mutation(pop.lane);
                inner.expired_event.notify(usize::MAX);
                drop(inner);
                // Runtime timer futures can own arbitrary drop behavior. Keep
                // their destruction outside the queue mutex just like entries.
                drop(expiration_wait);
                return Poll::Ready(Some(pop.output));
            } else if inner.senders == 0 && inner.queue.is_empty() {
                let expiration_wait = s.expiration_wait.take();
                drop(inner);
                drop(expiration_wait);
                return Poll::Ready(None);
            }

            let deadline = inner.queue.next_expiration();
            let rt = inner.rt.clone();
            let Poll::Pending = Self::register(&mut s.listener, &mut *inner, cx);
            drop(inner);
            if Self::poll_expiration_deadline(&mut s.expiration_wait, &rt, deadline, cx).is_ready()
            {
                continue;
            }
            return Poll::Pending;
        }
    }

    pub(crate) async fn recv_next_selecting(
        &mut self,
        mut select: impl FnMut(&T) -> IsolateQueueEligibility,
    ) -> Option<IsolateQueueOutput<T>> {
        poll_fn(|cx| self.poll_next_selecting(cx, &mut select)).await
    }

    pub(crate) fn metrics_reporter(&self) -> IsolateQueueMetricsReporter<RT, T> {
        IsolateQueueMetricsReporter {
            inner: self.inner.clone(),
        }
    }

    pub(crate) fn expired_receiver(&self) -> IsolateQueueExpiredReceiver<RT, T> {
        IsolateQueueExpiredReceiver {
            inner: self.inner.clone(),
            listener: None,
            expiration_wait: None,
        }
    }
}

impl<RT: Runtime, T> IsolateQueueExpiredReceiver<RT, T> {
    fn poll_next_expired(
        &mut self,
        cx: &mut Context<'_>,
        select: &mut impl FnMut(&T) -> IsolateQueueEligibility,
    ) -> Poll<Option<IsolateQueueOutput<T>>> {
        loop {
            let s = &mut *self;
            let mut inner = s.inner.lock();
            let now = inner.rt.monotonic_now();
            if let Some(pop) = inner.queue.pop_expired(now) {
                let listener = s.listener.take();
                let expiration_wait = s.expiration_wait.take();
                let ineligible = inner.queue.ineligible_counts(now, &mut *select);
                inner.log_ineligible(ineligible);
                inner.log_pop(&pop);
                inner.log_lane_mutation(pop.lane);
                // The consuming receiver may be waiting for closure or on a
                // deadline that this removal changed.
                inner.event.notify(usize::MAX);
                inner.expired_event.notify(usize::MAX);
                drop(inner);
                drop(listener);
                drop(expiration_wait);
                return Poll::Ready(Some(pop.output));
            }

            let ineligible = inner.queue.ineligible_counts(now, &mut *select);
            inner.log_ineligible(ineligible);
            if inner.receivers == 0 || (inner.senders == 0 && inner.queue.is_empty()) {
                let listener = s.listener.take();
                let expiration_wait = s.expiration_wait.take();
                drop(inner);
                drop(listener);
                drop(expiration_wait);
                return Poll::Ready(None);
            }

            let deadline = inner.queue.next_expiration();
            let rt = inner.rt.clone();
            let queue_changed = Pin::new(
                s.listener
                    .get_or_insert_with(|| inner.expired_event.listen()),
            )
            .poll(cx)
            .is_ready();
            drop(inner);
            if queue_changed {
                s.listener.take();
                continue;
            }
            if IsolateQueueReceiver::<RT, T>::poll_expiration_deadline(
                &mut s.expiration_wait,
                &rt,
                deadline,
                cx,
            )
            .is_ready()
            {
                continue;
            }
            return Poll::Pending;
        }
    }

    pub(crate) async fn recv_next_expired(
        &mut self,
        mut select: impl FnMut(&T) -> IsolateQueueEligibility,
    ) -> Option<IsolateQueueOutput<T>> {
        poll_fn(|cx| self.poll_next_expired(cx, &mut select)).await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            atomic::{
                AtomicUsize,
                Ordering,
            },
            Arc,
        },
        task::{
            Context,
            Poll,
        },
        time::{
            Duration,
            SystemTime,
        },
    };

    use common::{
        pause::PauseClient,
        runtime::{
            Runtime,
            SpawnHandle,
        },
    };
    use futures::{
        future::FusedFuture,
        FutureExt as _,
    };
    use parking_lot::Mutex;
    use rand::SeedableRng;
    use rand_chacha::ChaCha12Rng;

    use super::{
        new_isolate_queue,
        IsolateDelayQueue,
        IsolateQueueConfig,
        IsolateQueueEligibility,
        IsolateQueueLane,
        IsolateQueueRejection,
        IsolateQueueSendError,
        LaneDelayController,
    };

    fn config() -> IsolateQueueConfig {
        IsolateQueueConfig::new(
            Duration::from_millis(10),
            Duration::from_millis(100),
            Duration::from_millis(20),
            Duration::from_millis(1000),
            Duration::from_secs(30),
            4,
        )
        .unwrap()
    }

    fn eligible(_: &&'static str) -> IsolateQueueEligibility {
        IsolateQueueEligibility::eligible()
    }

    #[test]
    fn controller_requires_two_slow_samples_from_a_complete_interval() {
        let start = tokio::time::Instant::now();
        let config = config();
        let mut controller = LaneDelayController::new(start, config.interval);

        assert_eq!(
            controller.observe(
                start + Duration::from_millis(20),
                Duration::from_millis(20),
                config,
            ),
            Default::default()
        );
        // The boundary closes an interval with only one sample. The sample at
        // the boundary belongs to the next interval.
        assert_eq!(
            controller.observe(start + config.interval, Duration::from_millis(25), config,),
            Default::default()
        );
        assert!(!controller.overloaded);
        controller.observe(
            start + config.interval + Duration::from_millis(1),
            Duration::from_millis(30),
            config,
        );
        let transition = controller.observe(
            start + config.interval * 2,
            Duration::from_millis(30),
            config,
        );
        assert!(transition.entered);
        assert!(controller.overloaded);
    }

    #[test]
    fn config_rejects_zero_inconsistent_and_unrepresentable_durations() {
        assert!(IsolateQueueConfig::new(
            Duration::ZERO,
            Duration::from_millis(10),
            Duration::from_millis(20),
            Duration::from_millis(100),
            Duration::from_secs(30),
            4,
        )
        .is_err());
        assert!(IsolateQueueConfig::new(
            Duration::from_millis(1),
            Duration::ZERO,
            Duration::from_millis(2),
            Duration::from_millis(100),
            Duration::from_secs(30),
            4,
        )
        .is_err());
        assert!(IsolateQueueConfig::new(
            Duration::from_millis(10),
            Duration::from_millis(100),
            Duration::from_millis(10),
            Duration::from_millis(100),
            Duration::from_secs(30),
            4,
        )
        .is_err());
        assert!(IsolateQueueConfig::new(
            Duration::from_millis(10),
            Duration::from_millis(100),
            Duration::from_millis(20),
            Duration::from_millis(20),
            Duration::from_secs(30),
            4,
        )
        .is_err());
        assert!(IsolateQueueConfig::new(
            Duration::from_millis(1),
            Duration::MAX,
            Duration::from_millis(2),
            Duration::from_millis(100),
            Duration::from_secs(30),
            4,
        )
        .is_err());
        assert!(IsolateQueueConfig::new(
            Duration::from_millis(1),
            Duration::from_millis(10),
            Duration::from_millis(2),
            Duration::MAX,
            Duration::from_secs(30),
            4,
        )
        .is_err());
        assert!(IsolateQueueConfig::new(
            Duration::from_millis(1),
            Duration::from_millis(10),
            Duration::from_millis(2),
            Duration::from_millis(100),
            Duration::ZERO,
            4,
        )
        .is_err());
        assert!(IsolateQueueConfig::new(
            Duration::from_millis(1),
            Duration::from_millis(10),
            Duration::from_millis(2),
            Duration::from_millis(100),
            Duration::MAX,
            4,
        )
        .is_err());
        assert!(IsolateQueueConfig::new(
            Duration::from_millis(1),
            Duration::from_millis(10),
            Duration::from_millis(2),
            Duration::from_millis(100),
            Duration::from_secs(30),
            0,
        )
        .is_err());
    }

    #[test]
    fn adaptive_shedding_uses_explicit_queue_age_threshold() {
        let start = tokio::time::Instant::now();
        let config = IsolateQueueConfig::new(
            Duration::from_millis(10),
            Duration::from_millis(100),
            Duration::from_millis(40),
            Duration::from_secs(1),
            Duration::from_secs(30),
            4,
        )
        .unwrap();
        let mut queue = IsolateDelayQueue::new(start, 1, 0, config);
        queue
            .push("at_threshold", IsolateQueueLane::Ordinary, start)
            .unwrap();
        queue.controllers[IsolateQueueLane::Ordinary.index()].overloaded = true;

        let at_threshold = queue
            .pop_selecting(start + Duration::from_millis(40), &mut eligible)
            .selected
            .unwrap();
        assert_eq!(at_threshold.output.rejection, None);

        queue
            .push(
                "above_threshold",
                IsolateQueueLane::Ordinary,
                start + Duration::from_millis(40),
            )
            .unwrap();
        queue.controllers[IsolateQueueLane::Ordinary.index()].overloaded = true;
        let above_threshold = queue
            .pop_selecting(start + Duration::from_millis(90), &mut eligible)
            .selected
            .unwrap();
        assert_eq!(
            above_threshold.output.rejection,
            Some(IsolateQueueRejection::DelayControlShed)
        );
    }

    #[test]
    fn zero_sample_interval_preserves_state_and_one_healthy_sample_clears_it() {
        let start = tokio::time::Instant::now();
        let config = config();
        let mut controller = LaneDelayController::new(start, config.interval);
        controller.overloaded = true;

        let transition = controller.observe(
            start + config.interval * 3,
            Duration::from_millis(5),
            config,
        );
        assert_eq!(transition, Default::default());
        assert!(controller.overloaded);

        let transition = controller.observe(
            start + config.interval * 4,
            Duration::from_millis(5),
            config,
        );
        assert!(transition.cleared);
        assert!(!controller.overloaded);
    }

    #[test]
    fn fifo_skips_ineligible_action_then_resumes_in_order() {
        let start = tokio::time::Instant::now();
        let mut queue = IsolateDelayQueue::new(start, 8, 2, config());
        queue
            .push("action_a", IsolateQueueLane::IndependentAction, start)
            .unwrap();
        queue
            .push("action_b", IsolateQueueLane::IndependentAction, start)
            .unwrap();
        queue
            .push("dependency", IsolateQueueLane::Dependency, start)
            .unwrap();

        let attempt = queue.pop_selecting(start, &mut |item| {
            if item.starts_with("action") {
                IsolateQueueEligibility {
                    independent_action_cap: true,
                    ..Default::default()
                }
            } else {
                IsolateQueueEligibility::eligible()
            }
        });
        assert_eq!(attempt.selected.unwrap().output.item, "dependency");
        assert_eq!(
            attempt.ineligible.0[IsolateQueueLane::IndependentAction.index()][4],
            2
        );

        let first_action = queue.pop_selecting(start, &mut eligible).selected.unwrap();
        let second_action = queue.pop_selecting(start, &mut eligible).selected.unwrap();
        assert_eq!(first_action.output.item, "action_a");
        assert_eq!(second_action.output.item, "action_b");
    }

    #[test]
    fn action_backlog_overload_does_not_affect_other_lanes() {
        let start = tokio::time::Instant::now();
        let config = config();
        let mut queue = IsolateDelayQueue::new(start, 16, 2, config);
        for item in ["action_a", "action_b", "action_c", "action_d"] {
            queue
                .push(item, IsolateQueueLane::IndependentAction, start)
                .unwrap();
        }
        queue
            .pop_selecting(start + Duration::from_millis(20), &mut eligible)
            .selected
            .unwrap();
        queue
            .pop_selecting(start + Duration::from_millis(30), &mut eligible)
            .selected
            .unwrap();
        queue
            .push(
                "action_e",
                IsolateQueueLane::IndependentAction,
                start + config.interval,
            )
            .unwrap();
        let shed = queue
            .pop_selecting(start + config.interval, &mut eligible)
            .selected
            .unwrap();
        assert_eq!(shed.output.item, "action_c");
        assert_eq!(
            shed.output.rejection,
            Some(IsolateQueueRejection::DelayControlShed)
        );
        assert!(queue.controllers[IsolateQueueLane::IndependentAction.index()].overloaded);

        queue
            .push(
                "ordinary",
                IsolateQueueLane::Ordinary,
                start + config.interval,
            )
            .unwrap();
        queue
            .push(
                "dependency",
                IsolateQueueLane::Dependency,
                start + config.interval,
            )
            .unwrap();
        let mut non_actions_only = |item: &&str| {
            if item.starts_with("action") {
                IsolateQueueEligibility {
                    independent_action_cap: true,
                    ..Default::default()
                }
            } else {
                IsolateQueueEligibility::eligible()
            }
        };
        let ordinary = queue
            .pop_selecting(start + config.interval, &mut non_actions_only)
            .selected
            .unwrap();
        let dependency = queue
            .pop_selecting(start + config.interval, &mut non_actions_only)
            .selected
            .unwrap();
        assert_eq!(ordinary.output.item, "ordinary");
        assert_eq!(ordinary.output.rejection, None);
        assert_eq!(dependency.output.item, "dependency");
        assert_eq!(dependency.output.rejection, None);
        assert!(!queue.controllers[IsolateQueueLane::Ordinary.index()].overloaded);
        assert!(!queue.controllers[IsolateQueueLane::Dependency.index()].overloaded);
    }

    #[test]
    fn shedding_uses_selected_requests_own_sojourn() {
        let start = tokio::time::Instant::now();
        let config = config();
        let mut queue = IsolateDelayQueue::new(start, 4, 0, config);
        queue
            .push("blocked_old", IsolateQueueLane::Ordinary, start)
            .unwrap();
        queue
            .push(
                "eligible_young",
                IsolateQueueLane::Ordinary,
                start + Duration::from_millis(15),
            )
            .unwrap();
        queue.controllers[IsolateQueueLane::Ordinary.index()].overloaded = true;

        let now = start + Duration::from_millis(25);
        let selected = queue
            .pop_selecting(now, &mut |item| IsolateQueueEligibility {
                shared_base: (*item == "blocked_old"),
                ..Default::default()
            })
            .selected
            .unwrap();
        assert_eq!(selected.output.item, "eligible_young");
        assert_eq!(selected.output.rejection, None);

        let old = queue.pop_selecting(now, &mut eligible).selected.unwrap();
        assert_eq!(old.output.item, "blocked_old");
        assert_eq!(
            old.output.rejection,
            Some(IsolateQueueRejection::DelayControlShed)
        );
    }

    #[test]
    fn transient_burst_and_lane_drain_reset_delay_state() {
        let start = tokio::time::Instant::now();
        let config = config();
        let mut queue = IsolateDelayQueue::new(start, 8, 0, config);
        queue.push("a", IsolateQueueLane::Ordinary, start).unwrap();
        queue.push("b", IsolateQueueLane::Ordinary, start).unwrap();
        queue
            .pop_selecting(start + Duration::from_millis(20), &mut eligible)
            .selected
            .unwrap();
        queue
            .pop_selecting(start + Duration::from_millis(30), &mut eligible)
            .selected
            .unwrap();
        let controller = queue.controllers[IsolateQueueLane::Ordinary.index()];
        assert!(!controller.overloaded);
        assert_eq!(controller.sample_count, 0);
        assert_eq!(controller.min_sojourn, None);

        queue.controllers[IsolateQueueLane::Ordinary.index()].overloaded = true;
        queue
            .push("fresh", IsolateQueueLane::Ordinary, start + config.interval)
            .unwrap();
        assert!(!queue.controllers[IsolateQueueLane::Ordinary.index()].overloaded);
    }

    #[test]
    fn dependencies_are_not_shed_but_keep_capacity_and_hard_age_bounds() {
        let start = tokio::time::Instant::now();
        let config = config();
        let mut queue = IsolateDelayQueue::new(start, 1, 1, config);
        queue
            .push("dependency", IsolateQueueLane::Dependency, start)
            .unwrap();
        queue.controllers[IsolateQueueLane::Dependency.index()].overloaded = true;
        let dependency = queue
            .pop_selecting(start + Duration::from_millis(30), &mut eligible)
            .selected
            .unwrap();
        assert_eq!(dependency.output.rejection, None);

        queue
            .push("ordinary", IsolateQueueLane::Ordinary, start)
            .unwrap();
        assert!(queue
            .push("ordinary_full", IsolateQueueLane::Ordinary, start)
            .is_err());
        assert!(queue
            .push("dependency_reserve", IsolateQueueLane::Dependency, start)
            .unwrap());
        assert!(queue
            .push("dependency_full", IsolateQueueLane::Dependency, start)
            .is_err());
        let expired = queue
            .pop_selecting(start + config.hard_max_age, &mut eligible)
            .selected
            .unwrap();
        assert_eq!(expired.output.item, "ordinary");
        assert_eq!(
            expired.output.rejection,
            Some(IsolateQueueRejection::HardExpired)
        );
        let expired = queue
            .pop_selecting(start + config.hard_max_age, &mut eligible)
            .selected
            .unwrap();
        assert_eq!(expired.output.item, "dependency_reserve");
        assert_eq!(
            expired.output.rejection,
            Some(IsolateQueueRejection::HardExpired)
        );
    }

    #[test]
    fn control_plane_lane_uses_shared_capacity_and_has_a_local_cap() {
        let start = tokio::time::Instant::now();
        let config = IsolateQueueConfig::new(
            Duration::from_millis(10),
            Duration::from_millis(100),
            Duration::from_millis(20),
            Duration::from_secs(1),
            Duration::from_secs(30),
            1,
        )
        .unwrap();
        let mut queue = IsolateDelayQueue::new(start, 2, 1, config);

        assert!(!queue
            .push("control_plane", IsolateQueueLane::ControlPlane, start)
            .unwrap());
        assert_eq!(queue.depth(IsolateQueueLane::ControlPlane), 1);
        assert_eq!(
            queue
                .push(
                    "control_plane_lane_full",
                    IsolateQueueLane::ControlPlane,
                    start,
                )
                .unwrap_err()
                .0,
            IsolateQueueSendError::LaneFull,
        );
        assert!(!queue
            .push("ordinary", IsolateQueueLane::Ordinary, start)
            .unwrap());
        assert!(queue
            .push("dependency_reserve", IsolateQueueLane::Dependency, start)
            .unwrap());

        let dispatched = queue.pop_selecting(start, &mut eligible).selected.unwrap();
        assert_eq!(dispatched.output.item, "control_plane");
        assert_eq!(queue.depth(IsolateQueueLane::ControlPlane), 0);
        assert_eq!(
            queue
                .push(
                    "control_plane_queue_full",
                    IsolateQueueLane::ControlPlane,
                    start,
                )
                .unwrap_err()
                .0,
            IsolateQueueSendError::QueueFull,
        );
    }

    #[test]
    fn control_plane_preserves_fifo_with_ordinary_requests() {
        let start = tokio::time::Instant::now();
        let mut queue = IsolateDelayQueue::new(start, 8, 1, config());
        queue
            .push("ordinary_old", IsolateQueueLane::Ordinary, start)
            .unwrap();
        queue
            .push(
                "control_plane_new",
                IsolateQueueLane::ControlPlane,
                start + Duration::from_millis(1),
            )
            .unwrap();
        assert_eq!(
            queue
                .pop_selecting(start + Duration::from_millis(2), &mut eligible)
                .selected
                .unwrap()
                .output
                .item,
            "ordinary_old",
        );
        assert_eq!(
            queue
                .pop_selecting(start + Duration::from_millis(2), &mut eligible)
                .selected
                .unwrap()
                .output
                .item,
            "control_plane_new",
        );

        queue
            .push(
                "control_plane_old",
                IsolateQueueLane::ControlPlane,
                start + Duration::from_millis(3),
            )
            .unwrap();
        queue
            .push(
                "ordinary_new",
                IsolateQueueLane::Ordinary,
                start + Duration::from_millis(4),
            )
            .unwrap();
        assert_eq!(
            queue
                .pop_selecting(start + Duration::from_millis(5), &mut eligible)
                .selected
                .unwrap()
                .output
                .item,
            "control_plane_old",
        );
    }

    #[test]
    fn control_plane_bypasses_shedding_but_uses_its_finite_deadline() {
        let start = tokio::time::Instant::now();
        let config = config();
        let mut queue = IsolateDelayQueue::new(start, 4, 0, config);
        queue
            .push("control_plane", IsolateQueueLane::ControlPlane, start)
            .unwrap();
        queue.controllers[IsolateQueueLane::ControlPlane.index()].overloaded = true;
        let selected = queue
            .pop_selecting(start + Duration::from_millis(30), &mut eligible)
            .selected
            .unwrap();
        assert_eq!(selected.output.rejection, None);

        queue
            .push("expires", IsolateQueueLane::ControlPlane, start)
            .unwrap();
        let expired = queue
            .pop_selecting(start + config.control_plane_hard_max_age, &mut eligible)
            .selected
            .unwrap();
        assert_eq!(expired.output.item, "expires");
        assert_eq!(
            expired.output.rejection,
            Some(IsolateQueueRejection::HardExpired)
        );
    }

    #[test]
    fn per_entry_deadlines_expire_newer_ordinary_work_first() {
        let start = tokio::time::Instant::now();
        let config = config();
        let mut queue = IsolateDelayQueue::new(start, 4, 0, config);
        queue
            .push("control_plane_old", IsolateQueueLane::ControlPlane, start)
            .unwrap();
        let ordinary_enqueued_at = start + Duration::from_millis(100);
        queue
            .push(
                "ordinary_new",
                IsolateQueueLane::Ordinary,
                ordinary_enqueued_at,
            )
            .unwrap();
        assert_eq!(
            queue.next_expiration(),
            Some(ordinary_enqueued_at + config.hard_max_age)
        );

        let expired = queue
            .pop_selecting(ordinary_enqueued_at + config.hard_max_age, &mut eligible)
            .selected
            .unwrap();
        assert_eq!(expired.output.item, "ordinary_new");
        assert_eq!(
            expired.output.rejection,
            Some(IsolateQueueRejection::HardExpired)
        );
        assert_eq!(queue.depth(IsolateQueueLane::ControlPlane), 1);
    }

    #[test]
    fn hard_expiration_does_not_assume_physical_deadline_order() {
        let start = tokio::time::Instant::now();
        let config = config();
        let mut queue = IsolateDelayQueue::new(start, 4, 0, config);
        queue
            .push("front", IsolateQueueLane::Ordinary, start)
            .unwrap();
        queue
            .push("back", IsolateQueueLane::Ordinary, start)
            .unwrap();
        queue.buffer[0].hard_deadline = start + config.hard_max_age + Duration::from_millis(100);
        queue.buffer[1].hard_deadline = start + config.hard_max_age;

        let expired = queue
            .pop_selecting(start + config.hard_max_age, &mut eligible)
            .selected
            .unwrap();
        assert_eq!(expired.output.item, "back");
        assert_eq!(
            expired.output.rejection,
            Some(IsolateQueueRejection::HardExpired)
        );
    }

    #[test]
    fn hard_expiration_reports_remaining_ineligible_entries() {
        let start = tokio::time::Instant::now();
        let config = config();
        let mut queue = IsolateDelayQueue::new(start, 4, 0, config);
        queue
            .push("expired", IsolateQueueLane::Ordinary, start)
            .unwrap();
        queue
            .push(
                "blocked",
                IsolateQueueLane::IndependentAction,
                start + Duration::from_millis(1),
            )
            .unwrap();

        let attempt = queue.pop_selecting(start + config.hard_max_age, &mut |item| {
            IsolateQueueEligibility {
                independent_action_cap: *item == "blocked",
                ..Default::default()
            }
        });
        assert_eq!(attempt.selected.unwrap().output.item, "expired");
        assert_eq!(
            attempt.ineligible.0[IsolateQueueLane::IndependentAction.index()][4],
            1
        );
    }

    struct DropCounter(Arc<AtomicUsize>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[derive(Clone)]
    struct QueueTestRuntime {
        now: Arc<Mutex<tokio::time::Instant>>,
        timer_drop_probe: Arc<Mutex<Option<Arc<dyn Fn() + Send + Sync>>>>,
    }

    impl QueueTestRuntime {
        fn new() -> Self {
            Self {
                now: Arc::new(Mutex::new(tokio::time::Instant::now())),
                timer_drop_probe: Arc::new(Mutex::new(None)),
            }
        }

        fn set_timer_drop_probe(&self, probe: impl Fn() + Send + Sync + 'static) {
            *self.timer_drop_probe.lock() = Some(Arc::new(probe));
        }
    }

    struct QueueTestWait {
        inner: Pin<Box<dyn FusedFuture<Output = ()> + Send + 'static>>,
        drop_probe: Option<Arc<dyn Fn() + Send + Sync>>,
    }

    impl Future for QueueTestWait {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.inner.as_mut().poll(cx)
        }
    }

    impl FusedFuture for QueueTestWait {
        fn is_terminated(&self) -> bool {
            self.inner.is_terminated()
        }
    }

    impl Drop for QueueTestWait {
        fn drop(&mut self) {
            if let Some(probe) = self.drop_probe.take() {
                probe();
            }
        }
    }

    impl Runtime for QueueTestRuntime {
        fn wait(
            &self,
            duration: Duration,
        ) -> Pin<Box<dyn FusedFuture<Output = ()> + Send + 'static>> {
            let now = self.now.clone();
            let inner = Box::pin(
                async move {
                    tokio::time::sleep(duration).await;
                    *now.lock() += duration;
                }
                .fuse(),
            );
            let drop_probe = self.timer_drop_probe.lock().clone();
            Box::pin(QueueTestWait { inner, drop_probe })
        }

        fn spawn(
            &self,
            _name: &'static str,
            _f: impl Future<Output = ()> + Send + 'static,
        ) -> Box<dyn SpawnHandle> {
            panic!("QueueTestRuntime::spawn is not used by these tests")
        }

        fn spawn_thread<Fut: Future<Output = ()>, F: FnOnce() -> Fut + Send + 'static>(
            &self,
            _name: &str,
            _f: F,
        ) -> Box<dyn SpawnHandle> {
            panic!("QueueTestRuntime::spawn_thread is not used by these tests")
        }

        fn system_time(&self) -> SystemTime {
            SystemTime::UNIX_EPOCH
        }

        fn monotonic_now(&self) -> tokio::time::Instant {
            *self.now.lock()
        }

        fn rng(&self) -> Box<dyn rand::RngCore> {
            Box::new(ChaCha12Rng::seed_from_u64(0))
        }

        fn pause_client(&self) -> PauseClient {
            PauseClient::new()
        }
    }

    #[tokio::test]
    async fn canceled_receive_and_sender_shutdown_preserve_each_item_once() {
        let rt = QueueTestRuntime::new();
        let (sender, mut receiver) = new_isolate_queue(
            rt,
            "queue_test",
            2,
            0,
            IsolateQueueConfig::new(
                Duration::from_millis(10),
                Duration::from_millis(100),
                Duration::from_millis(20),
                Duration::from_secs(10),
                Duration::from_secs(30),
                1,
            )
            .unwrap(),
        );
        sender
            .try_send("queued", IsolateQueueLane::Ordinary)
            .unwrap();
        let mut pending = Box::pin(receiver.recv_next_selecting(|_| IsolateQueueEligibility {
            shared_base: true,
            ..Default::default()
        }));
        assert!(futures::poll!(&mut pending).is_pending());
        drop(pending);

        let output = receiver
            .recv_next_selecting(|_| IsolateQueueEligibility::eligible())
            .await
            .unwrap();
        assert_eq!(output.item, "queued");
        drop(sender);
        assert!(receiver
            .recv_next_selecting(|_| IsolateQueueEligibility::eligible())
            .await
            .is_none());
    }

    #[tokio::test]
    async fn receiver_wakes_at_hard_expiration_without_an_enqueue() {
        let rt = QueueTestRuntime::new();
        let (sender, mut receiver) = new_isolate_queue(
            rt,
            "queue_test",
            1,
            0,
            IsolateQueueConfig::new(
                Duration::from_millis(1),
                Duration::from_millis(10),
                Duration::from_millis(2),
                Duration::from_millis(3),
                Duration::from_secs(30),
                1,
            )
            .unwrap(),
        );
        sender
            .try_send("queued", IsolateQueueLane::Ordinary)
            .unwrap();

        let output = tokio::time::timeout(
            Duration::from_secs(1),
            receiver.recv_next_selecting(|_| IsolateQueueEligibility {
                shared_base: true,
                ..Default::default()
            }),
        )
        .await
        .expect("receiver did not wake at the hard expiration")
        .expect("sender is still open");
        assert_eq!(output.item, "queued");
        assert_eq!(output.rejection, Some(IsolateQueueRejection::HardExpired));
    }

    #[tokio::test]
    async fn expiration_companion_wakes_while_main_receiver_is_idle() {
        let rt = QueueTestRuntime::new();
        let (sender, receiver) = new_isolate_queue(
            rt,
            "queue_test",
            1,
            0,
            IsolateQueueConfig::new(
                Duration::from_millis(1),
                Duration::from_millis(10),
                Duration::from_millis(2),
                Duration::from_millis(3),
                Duration::from_secs(30),
                1,
            )
            .unwrap(),
        );
        let mut expired_receiver = receiver.expired_receiver();
        sender
            .try_send("queued", IsolateQueueLane::Ordinary)
            .unwrap();
        drop(sender);

        let output = tokio::time::timeout(
            Duration::from_secs(1),
            expired_receiver.recv_next_expired(|_| IsolateQueueEligibility {
                shared_base: true,
                ..Default::default()
            }),
        )
        .await
        .expect("expiration companion did not wake at the hard deadline")
        .expect("queued entry was lost when the last sender closed");
        assert_eq!(output.item, "queued");
        assert_eq!(output.rejection, Some(IsolateQueueRejection::HardExpired));
        assert!(expired_receiver
            .recv_next_expired(|_| IsolateQueueEligibility::eligible())
            .await
            .is_none());
    }

    #[tokio::test]
    async fn expiration_companion_replaces_later_lane_deadline() {
        let rt = QueueTestRuntime::new();
        let (sender, receiver) = new_isolate_queue(
            rt,
            "queue_test",
            2,
            0,
            IsolateQueueConfig::new(
                Duration::from_millis(1),
                Duration::from_millis(10),
                Duration::from_millis(2),
                Duration::from_millis(3),
                Duration::from_millis(100),
                1,
            )
            .unwrap(),
        );
        sender
            .try_send("control_plane", IsolateQueueLane::ControlPlane)
            .unwrap();
        let mut expired_receiver = receiver.expired_receiver();
        let mut pending =
            Box::pin(expired_receiver.recv_next_expired(|_| IsolateQueueEligibility::eligible()));
        assert!(futures::poll!(pending.as_mut()).is_pending());
        drop(pending);

        sender
            .try_send("ordinary", IsolateQueueLane::Ordinary)
            .unwrap();
        let output = tokio::time::timeout(
            Duration::from_secs(1),
            expired_receiver.recv_next_expired(|_| IsolateQueueEligibility::eligible()),
        )
        .await
        .expect("expiration companion kept the obsolete control-plane timer")
        .expect("main receiver is still open");
        assert_eq!(output.item, "ordinary");
        assert_eq!(output.rejection, Some(IsolateQueueRejection::HardExpired));
    }

    #[tokio::test]
    async fn expiration_companion_closes_with_main_receiver() {
        let rt = QueueTestRuntime::new();
        let (sender, receiver) = new_isolate_queue(rt, "queue_test", 1, 0, config());
        let mut expired_receiver = receiver.expired_receiver();
        sender
            .try_send("queued", IsolateQueueLane::Ordinary)
            .unwrap();

        drop(receiver);
        assert!(expired_receiver
            .recv_next_expired(|_| IsolateQueueEligibility::eligible())
            .await
            .is_none());
        assert!(sender.is_closed());
    }

    #[tokio::test]
    async fn receiver_drops_expiration_timer_outside_queue_lock() {
        let rt = QueueTestRuntime::new();
        let (sender, mut receiver) = new_isolate_queue(
            rt.clone(),
            "queue_test",
            1,
            0,
            IsolateQueueConfig::new(
                Duration::from_millis(1),
                Duration::from_millis(10),
                Duration::from_millis(2),
                Duration::from_secs(60),
                Duration::from_secs(90),
                1,
            )
            .unwrap(),
        );
        sender
            .try_send("queued", IsolateQueueLane::Ordinary)
            .unwrap();

        let inner = Arc::downgrade(&sender.inner);
        let unlocked_drops = Arc::new(AtomicUsize::new(0));
        let unlocked_drops_for_probe = unlocked_drops.clone();
        rt.set_timer_drop_probe(move || {
            let inner = inner.upgrade().expect("queue must still exist");
            if inner.try_lock().is_some() {
                unlocked_drops_for_probe.fetch_add(1, Ordering::Relaxed);
            }
        });

        let mut pending = Box::pin(receiver.recv_next_selecting(|_| IsolateQueueEligibility {
            shared_base: true,
            ..Default::default()
        }));
        assert!(futures::poll!(&mut pending).is_pending());
        drop(pending);

        let output = receiver
            .recv_next_selecting(|_| IsolateQueueEligibility::eligible())
            .await
            .expect("sender is still open");
        assert_eq!(output.item, "queued");
        assert_eq!(output.rejection, None);
        assert_eq!(unlocked_drops.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn last_sender_keeps_ineligible_items_until_hard_expiration() {
        let rt = QueueTestRuntime::new();
        let (sender, mut receiver) = new_isolate_queue(
            rt,
            "queue_test",
            1,
            0,
            IsolateQueueConfig::new(
                Duration::from_millis(1),
                Duration::from_millis(10),
                Duration::from_millis(2),
                Duration::from_millis(3),
                Duration::from_secs(30),
                1,
            )
            .unwrap(),
        );
        sender
            .try_send("queued", IsolateQueueLane::Ordinary)
            .unwrap();
        drop(sender);

        let output = tokio::time::timeout(
            Duration::from_secs(1),
            receiver.recv_next_selecting(|_| IsolateQueueEligibility {
                shared_base: true,
                ..Default::default()
            }),
        )
        .await
        .expect("closed queue did not retain its item until hard expiration")
        .expect("queued item was lost when the last sender closed");
        assert_eq!(output.item, "queued");
        assert_eq!(output.rejection, Some(IsolateQueueRejection::HardExpired));
        assert!(receiver
            .recv_next_selecting(|_| IsolateQueueEligibility::eligible())
            .await
            .is_none());
    }

    #[test]
    fn dropping_last_receiver_closes_and_drains_exactly_once() {
        let rt = QueueTestRuntime::new();
        let (sender, receiver) = new_isolate_queue(rt, "queue_test", 1, 0, config());
        let drops = Arc::new(AtomicUsize::new(0));
        sender
            .try_send(DropCounter(drops.clone()), IsolateQueueLane::Ordinary)
            .unwrap();

        drop(receiver);
        assert!(sender.is_closed());
        assert_eq!(drops.load(Ordering::Relaxed), 1);

        assert!(sender
            .try_send(DropCounter(drops.clone()), IsolateQueueLane::Ordinary,)
            .is_err());
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }
}
