use std::{
    collections::VecDeque,
    sync::Arc,
};

use parking_lot::Mutex;
use tokio::sync::oneshot;

/// A concurrency gate with shared base capacity and dependency-only overflow.
///
/// Every request may run while total occupancy is below `base_capacity`.
/// Dependencies may additionally run up to `total_capacity`. Waiters retain
/// FIFO order whenever the queue head is eligible; dependency waiters only
/// bypass ordinary waiters while occupancy is in the overflow range.
#[derive(Clone)]
pub struct DependencyOverflowGate {
    inner: Arc<Inner>,
}

struct Inner {
    base_capacity: usize,
    total_capacity: usize,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    active: usize,
    next_waiter_id: u64,
    waiters: VecDeque<Waiter>,
}

struct Waiter {
    id: u64,
    is_dependency: bool,
    ready: oneshot::Sender<DependencyOverflowPermit>,
    waiter_guard: Option<Box<dyn Send>>,
}

/// A permit returned by [`DependencyOverflowGate`].
pub struct DependencyOverflowPermit {
    inner: Option<Arc<Inner>>,
}

struct WaiterCancellation {
    inner: Arc<Inner>,
    waiter_id: Option<u64>,
}

impl DependencyOverflowGate {
    /// Creates a gate with capacity shared by every request and additional
    /// total capacity available only to dependencies.
    pub fn new(base_capacity: usize, total_capacity: usize) -> Self {
        assert!(
            base_capacity <= total_capacity,
            "dependency overflow base capacity must not exceed total capacity"
        );
        Self {
            inner: Arc::new(Inner {
                base_capacity,
                total_capacity,
                state: Mutex::new(State::default()),
            }),
        }
    }

    /// Acquires a permit.
    pub async fn acquire(&self, is_dependency: bool) -> DependencyOverflowPermit {
        self.acquire_with_waiter(is_dependency, || ()).await
    }

    /// Acquires a permit and creates `make_waiter_guard` only when the request
    /// must enter the wait queue. The guard remains alive until permit handoff
    /// or cancellation, allowing callers to account for actual waiters without
    /// changing gate fairness.
    pub async fn acquire_with_waiter<G: Send + 'static>(
        &self,
        is_dependency: bool,
        make_waiter_guard: impl FnOnce() -> G,
    ) -> DependencyOverflowPermit {
        let (waiter_id, ready, cancellation) = {
            let mut state = self.inner.state.lock();
            if state.waiters.is_empty() && self.inner.can_start(state.active, is_dependency) {
                state.active += 1;
                return DependencyOverflowPermit {
                    inner: Some(self.inner.clone()),
                };
            }

            let waiter_id = state.next_waiter_id;
            state.next_waiter_id = state
                .next_waiter_id
                .checked_add(1)
                .expect("dependency overflow waiter id overflow");
            let (ready_sender, ready) = oneshot::channel();
            state.waiters.push_back(Waiter {
                id: waiter_id,
                is_dependency,
                ready: ready_sender,
                // Do not dispatch this entry until its waiter guard is attached.
                // Otherwise a concurrent release can hand off the permit before
                // waiter metrics start.
                waiter_guard: None,
            });
            (
                waiter_id,
                ready,
                WaiterCancellation {
                    inner: self.inner.clone(),
                    waiter_id: Some(waiter_id),
                },
            )
        };

        let waiter_guard: Box<dyn Send> = Box::new(make_waiter_guard());
        {
            let mut state = self.inner.state.lock();
            let waiter = state
                .waiters
                .iter_mut()
                .find(|waiter| waiter.id == waiter_id)
                .expect("new dependency overflow waiter disappeared before registration");
            assert!(
                waiter.waiter_guard.is_none(),
                "dependency overflow waiter guard registered twice"
            );
            waiter.waiter_guard = Some(waiter_guard);
        }
        self.inner.dispatch_waiters();
        let permit = ready
            .await
            .expect("dependency overflow gate dropped while acquiring a permit");
        cancellation.disarm();
        permit
    }

    /// Returns the number of permits currently in use.
    pub fn active(&self) -> usize {
        self.inner.state.lock().active
    }

    /// Returns shared base occupancy, excluding dependency overflow.
    pub fn base_in_use(&self) -> usize {
        self.active().min(self.inner.base_capacity)
    }
}

impl Inner {
    fn can_start(&self, active: usize, is_dependency: bool) -> bool {
        active
            < if is_dependency {
                self.total_capacity
            } else {
                self.base_capacity
            }
    }

    fn dispatch_waiters(self: &Arc<Self>) {
        loop {
            let waiter = {
                let mut state = self.state.lock();
                if state.active >= self.total_capacity {
                    return;
                }
                let index = if state.active < self.base_capacity {
                    state
                        .waiters
                        .front()
                        .and_then(|waiter| waiter.waiter_guard.is_some().then_some(0))
                } else {
                    let dependency = state.waiters.iter().position(|waiter| waiter.is_dependency);
                    dependency.filter(|index| state.waiters[*index].waiter_guard.is_some())
                };
                let Some(index) = index else {
                    return;
                };
                let mut waiter = state
                    .waiters
                    .remove(index)
                    .expect("selected dependency overflow waiter must exist");
                state.active += 1;
                let waiter_guard = waiter
                    .waiter_guard
                    .take()
                    .expect("selected dependency overflow waiter must be registered");
                drop(state);
                // The request stops being a waiter when the gate grants its
                // permit, not when the receiving task is next polled.
                drop(waiter_guard);
                waiter
            };

            let permit = DependencyOverflowPermit {
                inner: Some(self.clone()),
            };
            if let Err(mut permit) = waiter.ready.send(permit) {
                // Avoid recursively dispatching through an arbitrarily long
                // run of canceled waiters.
                permit.release_without_dispatch();
            }
        }
    }

    fn release(self: &Arc<Self>) {
        {
            let mut state = self.state.lock();
            state.active = state
                .active
                .checked_sub(1)
                .expect("dependency overflow active count underflow");
        }
        self.dispatch_waiters();
    }
}

impl Drop for DependencyOverflowPermit {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.release();
        }
    }
}

impl DependencyOverflowPermit {
    fn release_without_dispatch(&mut self) {
        let inner = self
            .inner
            .take()
            .expect("dependency overflow permit released twice");
        let mut state = inner.state.lock();
        state.active = state
            .active
            .checked_sub(1)
            .expect("dependency overflow active count underflow");
    }
}

impl WaiterCancellation {
    fn disarm(mut self) {
        self.waiter_id.take();
    }
}

impl Drop for WaiterCancellation {
    fn drop(&mut self) {
        let Some(waiter_id) = self.waiter_id else {
            return;
        };
        let removed = {
            let mut state = self.inner.state.lock();
            state
                .waiters
                .iter()
                .position(|waiter| waiter.id == waiter_id)
                .map(|index| {
                    state
                        .waiters
                        .remove(index)
                        .expect("selected canceled dependency overflow waiter must exist")
                })
        };
        if removed.is_some() {
            // A canceled entry may have been the non-dispatchable FIFO head
            // while capacity was available. Removing it must wake the next
            // eligible waiter without waiting for another permit release.
            drop(removed);
            self.inner.dispatch_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{
                AtomicUsize,
                Ordering,
            },
            Arc,
        },
        time::Duration,
    };

    use super::DependencyOverflowGate;

    #[tokio::test]
    async fn dependencies_share_base_and_alone_use_overflow() {
        let gate = DependencyOverflowGate::new(2, 3);
        let dependency_in_base = gate.acquire(true).await;
        let ordinary_in_base = gate.acquire(false).await;
        assert_eq!(gate.base_in_use(), 2);
        assert_eq!(gate.active() - gate.base_in_use(), 0);

        let mut ordinary_waiter = std::pin::pin!(gate.acquire(false));
        assert!(futures::poll!(&mut ordinary_waiter).is_pending());

        let dependency_in_overflow = gate.acquire(true).await;
        assert_eq!(gate.active(), 3);
        assert_eq!(gate.active() - gate.base_in_use(), 1);

        let mut dependency_waiter = std::pin::pin!(gate.acquire(true));
        assert!(futures::poll!(&mut dependency_waiter).is_pending());

        drop(dependency_in_base);
        // Total occupancy remains B, so the ordinary waiter is still
        // ineligible and the dependency waiter receives the newly free slot.
        let dependency_after_release =
            tokio::time::timeout(Duration::from_secs(1), &mut dependency_waiter)
                .await
                .expect("dependency did not use available overflow capacity");
        assert_eq!(gate.active(), 3);
        assert!(futures::poll!(&mut ordinary_waiter).is_pending());

        drop(ordinary_in_base);
        assert!(futures::poll!(&mut ordinary_waiter).is_pending());
        drop(dependency_in_overflow);
        let ordinary_after_release =
            tokio::time::timeout(Duration::from_secs(1), &mut ordinary_waiter)
                .await
                .expect("ordinary waiter did not enter shared base capacity");

        drop(dependency_after_release);
        drop(ordinary_after_release);
        assert_eq!(gate.active(), 0);
    }

    #[tokio::test]
    async fn fifo_is_preserved_below_base_capacity() {
        let gate = DependencyOverflowGate::new(1, 1);
        let active = gate.acquire(false).await;
        let mut ordinary = std::pin::pin!(gate.acquire(false));
        let mut dependency = std::pin::pin!(gate.acquire(true));
        assert!(futures::poll!(&mut ordinary).is_pending());
        assert!(futures::poll!(&mut dependency).is_pending());

        drop(active);
        let ordinary = tokio::time::timeout(Duration::from_secs(1), &mut ordinary)
            .await
            .expect("FIFO head did not receive base capacity");
        assert!(futures::poll!(&mut dependency).is_pending());

        drop(ordinary);
        let dependency = tokio::time::timeout(Duration::from_secs(1), &mut dependency)
            .await
            .expect("dependency did not receive released base capacity");
        drop(dependency);
    }

    #[tokio::test]
    async fn canceled_waiters_do_not_leak_capacity() {
        let gate = DependencyOverflowGate::new(1, 2);
        let active = gate.acquire(false).await;
        let mut waiter = Box::pin(gate.acquire(false));
        assert!(futures::poll!(&mut waiter).is_pending());
        drop(waiter);

        drop(active);
        let permit = tokio::time::timeout(Duration::from_secs(1), gate.acquire(false))
            .await
            .expect("canceled waiter leaked capacity");
        drop(permit);
        assert_eq!(gate.active(), 0);
    }

    #[tokio::test]
    async fn canceled_permit_handoffs_do_not_leak_capacity() {
        let gate = DependencyOverflowGate::new(1, 1);
        let active = gate.acquire(false).await;
        let mut waiter = Box::pin(gate.acquire(false));
        assert!(futures::poll!(&mut waiter).is_pending());

        // Dispatch a permit into the waiter's channel, then cancel the waiter
        // before it polls the channel and takes ownership of that permit.
        drop(active);
        assert_eq!(gate.active(), 1);
        drop(waiter);
        assert_eq!(gate.active(), 0);

        let permit = tokio::time::timeout(Duration::from_secs(1), gate.acquire(false))
            .await
            .expect("canceled permit handoff leaked capacity");
        drop(permit);
    }

    #[tokio::test]
    async fn cancellation_redispatches_to_the_next_waiter() {
        let gate = DependencyOverflowGate::new(1, 1);
        let active = gate.acquire(false).await;
        let mut canceled_before_handoff = Box::pin(gate.acquire(false));
        let mut next = Box::pin(gate.acquire(false));
        assert!(futures::poll!(&mut canceled_before_handoff).is_pending());
        assert!(futures::poll!(&mut next).is_pending());

        drop(canceled_before_handoff);
        drop(active);
        let permit = tokio::time::timeout(Duration::from_secs(1), &mut next)
            .await
            .expect("waiter did not progress after the queue head was canceled");
        drop(permit);
        assert_eq!(gate.active(), 0);

        let active = gate.acquire(false).await;
        let mut canceled_after_handoff = Box::pin(gate.acquire(false));
        let mut next = Box::pin(gate.acquire(false));
        assert!(futures::poll!(&mut canceled_after_handoff).is_pending());
        assert!(futures::poll!(&mut next).is_pending());

        drop(active);
        assert_eq!(gate.active(), 1);
        assert!(futures::poll!(&mut next).is_pending());
        drop(canceled_after_handoff);
        let permit = tokio::time::timeout(Duration::from_secs(1), &mut next)
            .await
            .expect("waiter did not progress after a permit handoff was canceled");
        drop(permit);
        assert_eq!(gate.active(), 0);
    }

    #[tokio::test]
    async fn waiter_guard_only_covers_actual_queue_wait() {
        struct WaiterGuard(Arc<AtomicUsize>);
        impl Drop for WaiterGuard {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let immediate_gate = DependencyOverflowGate::new(1, 1);
        let immediate = immediate_gate
            .acquire_with_waiter(false, || panic!("immediate permit reported a waiter"))
            .await;
        drop(immediate);

        let gate = DependencyOverflowGate::new(1, 1);
        let active = gate.acquire(false).await;
        let started = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let started_for_waiter = started.clone();
        let dropped_for_waiter = dropped.clone();
        let mut waiter = Box::pin(gate.acquire_with_waiter(false, move || {
            started_for_waiter.fetch_add(1, Ordering::Relaxed);
            WaiterGuard(dropped_for_waiter)
        }));
        assert!(futures::poll!(&mut waiter).is_pending());
        assert_eq!(started.load(Ordering::Relaxed), 1);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);

        drop(active);
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        let permit = waiter.await;
        drop(permit);
        assert_eq!(gate.active(), 0);

        let active = gate.acquire(false).await;
        let dropped_for_cancellation = dropped.clone();
        let mut canceled = Box::pin(
            gate.acquire_with_waiter(false, move || WaiterGuard(dropped_for_cancellation)),
        );
        assert!(futures::poll!(&mut canceled).is_pending());
        drop(canceled);
        assert_eq!(dropped.load(Ordering::Relaxed), 2);
        drop(active);
    }
}
