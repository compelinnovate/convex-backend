use std::{
    collections::VecDeque,
    sync::Arc,
};

use parking_lot::Mutex;
use tokio::sync::oneshot;

/// A concurrency gate with shared base capacity and dependency-only overflow.
/// Waiters retain FIFO order whenever the queue head is eligible; dependencies
/// only bypass ordinary waiters while occupancy is in the overflow range.
#[derive(Clone)]
pub(super) struct DependencyOverflowGate {
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
}

pub(super) struct DependencyOverflowPermit {
    inner: Option<Arc<Inner>>,
}

struct WaiterCancellation {
    inner: Arc<Inner>,
    waiter_id: Option<u64>,
}

impl DependencyOverflowGate {
    pub(super) fn new(base_capacity: usize, total_capacity: usize) -> Self {
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

    pub(super) async fn acquire(&self, is_dependency: bool) -> DependencyOverflowPermit {
        let (ready, cancellation) = {
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
            });
            (
                ready,
                WaiterCancellation {
                    inner: self.inner.clone(),
                    waiter_id: Some(waiter_id),
                },
            )
        };

        self.inner.dispatch_waiters();
        let permit = ready
            .await
            .expect("dependency overflow gate dropped while acquiring a permit");
        cancellation.disarm();
        permit
    }

    pub(super) fn active(&self) -> usize {
        self.inner.state.lock().active
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
                    (!state.waiters.is_empty()).then_some(0)
                } else {
                    state.waiters.iter().position(|waiter| waiter.is_dependency)
                };
                let Some(index) = index else {
                    return;
                };
                let waiter = state
                    .waiters
                    .remove(index)
                    .expect("selected dependency overflow waiter must exist");
                state.active += 1;
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
        let mut state = self.inner.state.lock();
        if let Some(index) = state
            .waiters
            .iter()
            .position(|waiter| waiter.id == waiter_id)
        {
            state.waiters.remove(index);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::DependencyOverflowGate;

    #[tokio::test]
    async fn dependencies_share_base_and_alone_use_overflow() {
        let gate = DependencyOverflowGate::new(2, 3);
        let dependency_in_base = gate.acquire(true).await;
        let ordinary_in_base = gate.acquire(false).await;

        let mut ordinary_waiter = std::pin::pin!(gate.acquire(false));
        assert!(futures::poll!(&mut ordinary_waiter).is_pending());

        let dependency_in_overflow = gate.acquire(true).await;
        assert_eq!(gate.active(), 3);

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

        drop(active);
        assert_eq!(gate.active(), 1);
        drop(waiter);
        assert_eq!(gate.active(), 0);

        let permit = tokio::time::timeout(Duration::from_secs(1), gate.acquire(false))
            .await
            .expect("canceled permit handoff leaked capacity");
        drop(permit);
    }
}
