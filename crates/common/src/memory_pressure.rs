use std::sync::Arc;

use parking_lot::{
    RwLock,
    RwLockReadGuard,
};
use tokio::sync::watch;

/// Process-local cgroup memory-reclamation state shared with subsystems that
/// can release optional retained state. The controller owns state changes;
/// consumers may observe the current value or wait for a transition.
#[derive(Clone)]
pub struct MemoryPressureSignal {
    inner: Arc<MemoryPressureSignalInner>,
}

struct MemoryPressureSignalInner {
    active: RwLock<bool>,
    sender: watch::Sender<bool>,
}

/// Keeps the observed pressure state stable until this guard is dropped.
/// Controller publication takes the corresponding write lock.
pub struct MemoryPressureStateGuard<'a> {
    active: RwLockReadGuard<'a, bool>,
}

impl MemoryPressureStateGuard<'_> {
    pub fn is_active(&self) -> bool {
        *self.active
    }
}

impl MemoryPressureSignal {
    pub fn new(active: bool) -> Self {
        let (sender, _) = watch::channel(active);
        Self {
            inner: Arc::new(MemoryPressureSignalInner {
                active: RwLock::new(active),
                sender,
            }),
        }
    }

    pub fn is_active(&self) -> bool {
        self.lock_state().is_active()
    }

    /// Locks the current state against controller transitions. Callers must
    /// keep this guard within bounded synchronous work and never across an
    /// await point.
    pub fn lock_state(&self) -> MemoryPressureStateGuard<'_> {
        MemoryPressureStateGuard {
            active: self.inner.active.read(),
        }
    }

    /// Returns the prior state so the one controller can verify transitions.
    pub fn set_active(&self, active: bool) -> bool {
        let mut state = self.inner.active.write();
        let prior = std::mem::replace(&mut *state, active);
        let watched_prior = self.inner.sender.send_replace(active);
        assert_eq!(
            watched_prior, prior,
            "memory-pressure state and watch publication drifted"
        );
        prior
    }

    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.inner.sender.subscribe()
    }
}

impl Default for MemoryPressureSignal {
    fn default() -> Self {
        Self::new(false)
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryPressureSignal;

    #[test]
    fn signal_preserves_current_state_and_transition_order() {
        let signal = MemoryPressureSignal::default();
        let receiver = signal.subscribe();
        assert!(!signal.is_active());
        assert!(!*receiver.borrow());

        let state = signal.lock_state();
        assert!(!state.is_active());
        assert!(signal.inner.active.try_write().is_none());
        drop(state);
        assert!(signal.inner.active.try_write().is_some());

        assert!(!signal.set_active(true));
        assert!(signal.is_active());
        assert!(*receiver.borrow());
        assert!(signal.set_active(false));
        assert!(!signal.is_active());
    }
}
