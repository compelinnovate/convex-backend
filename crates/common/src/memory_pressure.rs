use tokio::sync::watch;

/// Process-local cgroup memory-reclamation state shared with subsystems that
/// can release optional retained state. The controller owns state changes;
/// consumers may observe the current value or wait for a transition.
#[derive(Clone)]
pub struct MemoryPressureSignal {
    sender: watch::Sender<bool>,
}

impl MemoryPressureSignal {
    pub fn new(active: bool) -> Self {
        let (sender, _) = watch::channel(active);
        Self { sender }
    }

    pub fn is_active(&self) -> bool {
        *self.sender.borrow()
    }

    /// Returns the prior state so the one controller can verify transitions.
    pub fn set_active(&self, active: bool) -> bool {
        self.sender.send_replace(active)
    }

    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.sender.subscribe()
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

        assert!(!signal.set_active(true));
        assert!(signal.is_active());
        assert!(*receiver.borrow());
        assert!(signal.set_active(false));
        assert!(!signal.is_active());
    }
}
