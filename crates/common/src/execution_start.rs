use anyhow::Context;
use tokio::sync::oneshot;

/// Scheduler-side owner of a function execution start barrier.
///
/// The paired [`FunctionExecutionStartGate`] reports when the runtime owns its
/// environment-specific admission. Dropping this controller before `start`
/// cancels that prepared execution; `start` is used only after the matching
/// durable state transition completes.
pub struct FunctionExecutionStartController {
    ready_receiver: oneshot::Receiver<()>,
    start_sender: oneshot::Sender<()>,
}

impl FunctionExecutionStartController {
    pub async fn wait_until_ready(&mut self) -> anyhow::Result<()> {
        (&mut self.ready_receiver)
            .await
            .context("Function execution ended before reaching its start barrier")
    }

    pub fn start(self) -> anyhow::Result<()> {
        self.start_sender
            .send(())
            .map_err(|_| anyhow::anyhow!("Function execution ended before its start was released"))
    }
}

/// Runtime-side linear owner that reports exact admission and blocks user code
/// until the controller releases it.
pub struct FunctionExecutionStartGate {
    ready_sender: oneshot::Sender<()>,
    start_receiver: oneshot::Receiver<()>,
    start_observer: Option<oneshot::Sender<oneshot::Sender<()>>>,
}

impl FunctionExecutionStartGate {
    pub async fn wait(self) -> anyhow::Result<()> {
        self.ready_sender.send(()).map_err(|_| {
            anyhow::anyhow!("Function execution start controller was dropped before admission")
        })?;
        self.start_receiver
            .await
            .context("Function execution start controller was dropped before release")?;
        if let Some(start_observer) = self.start_observer {
            let (observed_sender, observed_receiver) = oneshot::channel();
            if start_observer.send(observed_sender).is_ok() {
                // Keep post-release timing ahead of execution without giving
                // the observer authority. Dropping either observer endpoint
                // must not revoke an execution the controller released.
                let _ = observed_receiver.await;
            }
        }
        Ok(())
    }

    /// Attach a one-shot observation handshake for post-release timing. The
    /// observer never owns or revokes execution authority.
    pub fn with_start_observer(
        mut self,
        start_observer: oneshot::Sender<oneshot::Sender<()>>,
    ) -> Self {
        assert!(
            self.start_observer.is_none(),
            "Function execution start gate already has a start observer"
        );
        self.start_observer = Some(start_observer);
        self
    }

    /// Convert the gate to the channel pair used by isolate worker admission.
    pub fn into_channels(self) -> (oneshot::Sender<()>, oneshot::Receiver<()>) {
        let Self {
            ready_sender,
            start_receiver,
            start_observer,
        } = self;
        assert!(
            start_observer.is_none(),
            "Observed function execution start gate cannot become an isolate channel pair"
        );
        (ready_sender, start_receiver)
    }
}

pub fn function_execution_start_barrier(
) -> (FunctionExecutionStartController, FunctionExecutionStartGate) {
    let (ready_sender, ready_receiver) = oneshot::channel();
    let (start_sender, start_receiver) = oneshot::channel();
    (
        FunctionExecutionStartController {
            ready_receiver,
            start_sender,
        },
        FunctionExecutionStartGate {
            ready_sender,
            start_receiver,
            start_observer: None,
        },
    )
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use super::function_execution_start_barrier;

    #[tokio::test]
    async fn gate_waits_for_controller_release() {
        let (mut controller, gate) = function_execution_start_barrier();
        let gate_task = tokio::spawn(gate.wait());

        controller.wait_until_ready().await.unwrap();
        assert!(!gate_task.is_finished());
        controller.start().unwrap();
        gate_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn start_observer_fires_only_after_release() {
        let (mut controller, gate) = function_execution_start_barrier();
        let (start_observer, mut execution_started) = oneshot::channel();
        let gate_task = tokio::spawn(gate.with_start_observer(start_observer).wait());

        controller.wait_until_ready().await.unwrap();
        assert!(matches!(
            execution_started.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        controller.start().unwrap();
        let observed_sender = execution_started.await.unwrap();
        assert!(!gate_task.is_finished());
        observed_sender.send(()).unwrap();
        gate_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn dropping_start_observer_does_not_revoke_release() {
        let (mut controller, gate) = function_execution_start_barrier();
        let (start_observer, execution_started) = oneshot::channel();
        drop(execution_started);
        let gate_task = tokio::spawn(gate.with_start_observer(start_observer).wait());

        controller.wait_until_ready().await.unwrap();
        controller.start().unwrap();
        gate_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn dropping_start_acknowledgment_does_not_revoke_release() {
        let (mut controller, gate) = function_execution_start_barrier();
        let (start_observer, execution_started) = oneshot::channel();
        let gate_task = tokio::spawn(gate.with_start_observer(start_observer).wait());

        controller.wait_until_ready().await.unwrap();
        controller.start().unwrap();
        drop(execution_started.await.unwrap());
        gate_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn dropping_controller_cancels_gate() {
        let (controller, gate) = function_execution_start_barrier();
        drop(controller);

        let error = gate.wait().await.unwrap_err();
        assert!(error.to_string().contains("dropped before admission"));
    }

    #[tokio::test]
    async fn dropping_controller_after_admission_cancels_gate() {
        let (mut controller, gate) = function_execution_start_barrier();
        let gate_task = tokio::spawn(gate.wait());

        controller.wait_until_ready().await.unwrap();
        drop(controller);

        let error = gate_task.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("dropped before release"));
    }

    #[tokio::test]
    async fn dropping_gate_cancels_controller_wait() {
        let (mut controller, gate) = function_execution_start_barrier();
        drop(gate);

        let error = controller.wait_until_ready().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("before reaching its start barrier"));
    }
}
