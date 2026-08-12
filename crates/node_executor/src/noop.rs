use async_trait::async_trait;
use common::{
    execution_start::FunctionExecutionStartGate,
    log_lines::LogLine,
};
use errors::ErrorMetadata;
use model::source_packages::types::NodeExecutorPoolTopology;
use tokio::sync::mpsc;

use crate::executor::{
    ExecutorRequest,
    InvokeResponse,
    NodeExecutor,
};
pub struct NoopNodeExecutor {}

impl NoopNodeExecutor {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl NodeExecutor for NoopNodeExecutor {
    fn enable(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn validate_pool_topology(&self, topology: &NodeExecutorPoolTopology) -> anyhow::Result<()> {
        if !topology.is_empty() {
            anyhow::bail!(ErrorMetadata::bad_request(
                "NodeExecutorPoolsNotSupported",
                "This runtime does not support dedicated Node executor pools",
            ));
        }
        Ok(())
    }

    fn reconcile_pool_topology(
        &self,
        topology: &NodeExecutorPoolTopology,
        _version: common::types::Timestamp,
    ) -> anyhow::Result<()> {
        self.validate_pool_topology(topology)
    }

    async fn invoke(
        &self,
        _request: ExecutorRequest,
        _log_line_sender: mpsc::UnboundedSender<LogLine>,
        _function_execution_start: Option<FunctionExecutionStartGate>,
    ) -> anyhow::Result<InvokeResponse> {
        anyhow::bail!("NoopNodeExecutor cannot be used to invoke code.");
    }

    fn shutdown(&self) {}
}

#[cfg(test)]
mod tests {
    use common::execution_start::function_execution_start_barrier;

    use super::*;

    #[tokio::test]
    async fn gated_invocation_fails_before_ready() {
        let (mut controller, gate) = function_execution_start_barrier();
        let (log_line_sender, _log_line_receiver) = mpsc::unbounded_channel();
        let executor = NoopNodeExecutor::new();
        let invocation = executor.invoke(
            ExecutorRequest::BuildDeps(crate::executor::BuildDepsRequest {
                deps: vec![],
                upload_url: String::new(),
            }),
            log_line_sender,
            Some(gate),
        );

        assert!(invocation.await.is_err());
        assert!(controller.wait_until_ready().await.is_err());
    }
}
