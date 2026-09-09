#![feature(never_type)]
#![feature(unwrap_infallible)]
#![feature(stmt_expr_attributes)]
#![feature(try_blocks)]
#![feature(try_blocks_heterogeneous)]
#![feature(slice_split_once)]
#![feature(coroutines)]

mod executor;
pub mod local;
mod metrics;
pub mod noop;
pub mod routed;
pub mod source_package;

pub use crate::executor::{
    error_response_json,
    handle_node_executor_stream,
    AnalyzeRequest,
    AnalyzeResponse,
    BuildDepsRequest,
    ExecuteRequest,
    ExecutorRequest,
    InvokeCompletion,
    InvokeResponse,
    NodeActionOutcome,
    NodeActions,
    NodeExecutor,
    NodeExecutorCutoverReservation,
    NodeExecutorCutoverTarget,
    NodeExecutorStreamPart,
    Package,
    SourcePackage,
    ARGS_TOO_LARGE_RESPONSE_MESSAGE,
    EXECUTE_TIMEOUT_RESPONSE_JSON,
};
