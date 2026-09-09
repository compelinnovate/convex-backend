use bytes::Bytes;
use node_executor::{
    handle_node_executor_stream,
    InvokeCompletion,
    InvokeResponse,
    NodeExecutorStreamPart,
};
use serde_json::json;
use tokio::sync::mpsc;

#[tokio::test]
async fn public_stream_api_preserves_completion_authority() {
    for explicit in [false, true] {
        let failure = InvokeResponse {
            response: json!({ "type": "error" }),
            aws_request_id: None,
        };
        let completion = if explicit {
            InvokeCompletion::ExplicitError(failure)
        } else {
            InvokeCompletion::ImplicitError(failure)
        };
        let (sender, _receiver) = mpsc::unbounded_channel();
        let stream = futures::stream::iter([
            Ok(NodeExecutorStreamPart::Chunk(Bytes::from_static(
                b"{\"type\":\"success\"}\n{\"crash\":\"\xff",
            ))),
            Ok(NodeExecutorStreamPart::InvokeComplete(completion)),
        ]);
        match handle_node_executor_stream(sender, stream).await.unwrap() {
            Ok(result) => {
                assert!(!explicit);
                assert_eq!(result, json!({ "type": "success" }));
            },
            Err(failure) => {
                assert!(explicit);
                assert_eq!(failure.response, json!({ "type": "error" }));
            },
        }
    }
}

#[tokio::test]
async fn public_stream_api_rejects_duplicate_results_with_inferred_failure() {
    let (sender, _receiver) = mpsc::unbounded_channel();
    let stream = futures::stream::iter([
        Ok(NodeExecutorStreamPart::Chunk(Bytes::from_static(
            b"{\"type\":\"success\"}\n{\"type\":\"success\"}",
        ))),
        Ok(NodeExecutorStreamPart::InvokeComplete(
            InvokeCompletion::ImplicitError(InvokeResponse {
                response: json!({ "type": "error" }),
                aws_request_id: None,
            }),
        )),
    ]);
    assert!(handle_node_executor_stream(sender, stream).await.is_err());
}
