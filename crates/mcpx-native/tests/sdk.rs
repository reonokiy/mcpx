use mcpx_core::{CallOutcome, Dialect, OutputValidation};
use mcpx_native::{
    Client, Execution, ProtocolMode, QueueOptions, QueuedRemote, ServerConfig as NativeServerConfig,
};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;
#[path = "support/sdk_server.rs"]
mod sdk_server;
use sdk_server::SdkServer;

#[tokio::test]
async fn official_sdk_http_supports_modern_legacy_json_sse_and_explicit_resume() {
    for (dialect, json_response) in [
        (Dialect::Modern, true),
        (Dialect::Modern, false),
        (Dialect::Legacy, true),
        (Dialect::Legacy, false),
        (Dialect::Auto, true),
    ] {
        let cancel = CancellationToken::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = SdkServer(calls.clone());
        let service: StreamableHttpService<SdkServer, LocalSessionManager> =
            StreamableHttpService::new(
                move || Ok(handler.clone()),
                Default::default(),
                StreamableHttpServerConfig::default()
                    .with_legacy_session_mode(dialect == Dialect::Legacy)
                    .with_json_response(json_response)
                    .with_cancellation_token(cancel.child_token()),
            );
        let router = axum::Router::new().nest_service("/mcp", service);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
        let shutdown = cancel.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .unwrap()
        });
        let mut remote = Client::new()
            .unwrap()
            .connect(&endpoint, dialect)
            .await
            .unwrap();
        let binding = remote.tool("echo").unwrap();
        assert!(binding.validate_json(json!({"message":1})).is_err());
        let args = binding
            .args()
            .set("message", "SDK interop")
            .unwrap()
            .finish()
            .unwrap();
        let CallOutcome::Completed(response) = remote.call(&binding, &args).await.unwrap() else {
            panic!()
        };
        assert!(matches!(
            response.output_validation,
            OutputValidation::Valid
        ));
        assert_eq!(response.raw["structuredContent"]["message"], "SDK interop");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            remote
                .drain_events()
                .events
                .iter()
                .any(|event| event.kind == "progress")
        );
        if dialect != Dialect::Legacy {
            remote.set_elicitation(true, false).unwrap();
            let args = binding
                .args()
                .set("message", "interactive")
                .unwrap()
                .finish()
                .unwrap();
            let CallOutcome::NeedsInput(handle) = remote.call(&binding, &args).await.unwrap()
            else {
                panic!()
            };
            assert!(matches!(
                remote
                    .resume(
                        &handle,
                        json!({"confirm":{"action":"accept","content":{"yes":true}}})
                    )
                    .await
                    .unwrap(),
                CallOutcome::Completed(_)
            ));
            assert_eq!(calls.load(Ordering::SeqCst), 3);
        }
        remote.close().await.unwrap();
        let before = calls.load(Ordering::SeqCst);
        let queued = QueuedRemote::connect(
            NativeServerConfig::Http {
                url: endpoint.clone(),
                credential: None,
                protocol: match dialect {
                    Dialect::Auto => ProtocolMode::Auto,
                    Dialect::Legacy => ProtocolMode::Legacy,
                    Dialect::Modern => ProtocolMode::Modern,
                },
            },
            QueueOptions::default(),
        )
        .await
        .unwrap();
        let echo = queued.tool("echo").await.unwrap();
        let mut operations: Vec<
            std::pin::Pin<
                Box<dyn std::future::Future<Output = mcpx_core::Result<CallOutcome>> + Send>,
            >,
        > = Vec::new();
        for index in 0..16 {
            if index % 3 == 0 {
                operations.push(Box::pin(
                    echo.submit_json(json!({"message":format!("sdk-concurrent-{index}")}))
                        .unwrap(),
                ));
            } else {
                let call = if index % 3 == 1 {
                    queued.execute("echo", &format!("--message sdk-concurrent-{index}"))
                } else {
                    queued.execute_argv(
                        "echo",
                        &["--message".into(), format!("sdk-concurrent-{index}")],
                    )
                }
                .unwrap();
                operations.push(Box::pin(async move {
                    call.await.map(|value| match value {
                        Execution::Outcome(value) => value,
                        _ => panic!(),
                    })
                }));
            }
        }
        for (index, operation) in operations.into_iter().enumerate() {
            let CallOutcome::Completed(response) = operation.await.unwrap() else {
                panic!()
            };
            assert_eq!(
                response.raw["structuredContent"]["message"],
                format!("sdk-concurrent-{index}")
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), before + 16);
        if dialect != Dialect::Legacy {
            queued.set_elicitation(true, false).unwrap().await.unwrap();
            let mut inputs = Vec::new();
            for _ in 0..16 {
                inputs.push(queued.execute("echo", "--message interactive").unwrap());
            }
            let mut resumed = Vec::new();
            for input in inputs {
                let Execution::Outcome(CallOutcome::NeedsInput(handle)) = input.await.unwrap()
                else {
                    panic!()
                };
                resumed.push(
                    queued
                        .resume(
                            &handle,
                            json!({"confirm":{"action":"accept","content":{"yes":true}}}),
                        )
                        .unwrap(),
                );
            }
            for call in resumed {
                assert!(matches!(call.await.unwrap(), CallOutcome::Completed(_)));
            }
            assert_eq!(calls.load(Ordering::SeqCst), before + 48);
        }
        queued.close().await.unwrap();
        cancel.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
    }
}
