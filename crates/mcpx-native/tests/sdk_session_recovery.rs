use mcpx_core::{CallOutcome, ExecutionCertainty};
use mcpx_native::{
    Client, Execution, HttpSessionRecoveryPolicy, ProtocolMode, QueueOptions, QueuedRemote,
    ServerConfig,
};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService,
    session::{SessionManager, local::LocalSessionManager},
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
#[path = "support/sdk_server.rs"]
mod sdk_server;

struct InitialExpiry {
    manager: Arc<LocalSessionManager>,
    first: std::sync::atomic::AtomicBool,
    initialized: AtomicUsize,
    deleted: AtomicUsize,
    actual_status: AtomicUsize,
}
async fn expire_before_first_sdk_list(
    axum::extract::State(state): axum::extract::State<Arc<InitialExpiry>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let (parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, mcpx_protocol::MAX_BODY)
        .await
        .unwrap();
    let rpc: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    if rpc["method"] == "initialize" {
        state.initialized.fetch_add(1, Ordering::SeqCst);
    }
    if parts.method == axum::http::Method::DELETE {
        state.deleted.fetch_add(1, Ordering::SeqCst);
    }
    let expire = rpc["method"] == "tools/list" && state.first.swap(false, Ordering::SeqCst);
    if expire {
        let session = {
            let sessions = state.manager.sessions.read().await;
            assert_eq!(sessions.len(), 1);
            sessions.keys().next().unwrap().clone()
        };
        assert_eq!(
            session.to_string(),
            parts.headers["mcp-session-id"].to_str().unwrap()
        );
        state.manager.close_session(&session).await.unwrap();
    }
    // Forward the original request. The stock SDK decides its response status.
    let response = next
        .run(axum::extract::Request::from_parts(
            parts,
            axum::body::Body::from(bytes),
        ))
        .await;
    if expire {
        state
            .actual_status
            .store(response.status().as_u16() as usize, Ordering::SeqCst);
    }
    response
}

fn message(outcome: CallOutcome) -> Value {
    let CallOutcome::Completed(value) = outcome else {
        panic!("expected SDK tool result")
    };
    value.raw["structuredContent"]["message"].clone()
}

#[tokio::test]
async fn actual_sdk_session_termination_renews_without_replaying_the_failed_tool() {
    for json_preferred in [true, false] {
        let cancel = CancellationToken::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = sdk_server::SdkServer(calls.clone());
        let manager = Arc::new(LocalSessionManager::default());
        let service: StreamableHttpService<sdk_server::SdkServer, LocalSessionManager> =
            StreamableHttpService::new(
                move || Ok(handler.clone()),
                manager.clone(),
                StreamableHttpServerConfig::default()
                    .with_legacy_session_mode(true)
                    .with_json_response(json_preferred)
                    .with_cancellation_token(cancel.child_token()),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
        let app = axum::Router::new().nest_service("/mcp", service);
        let shutdown = cancel.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        let remote = QueuedRemote::spawn_with(
            ServerConfig::Http {
                url: endpoint,
                protocol: ProtocolMode::Legacy,
                credential: None,
            },
            QueueOptions::default(),
            || {
                Client::new()?.with_http_session_recovery(HttpSessionRecoveryPolicy {
                    max_attempts: 2,
                    backoff: Duration::from_millis(10),
                    timeout: Duration::from_secs(3),
                })
            },
        )
        .await
        .unwrap();
        let binding = remote.tool("echo").await.unwrap();
        assert_eq!(
            message(
                binding
                    .submit_json(json!({"message":"before"}))
                    .unwrap()
                    .await
                    .unwrap()
            ),
            "before"
        );
        let old_session = {
            let sessions = manager.sessions.read().await;
            assert_eq!(sessions.len(), 1);
            sessions.keys().next().unwrap().clone()
        };
        manager.close_session(&old_session).await.unwrap();
        assert!(!manager.has_session(&old_session).await.unwrap());
        let error = binding
            .submit_json(json!({"message":"never-replayed"}))
            .unwrap()
            .await
            .unwrap_err();
        assert_eq!(error.code, "SessionExpired");
        assert_eq!(error.execution_certainty, ExecutionCertainty::Unknown);
        assert!(error.operation_id.is_some());
        let Execution::Outcome(outcome) = remote
            .execute("echo", "--message after")
            .unwrap()
            .await
            .unwrap()
        else {
            panic!("expected SDK result")
        };
        assert_eq!(message(outcome), "after");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "the failed tool was replayed after session renewal"
        );
        {
            let sessions = manager.sessions.read().await;
            assert_eq!(sessions.len(), 1);
            assert!(!sessions.contains_key(&old_session));
        }
        let stale = binding
            .submit_json(json!({"message":"never-old"}))
            .unwrap()
            .await
            .unwrap_err();
        assert_eq!(stale.code, "BindingStale");
        assert_eq!(stale.execution_certainty, ExecutionCertainty::NotSent);
        remote.close().await.unwrap();
        assert!(manager.sessions.read().await.is_empty());
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn actual_sdk_session_expiry_during_initial_catalog_discovery_is_recovered() {
    for queued in [false, true] {
        for json_preferred in [false, true] {
            let cancel = CancellationToken::new();
            let calls = Arc::new(AtomicUsize::new(0));
            let handler = sdk_server::SdkServer(calls.clone());
            let manager = Arc::new(LocalSessionManager::default());
            let state = Arc::new(InitialExpiry {
                manager: manager.clone(),
                first: std::sync::atomic::AtomicBool::new(true),
                initialized: AtomicUsize::new(0),
                deleted: AtomicUsize::new(0),
                actual_status: AtomicUsize::new(0),
            });
            let service: StreamableHttpService<sdk_server::SdkServer, LocalSessionManager> =
                StreamableHttpService::new(
                    move || Ok(handler.clone()),
                    manager.clone(),
                    StreamableHttpServerConfig::default()
                        .with_legacy_session_mode(true)
                        .with_json_response(json_preferred)
                        .with_cancellation_token(cancel.child_token()),
                );
            let app = axum::Router::new().nest_service("/mcp", service).layer(
                axum::middleware::from_fn_with_state(state.clone(), expire_before_first_sdk_list),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
            let shutdown = cancel.clone();
            let server = tokio::spawn(async move {
                axum::serve(listener, app)
                    .with_graceful_shutdown(shutdown.cancelled_owned())
                    .await
                    .unwrap();
            });
            let config = ServerConfig::Http {
                url: endpoint,
                protocol: ProtocolMode::Legacy,
                credential: None,
            };
            let client = || {
                Client::new()?.with_http_session_recovery(HttpSessionRecoveryPolicy {
                    max_attempts: 2,
                    backoff: Duration::from_millis(10),
                    timeout: Duration::from_secs(3),
                })
            };
            if queued {
                let remote = QueuedRemote::spawn_with(config, QueueOptions::default(), client)
                    .await
                    .unwrap();
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                let Execution::Outcome(result) = remote
                    .execute("echo", "--message after")
                    .unwrap()
                    .await
                    .unwrap()
                else {
                    panic!()
                };
                assert_eq!(message(result), "after");
                remote.close().await.unwrap();
            } else {
                let mut remote = config.connect(client().unwrap(), None).await.unwrap();
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                let Execution::Outcome(result) =
                    remote.execute("echo", "--message after").await.unwrap()
                else {
                    panic!()
                };
                assert_eq!(message(result), "after");
                remote.close().await.unwrap();
            }
            assert_eq!(state.actual_status.load(Ordering::SeqCst), 404);
            assert_eq!(state.initialized.load(Ordering::SeqCst), 2);
            assert_eq!(state.deleted.load(Ordering::SeqCst), 2);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(manager.sessions.read().await.is_empty());
            cancel.cancel();
            tokio::time::timeout(Duration::from_secs(3), server)
                .await
                .unwrap()
                .unwrap();
        }
    }
}

#[tokio::test]
async fn mutable_sdk_client_renews_before_the_next_operation_without_replaying_a_failure() {
    for json_preferred in [false, true] {
        let cancel = CancellationToken::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = sdk_server::SdkServer(calls.clone());
        let manager = Arc::new(LocalSessionManager::default());
        let service: StreamableHttpService<sdk_server::SdkServer, LocalSessionManager> =
            StreamableHttpService::new(
                move || Ok(handler.clone()),
                manager.clone(),
                StreamableHttpServerConfig::default()
                    .with_legacy_session_mode(true)
                    .with_json_response(json_preferred)
                    .with_cancellation_token(cancel.child_token()),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
        let shutdown = cancel.clone();
        let app = axum::Router::new().nest_service("/mcp", service);
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        let mut remote = Client::new()
            .unwrap()
            .with_http_session_recovery(HttpSessionRecoveryPolicy {
                max_attempts: 2,
                backoff: Duration::from_millis(10),
                timeout: Duration::from_secs(3),
            })
            .unwrap()
            .connect(&endpoint, mcpx_core::Dialect::Legacy)
            .await
            .unwrap();
        let old = remote.tool("echo").unwrap();
        assert_eq!(
            message(
                remote
                    .call_json(&old, json!({"message":"before"}))
                    .await
                    .unwrap()
            ),
            "before"
        );
        let expired = manager.sessions.read().await.keys().next().unwrap().clone();
        manager.close_session(&expired).await.unwrap();
        let e = remote
            .call_json(&old, json!({"message":"never-replayed"}))
            .await
            .unwrap_err();
        assert_eq!(e.code, "SessionExpired");
        assert_eq!(e.execution_certainty, ExecutionCertainty::Unknown);
        assert!(manager.sessions.read().await.is_empty());
        let Execution::Outcome(result) = remote.execute("echo", "--message after").await.unwrap()
        else {
            panic!()
        };
        assert_eq!(message(result), "after");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(manager.sessions.read().await.len(), 1);
        assert!(!manager.has_session(&expired).await.unwrap());
        assert_eq!(
            remote
                .call_json(&old, json!({"message":"never-old"}))
                .await
                .unwrap_err()
                .code,
            "BindingStale"
        );
        remote.close().await.unwrap();
        assert!(manager.sessions.read().await.is_empty());
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
    }
}
