use axum::{
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, Method},
    response::Response,
    routing::any,
};
use futures_util::StreamExt;
use mcpx_core::{CallOutcome, OutputValidation};
use mcpx_native::{
    Client, ProtocolMode, QueueOptions, QueuedRemote, ResponseRecoveryPolicy, ServerConfig,
};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
#[path = "support/sdk_server.rs"]
mod sdk_server;

struct Record {
    method: Method,
    headers: HeaderMap,
    rpc: Option<Value>,
    response: Arc<Mutex<Vec<u8>>>,
}
struct Proxy {
    endpoint: String,
    http: reqwest::Client,
    records: Mutex<Vec<Record>>,
    drainers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}
async fn proxy(State(state): State<Arc<Proxy>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, mcpx_protocol::MAX_BODY).await.unwrap();
    let rpc = (!body.is_empty()).then(|| serde_json::from_slice::<Value>(&body).unwrap());
    let cut = rpc
        .as_ref()
        .is_some_and(|rpc| rpc["method"] == "tools/call");
    let observed = Arc::new(Mutex::new(Vec::new()));
    state.records.lock().unwrap().push(Record {
        method: parts.method.clone(),
        headers: parts.headers.clone(),
        rpc,
        response: observed.clone(),
    });
    let mut headers = parts.headers;
    for name in ["host", "connection", "transfer-encoding", "content-length"] {
        headers.remove(name);
    }
    let response = state
        .http
        .request(parts.method, &state.endpoint)
        .headers(headers)
        .body(body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let mut headers = response.headers().clone();
    for name in ["connection", "transfer-encoding", "content-length"] {
        headers.remove(name);
    }
    let mut stream = response.bytes_stream();
    let body = if cut {
        let mut first = Vec::new();
        let end = loop {
            let bytes = stream.next().await.expect("SDK priming").unwrap();
            first.extend_from_slice(&bytes);
            assert!(first.len() <= 65536);
            if let Some(end) = first.windows(4).position(|w| w == b"\r\n\r\n") {
                break end + 4;
            }
            if let Some(end) = first.windows(2).position(|w| w == b"\n\n") {
                break end + 2;
            }
        };
        let priming = first[..end].to_vec();
        observed.lock().unwrap().extend(&priming);
        assert!(!String::from_utf8_lossy(&priming).contains("\"result\""));
        // Keep consuming the upstream original stream, so the stock SDK's live
        // request route remains populated. Only cut the frontend at a real event.
        let task = tokio::spawn(async move {
            while let Some(bytes) = stream.next().await {
                if bytes.is_err() {
                    break;
                }
            }
        });
        state.drainers.lock().unwrap().push(task);
        Body::from(priming)
    } else {
        Body::from_stream(stream.map(move |bytes| {
            if let Ok(bytes) = &bytes {
                let mut out = observed.lock().unwrap();
                if out.len() < mcpx_protocol::MAX_BODY {
                    out.extend_from_slice(bytes);
                }
            }
            bytes
        }))
    };
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

#[tokio::test]
async fn official_sdk_original_result_arrives_through_get_for_mutable_and_queued_clients() {
    for queued in [false, true] {
        let cancel = CancellationToken::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = sdk_server::SdkServer(calls.clone());
        let mut sessions = LocalSessionManager::default();
        sessions.session_config.sse_retry = Some(Duration::from_millis(10));
        let service: StreamableHttpService<sdk_server::SdkServer, LocalSessionManager> =
            StreamableHttpService::new(
                move || Ok(handler.clone()),
                Arc::new(sessions),
                StreamableHttpServerConfig::default()
                    .with_legacy_session_mode(true)
                    .with_sse_keep_alive(None)
                    .with_sse_retry(Some(Duration::from_millis(10)))
                    .with_cancellation_token(cancel.child_token()),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend = format!("http://{}/mcp", listener.local_addr().unwrap());
        let app = axum::Router::new().nest_service("/mcp", service);
        let shutdown = cancel.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        let state = Arc::new(Proxy {
            endpoint: backend,
            http: reqwest::Client::new(),
            records: Mutex::new(vec![]),
            drainers: Mutex::new(vec![]),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
        let app = axum::Router::new()
            .route("/mcp", any(proxy))
            .with_state(state.clone());
        let shutdown = cancel.clone();
        let proxy_server = tokio::spawn(async move {
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
        let policy = ResponseRecoveryPolicy {
            max_attempts: 2,
            backoff: Duration::ZERO,
            timeout: Duration::from_secs(3),
        };
        let result = if queued {
            let remote = QueuedRemote::spawn_with(
                config,
                QueueOptions {
                    response_recovery: Some(policy),
                    timeout: Duration::from_secs(5),
                    ..Default::default()
                },
                || Ok(Client::new()?.with_bearer("sdk-frozen".into())),
            )
            .await
            .unwrap();
            let binding = remote.tool("echo").await.unwrap();
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                binding
                    .submit_json(json!({"message":"SDK response recovery"}))
                    .unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
            remote.close().await.unwrap();
            result
        } else {
            let mut remote = config
                .connect(
                    Client::new()
                        .unwrap()
                        .with_bearer("sdk-frozen".into())
                        .with_response_recovery(policy)
                        .unwrap(),
                    None,
                )
                .await
                .unwrap();
            let binding = remote.fresh_tool("echo").await.unwrap();
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                remote.call_json(&binding, json!({"message":"SDK response recovery"})),
            )
            .await
            .unwrap()
            .unwrap();
            remote.close().await.unwrap();
            result
        };
        let CallOutcome::Completed(result) = result else {
            panic!("expected SDK final result")
        };
        assert_eq!(
            result.raw["structuredContent"]["message"],
            "SDK response recovery"
        );
        assert!(matches!(result.output_validation, OutputValidation::Valid));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        {
            let records = state.records.lock().unwrap();
            let posts: Vec<_> = records
                .iter()
                .filter(|r| r.rpc.as_ref().is_some_and(|r| r["method"] == "tools/call"))
                .collect();
            assert_eq!(posts.len(), 1);
            assert_eq!(
                records
                    .iter()
                    .filter(|r| r.rpc.as_ref().is_some_and(|r| r["method"] == "initialize"))
                    .count(),
                1
            );
            let gets: Vec<_> = records.iter().filter(|r| r.method == Method::GET).collect();
            assert!(!gets.is_empty());
            let id = posts[0].rpc.as_ref().unwrap()["id"].clone();
            assert!(gets.iter().all(|r| r.headers.contains_key("last-event-id")
                && r.headers["mcp-session-id"] == posts[0].headers["mcp-session-id"]));
            assert!(
                records
                    .iter()
                    .all(|r| r.headers["authorization"] == "Bearer sdk-frozen"
                        && (r.method == Method::GET || !r.headers.contains_key("last-event-id")))
            );
            assert!(
                !String::from_utf8_lossy(&posts[0].response.lock().unwrap()).contains("\"result\"")
            );
            let wire = gets
                .iter()
                .flat_map(|r| r.response.lock().unwrap().clone())
                .collect::<Vec<_>>();
            let wire = String::from_utf8(wire).unwrap();
            assert!(wire.contains(&format!("\"id\":{id}")));
            assert!(wire.contains("\"result\""));
            assert!(wire.contains("SDK response recovery"));
        }
        let drainers = std::mem::take(&mut *state.drainers.lock().unwrap());
        for task in drainers {
            tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
        }
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), proxy_server)
            .await
            .unwrap()
            .unwrap();
    }
}
