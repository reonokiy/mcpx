use axum::{
    body::{Body, Bytes, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, Method},
    response::Response,
    routing::any,
};
use futures_util::StreamExt;
use mcpx_core::NotificationPhase;
use mcpx_native::{
    NotificationRecoveryPolicy, ProtocolMode, QueueOptions, QueuedRemote, ServerConfig,
};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use serde_json::Value;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
#[path = "support/sdk_server.rs"]
mod sdk_server;

struct Proxy {
    endpoint: String,
    http: reqwest::Client,
    gets: AtomicUsize,
    records: Mutex<Vec<(Method, HeaderMap, Bytes)>>,
    release: AtomicBool,
    ready: Notify,
}
async fn proxy(State(state): State<Arc<Proxy>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let bytes = to_bytes(body, mcpx_protocol::MAX_BODY).await.unwrap();
    state.records.lock().unwrap().push((
        parts.method.clone(),
        parts.headers.clone(),
        bytes.clone(),
    ));
    let get = parts.method == Method::GET;
    let index = if get {
        state.gets.fetch_add(1, Ordering::SeqCst) + 1
    } else {
        0
    };
    if index > 1 {
        while !state.release.load(Ordering::SeqCst) {
            let ready = state.ready.notified();
            if state.release.load(Ordering::SeqCst) {
                break;
            }
            ready.await;
        }
    }
    let mut fields = parts.headers;
    for name in ["host", "connection", "transfer-encoding", "content-length"] {
        fields.remove(name);
    }
    let response = state
        .http
        .request(parts.method, &state.endpoint)
        .headers(fields)
        .body(bytes)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let mut fields = response.headers().clone();
    for name in ["connection", "transfer-encoding", "content-length"] {
        fields.remove(name);
    }
    let mut stream = response.bytes_stream();
    let body = if index == 1 {
        // Disconnect only after an intact SDK priming event. Neither the event
        // ID nor JSON-RPC bytes are invented or changed by the proxy.
        let mut first = Vec::new();
        while let Some(bytes) = stream.next().await {
            first.extend_from_slice(&bytes.unwrap());
            assert!(first.len() <= 65536);
            if first.windows(2).any(|w| w == b"\n\n") || first.windows(4).any(|w| w == b"\r\n\r\n")
            {
                break;
            }
        }
        drop(stream);
        Body::from(first)
    } else {
        Body::from_stream(stream)
    };
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = fields;
    response
}

#[tokio::test]
async fn official_sdk_replays_cached_notifications_after_manual_or_automatic_legacy_get_recovery() {
    for automatic in [false, true] {
        let cancellation = CancellationToken::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = sdk_server::SdkServer(calls.clone());
        let service: StreamableHttpService<sdk_server::SdkServer, LocalSessionManager> =
            StreamableHttpService::new(
                move || Ok(handler.clone()),
                Default::default(),
                StreamableHttpServerConfig::default()
                    .with_legacy_session_mode(true)
                    .with_sse_keep_alive(None)
                    .with_sse_retry(Some(Duration::from_millis(10)))
                    .with_cancellation_token(cancellation.child_token()),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend = format!("http://{}/mcp", listener.local_addr().unwrap());
        let app = axum::Router::new().nest_service("/mcp", service);
        let shutdown = cancellation.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        let state = Arc::new(Proxy {
            endpoint: backend,
            http: reqwest::Client::new(),
            gets: AtomicUsize::new(0),
            records: Mutex::new(Vec::new()),
            release: AtomicBool::new(false),
            ready: Notify::new(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
        let app = axum::Router::new()
            .route("/mcp", any(proxy))
            .with_state(state.clone());
        let shutdown = cancellation.clone();
        let proxy_server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        let remote = QueuedRemote::connect(
            ServerConfig::Http {
                url: endpoint,
                protocol: ProtocolMode::Legacy,
                credential: None,
            },
            QueueOptions {
                http_notifications: true,
                notification_recovery: automatic.then(|| NotificationRecoveryPolicy {
                    max_attempts: 1,
                    backoff: Duration::from_millis(300),
                    timeout: Duration::from_secs(3),
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let first = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(status) = remote.notification_status().unwrap().await.unwrap()
                    && (status.phase == NotificationPhase::Failed
                        || automatic
                            && status.phase == NotificationPhase::Opening
                            && state.gets.load(Ordering::SeqCst) >= 2)
                {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        remote
            .execute("echo", "--message 'SDK subscription change'")
            .unwrap()
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let mut changes = remote.subscribe_catalog().unwrap();
        let resumed = if automatic {
            None
        } else {
            Some(remote.resume_notifications(first.resource_id).unwrap())
        };
        state.release.store(true, Ordering::SeqCst);
        state.ready.notify_waiters();
        if let Some(resumed) = resumed {
            resumed.await.unwrap();
        }
        assert!(
            tokio::time::timeout(Duration::from_secs(4), changes.changed())
                .await
                .unwrap()
                .unwrap()
                .invalidated
        );
        remote.close().await.unwrap();
        {
            let records = state.records.lock().unwrap();
            let gets: Vec<_> = records
                .iter()
                .filter(|(method, _, _)| method == Method::GET)
                .collect();
            assert_eq!(gets.len(), 2);
            assert!(gets[0].1.get("last-event-id").is_none());
            assert_eq!(gets[1].1["last-event-id"], "0");
            assert_eq!(gets[1].1["mcp-session-id"], gets[0].1["mcp-session-id"]);
            let messages: Vec<Value> = records
                .iter()
                .filter(|(method, _, _)| method == Method::POST)
                .map(|(_, _, bytes)| serde_json::from_slice(bytes).unwrap())
                .collect();
            assert_eq!(
                messages
                    .iter()
                    .filter(|r| r["method"] == "tools/call")
                    .count(),
                1
            );
            assert_eq!(
                messages
                    .iter()
                    .filter(|r| r["method"] == "initialize")
                    .count(),
                1
            );
            assert_eq!(
                records
                    .iter()
                    .filter(|(method, _, _)| method == Method::DELETE)
                    .count(),
                1
            );
        }
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), proxy_server)
            .await
            .unwrap()
            .unwrap();
    }
}
