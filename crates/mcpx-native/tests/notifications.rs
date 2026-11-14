use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response, Sse, sse::Event},
    routing::post,
};
use futures_util::stream;
use mcpx_core::{
    Credential, CredentialFuture, CredentialProvider, CredentialRequest, NotificationPhase,
};
use mcpx_native::{Client, ProtocolMode, QueueOptions, QueuedRemote, ServerConfig};
use serde_json::{Value, json};
use std::{
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, mpsc};

type Subscription = (mpsc::UnboundedSender<Event>, Option<u64>, String);
struct Data {
    legacy: bool,
    ack: bool,
    unsupported: bool,
    subscriptions: Mutex<Vec<Subscription>>,
    requests: Mutex<Vec<Value>>,
    transports: Mutex<Vec<(String, String, String)>>,
    changed: Notify,
    release: Notify,
    released: AtomicBool,
    closed: AtomicUsize,
    replies: AtomicUsize,
    hold_reply: AtomicBool,
    release_reply: Notify,
    hold_metadata: AtomicBool,
    release_metadata: Notify,
}
struct Guard(Arc<Data>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.closed.fetch_add(1, Ordering::SeqCst);
        self.0.changed.notify_waiters();
    }
}
async fn subscribe(data: Arc<Data>, headers: HeaderMap, rpc: Option<u64>) -> Response {
    let auth = headers
        .get("authorization")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let session = headers
        .get("mcp-session-id")
        .map(|v| v.to_str().unwrap())
        .unwrap_or("")
        .to_owned();
    data.transports
        .lock()
        .unwrap()
        .push(("listen".into(), auth.clone(), session.clone()));
    if data.unsupported {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    assert_eq!(headers.get("last-event-id"), None);
    if data.legacy {
        assert!(rpc.is_none());
        assert_eq!(
            session,
            format!("session-{}", auth.trim_start_matches("Bearer "))
        );
        assert!(headers.get("mcp-method").is_none());
    } else {
        assert!(session.is_empty());
        assert_eq!(headers["mcp-method"], "subscriptions/listen");
    }
    let (sender, receiver) = mpsc::unbounded_channel();
    if !data.legacy && data.ack {
        sender.send(Event::default().data(json!({"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":rpc},"notifications":{"toolsListChanged":true}}}).to_string())).unwrap();
    }
    data.subscriptions.lock().unwrap().push((sender, rpc, auth));
    data.changed.notify_waiters();
    let body = stream::unfold(
        (receiver, Guard(data)),
        |(mut receiver, guard)| async move {
            receiver
                .recv()
                .await
                .map(|event| (Ok::<_, Infallible>(event), (receiver, guard)))
        },
    );
    Sse::new(body).into_response()
}
async fn get(State(data): State<Arc<Data>>, headers: HeaderMap) -> Response {
    subscribe(data, headers, None).await
}
async fn rpc(
    State(data): State<Arc<Data>>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Response {
    data.requests.lock().unwrap().push(request.clone());
    data.changed.notify_waiters();
    let auth = headers
        .get("authorization")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let method = request["method"].as_str().unwrap_or("reply");
    if method == "reply" {
        assert_eq!(request["id"], -7);
        assert_eq!(request["error"]["code"], -32601);
        data.replies.fetch_add(1, Ordering::SeqCst);
        data.changed.notify_waiters();
        if data.hold_reply.load(Ordering::SeqCst) {
            data.release_reply.notified().await;
        }
        return StatusCode::ACCEPTED.into_response();
    }
    if method == "subscriptions/listen" {
        return subscribe(data, headers, request["id"].as_u64()).await;
    }
    if data.legacy && method == "server/discover" {
        return(StatusCode::BAD_REQUEST,Json(json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32601,"message":"initialize first"}}))).into_response();
    }
    if method == "notifications/initialized" {
        return StatusCode::ACCEPTED.into_response();
    }
    let mut session = None;
    let result = match method {
        "server/discover" => {
            json!({"supportedVersions":["2026-07-28"],"capabilities":{"tools":{"listChanged":true}}})
        }
        "initialize" => {
            session = Some(format!("session-{}", auth.trim_start_matches("Bearer ")));
            json!({"protocolVersion":"2025-11-25","capabilities":{"tools":{"listChanged":true}}})
        }
        "tools/list" => {
            while data.hold_metadata.load(Ordering::SeqCst) {
                let ready = data.release_metadata.notified();
                if !data.hold_metadata.load(Ordering::SeqCst) {
                    break;
                }
                ready.await;
            }
            json!({"tools":[{"name":"echo","inputSchema":{"type":"object","properties":{"message":{"type":"string"}}}}]})
        }
        "tools/call" => {
            if request["params"]["arguments"]["message"]
                .as_str()
                .is_some_and(|m| m.starts_with("held"))
            {
                while !data.released.load(Ordering::SeqCst) {
                    let ready = data.release.notified();
                    if data.released.load(Ordering::SeqCst) {
                        break;
                    }
                    ready.await;
                }
            }
            json!({"content":[],"structuredContent":{"message":request["params"]["arguments"]["message"]}})
        }
        _ => panic!("unexpected method"),
    };
    let mut response =
        Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result})).into_response();
    if let Some(session) = session {
        response
            .headers_mut()
            .insert("mcp-session-id", session.parse().unwrap());
    }
    response
}
async fn delete(State(data): State<Arc<Data>>, headers: HeaderMap) -> StatusCode {
    data.transports.lock().unwrap().push((
        "delete".into(),
        headers["authorization"].to_str().unwrap().into(),
        headers["mcp-session-id"].to_str().unwrap().into(),
    ));
    StatusCode::NO_CONTENT
}
struct Fixture {
    data: Arc<Data>,
    config: ServerConfig,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn fixture(legacy: bool, auto: bool, ack: bool, unsupported: bool) -> Fixture {
    let data = Arc::new(Data {
        legacy,
        ack,
        unsupported,
        subscriptions: Mutex::new(Vec::new()),
        requests: Mutex::new(Vec::new()),
        transports: Mutex::new(Vec::new()),
        changed: Notify::new(),
        release: Notify::new(),
        released: AtomicBool::new(false),
        closed: AtomicUsize::new(0),
        replies: AtomicUsize::new(0),
        hold_reply: AtomicBool::new(false),
        release_reply: Notify::new(),
        hold_metadata: AtomicBool::new(false),
        release_metadata: Notify::new(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/mcp", post(rpc).get(get).delete(delete))
        .with_state(data.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Fixture {
        data,
        config: ServerConfig::Http {
            url: format!("http://{address}/mcp"),
            protocol: if auto {
                ProtocolMode::Auto
            } else if legacy {
                ProtocolMode::Legacy
            } else {
                ProtocolMode::Modern
            },
            credential: None,
        },
        task,
    }
}
async fn wait(data: &Data, predicate: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let ready = data.changed.notified();
            if predicate() {
                break;
            }
            ready.await;
        }
    })
    .await
    .unwrap();
}
async fn phase(remote: &QueuedRemote, expected: NotificationPhase) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if remote
                .notification_status()
                .unwrap()
                .await
                .unwrap()
                .is_some_and(|s| s.phase == expected)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn notify(data: &Data, index: usize, method: &str) {
    let entries = data.subscriptions.lock().unwrap();
    let (sender, rpc, _) = &entries[index];
    sender.send(Event::default().data(json!({"jsonrpc":"2.0","method":method,"params":if data.legacy{json!({})}else{json!({"_meta":{"io.modelcontextprotocol/subscriptionId":rpc}})}}).to_string())).unwrap();
}

#[tokio::test]
async fn manual_controls_bypass_full_business_admission_and_stop_survives_reconnect() {
    for legacy in [false, true] {
        for auto in [false, true] {
            let fixture = fixture(legacy, auto, true, false).await;
            let remote = QueuedRemote::spawn_with(
                fixture.config.clone(),
                QueueOptions {
                    capacity: 16,
                    ..Default::default()
                },
                || Ok(Client::new()?.with_bearer("alice".into())),
            )
            .await
            .unwrap();
            assert!(
                remote
                    .notification_status()
                    .unwrap()
                    .await
                    .unwrap()
                    .is_none()
            );
            let first = remote
                .start_notifications()
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            phase(&remote, NotificationPhase::Listening).await;
            assert_eq!(
                remote
                    .start_notifications()
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap()
                    .resource_id,
                first.resource_id
            );
            let mut calls = Vec::new();
            for index in 0..16 {
                calls.push(
                    remote
                        .execute("echo", &format!("--message held-{index}"))
                        .unwrap(),
                );
            }
            wait(&fixture.data, || {
                fixture
                    .data
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|r| r["method"] == "tools/call")
                    .count()
                    == 16
            })
            .await;
            assert_eq!(
                remote
                    .execute("echo", "--message overflow")
                    .err()
                    .unwrap()
                    .code,
                "QueueFull"
            );
            let stop = remote.stop_notifications().unwrap();
            let status = remote.notification_status().unwrap();
            let stopped = tokio::time::timeout(Duration::from_secs(1), stop)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(stopped.phase, NotificationPhase::Cancelled);
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), status)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap()
                    .phase,
                NotificationPhase::Cancelled
            );
            assert_eq!(fixture.data.subscriptions.lock().unwrap().len(), 1);
            wait(&fixture.data, || {
                fixture.data.closed.load(Ordering::SeqCst) == 1
            })
            .await;
            fixture.data.released.store(true, Ordering::SeqCst);
            fixture.data.release.notify_waiters();
            for call in calls {
                call.await.unwrap();
            }
            remote.reconnect().unwrap().await.unwrap();
            assert_eq!(fixture.data.subscriptions.lock().unwrap().len(), 1);
            let second = remote
                .start_notifications()
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            assert_ne!(second.resource_id, first.resource_id);
            phase(&remote, NotificationPhase::Listening).await;
            assert_eq!(fixture.data.subscriptions.lock().unwrap().len(), 2);
            remote.close().await.unwrap();
        }
    }
}

struct WaitingProvider {
    armed: Arc<AtomicBool>,
    entered: Arc<Notify>,
    calls: Arc<AtomicUsize>,
}

#[tokio::test]
async fn explicit_restart_replaces_failed_or_completed_resources_without_automatic_retry() {
    for (legacy, ack, unsupported, clean) in [
        (false, false, false, false),
        (true, true, true, false),
        (false, true, false, true),
    ] {
        let fixture = fixture(legacy, false, ack, unsupported).await;
        let remote =
            QueuedRemote::spawn_with(fixture.config.clone(), QueueOptions::default(), || {
                Ok(Client::new()?
                    .with_bearer("alice".into())
                    .with_timeout(Duration::from_millis(80)))
            })
            .await
            .unwrap();
        let first = remote
            .start_notifications()
            .unwrap()
            .await
            .unwrap()
            .unwrap();
        if clean {
            phase(&remote, NotificationPhase::Listening).await;
            let entries = fixture.data.subscriptions.lock().unwrap();
            let (sender, rpc, _) = &entries[0];
            sender.send(Event::default().data(json!({"jsonrpc":"2.0","id":rpc,"result":{"resultType":"complete","_meta":{"io.modelcontextprotocol/subscriptionId":rpc}}}).to_string())).unwrap();
        }
        phase(
            &remote,
            if clean {
                NotificationPhase::Closed
            } else {
                NotificationPhase::Failed
            },
        )
        .await;
        let before = fixture
            .data
            .transports
            .lock()
            .unwrap()
            .iter()
            .filter(|(method, _, _)| method == "listen")
            .count();
        assert_eq!(before, 1);
        let second = remote
            .start_notifications()
            .unwrap()
            .await
            .unwrap()
            .unwrap();
        assert_ne!(second.resource_id, first.resource_id);
        if !clean {
            phase(&remote, NotificationPhase::Failed).await;
        } else {
            phase(&remote, NotificationPhase::Listening).await;
        }
        assert_eq!(
            fixture
                .data
                .transports
                .lock()
                .unwrap()
                .iter()
                .filter(|(method, _, _)| method == "listen")
                .count(),
            2
        );
        remote.close().await.unwrap();
    }
}
impl CredentialProvider for WaitingProvider {
    fn resolve<'a>(&'a mut self, _: CredentialRequest<'a>) -> CredentialFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let waiting = self.armed.swap(false, Ordering::SeqCst);
        Box::pin(async move {
            if waiting {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            Credential::bearer("alice".into(), "alice".into())
        })
    }
}

#[tokio::test]
async fn stop_and_status_progress_during_start_provider_wait_without_resolving_again() {
    let fixture = fixture(false, false, true, false).await;
    let armed = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let provider = WaitingProvider {
        armed: armed.clone(),
        entered: entered.clone(),
        calls: calls.clone(),
    };
    let remote =
        QueuedRemote::spawn_with(fixture.config.clone(), QueueOptions::default(), move || {
            Ok(Client::new()?.with_credential_provider(provider))
        })
        .await
        .unwrap();
    let initial = calls.load(Ordering::SeqCst);
    armed.store(true, Ordering::SeqCst);
    let start = remote.start_notifications().unwrap();
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(1),
            remote.notification_status().unwrap()
        )
        .await
        .unwrap()
        .unwrap()
        .is_none()
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(1), remote.stop_notifications().unwrap())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    let error = tokio::time::timeout(Duration::from_secs(1), start)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code, "Cancelled");
    assert_eq!(
        error.execution_certainty,
        mcpx_core::ExecutionCertainty::NotSent
    );
    assert_eq!(calls.load(Ordering::SeqCst), initial + 1);
    assert!(fixture.data.subscriptions.lock().unwrap().is_empty());
    remote.start_notifications().unwrap().await.unwrap();
    phase(&remote, NotificationPhase::Listening).await;
    assert_eq!(calls.load(Ordering::SeqCst), initial + 2);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn reserved_controls_have_independent_bounds_deadlines_cancellation_and_lifecycle() {
    let fixture = fixture(false, false, true, false).await;
    let (remote, mut events) = QueuedRemote::spawn_with_events(
        fixture.config.clone(),
        QueueOptions {
            capacity: 1,
            http_notifications: true,
            ..Default::default()
        },
        mcpx_core::EventLimits::default(),
        || Ok(Client::new()?.with_bearer("alice".into())),
    )
    .await
    .unwrap();
    phase(&remote, NotificationPhase::Listening).await;
    fixture.data.hold_metadata.store(true, Ordering::SeqCst);
    let refresh = remote.refresh().unwrap();
    wait(&fixture.data, || {
        fixture
            .data
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r["method"] == "tools/list")
            .count()
            == 2
    })
    .await;
    let first = remote
        .notification_status_with_timeout(Duration::from_millis(40))
        .unwrap();
    let second = remote
        .stop_notifications_with_timeout(Duration::from_millis(40))
        .unwrap();
    let mut ids = vec![first.id(), second.id()];
    assert_eq!(
        remote.notification_status().err().unwrap().code,
        "QueueFull"
    );
    for operation in [first, second] {
        let error = tokio::time::timeout(Duration::from_secs(1), operation)
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, "Timeout");
        assert_eq!(
            error.execution_certainty,
            mcpx_core::ExecutionCertainty::NotSent
        );
    }
    let cancelled = remote.stop_notifications().unwrap();
    ids.push(cancelled.id());
    cancelled.cancel();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), cancelled)
            .await
            .unwrap()
            .unwrap_err()
            .code,
        "Cancelled"
    );
    fixture.data.hold_metadata.store(false, Ordering::SeqCst);
    fixture.data.release_metadata.notify_waiters();
    refresh.await.unwrap();
    phase(&remote, NotificationPhase::Listening).await;
    assert_eq!(fixture.data.subscriptions.lock().unwrap().len(), 1);
    assert_eq!(
        remote
            .start_notifications_with_timeout(Duration::ZERO)
            .err()
            .unwrap()
            .code,
        "ConfigurationError"
    );
    remote.close().await.unwrap();
    let mut records = Vec::new();
    while let Some(batch) = events.next_batch().await {
        records.extend(batch.events);
    }
    for id in ids {
        let terminals: Vec<_> = records
            .iter()
            .filter(|event| {
                event.queue_operation_id == id
                    && !matches!(
                        event.kind,
                        mcpx_core::OperationEventKind::Queued
                            | mcpx_core::OperationEventKind::Started
                    )
            })
            .collect();
        assert_eq!(terminals.len(), 1);
    }
    assert_eq!(
        remote.notification_status().err().unwrap().code,
        "TransportClosed"
    );
}

#[tokio::test]
async fn background_notifications_do_not_consume_call_slots_and_invalidate_while_sixteen_calls_are_pending()
 {
    for legacy in [false, true] {
        for auto in [false, true] {
            let fixture = fixture(legacy, auto, true, false).await;
            let remote = QueuedRemote::spawn_with(
                fixture.config.clone(),
                QueueOptions {
                    http_notifications: true,
                    ..Default::default()
                },
                || Ok(Client::new()?.with_bearer("alice".into())),
            )
            .await
            .unwrap();
            phase(&remote, NotificationPhase::Listening).await;
            let mut watcher = remote.subscribe_catalog().unwrap();
            let mut calls = Vec::new();
            for index in 0..16 {
                calls.push(
                    remote
                        .execute_argv("echo", &["--message".into(), format!("held-{index}")])
                        .unwrap(),
                );
            }
            wait(&fixture.data, || {
                fixture
                    .data
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|r| r["method"] == "tools/call")
                    .count()
                    == 16
            })
            .await;
            let last = remote
                .execute_argv("echo", &["--message".into(), "seventeenth".into()])
                .unwrap();
            notify(&fixture.data, 0, "notifications/tools/list_changed");
            assert!(
                tokio::time::timeout(Duration::from_secs(2), watcher.changed())
                    .await
                    .unwrap()
                    .unwrap()
                    .invalidated
            );
            assert_eq!(
                fixture
                    .data
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|r| r["method"] == "tools/list")
                    .count(),
                1
            );
            fixture.data.released.store(true, Ordering::SeqCst);
            fixture.data.release.notify_waiters();
            for call in calls {
                call.await.unwrap();
            }
            last.await.unwrap();
            assert_eq!(
                fixture
                    .data
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|r| r["method"] == "tools/call")
                    .count(),
                17
            );
            assert_eq!(
                fixture
                    .data
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|r| r["method"] == "tools/list")
                    .count(),
                2
            );
            if legacy {
                fixture.data.subscriptions.lock().unwrap()[0]
                    .0
                    .send(
                        Event::default().data(
                            json!({"jsonrpc":"2.0","id":-7,"method":"roots/list"}).to_string(),
                        ),
                    )
                    .unwrap();
                wait(&fixture.data, || {
                    fixture.data.replies.load(Ordering::SeqCst) == 1
                })
                .await;
            }
            remote.close().await.unwrap();
            wait(&fixture.data, || {
                fixture.data.closed.load(Ordering::SeqCst) == 1
            })
            .await;
            assert_eq!(fixture.data.subscriptions.lock().unwrap().len(), 1);
        }
    }
}

struct Provider(Arc<AtomicUsize>);
impl CredentialProvider for Provider {
    fn resolve<'a>(&'a mut self, _: CredentialRequest) -> CredentialFuture<'a> {
        let name = if self.0.load(Ordering::SeqCst) == 0 {
            "alice"
        } else {
            "bob"
        };
        Box::pin(async move { Credential::bearer(name.into(), name.into()) })
    }
}
#[tokio::test]
async fn identity_change_replaces_notification_io_with_frozen_tokens_and_cleans_legacy_sessions() {
    for legacy in [false, true] {
        let fixture = fixture(legacy, false, true, false).await;
        let identity = Arc::new(AtomicUsize::new(0));
        let provider = identity.clone();
        let remote = QueuedRemote::spawn_with(
            fixture.config.clone(),
            QueueOptions {
                http_notifications: true,
                ..Default::default()
            },
            move || Ok(Client::new()?.with_credential_provider(Provider(provider))),
        )
        .await
        .unwrap();
        phase(&remote, NotificationPhase::Listening).await;
        identity.store(1, Ordering::SeqCst);
        remote.tool("echo").await.unwrap();
        wait(&fixture.data, || {
            fixture.data.subscriptions.lock().unwrap().len() == 2
        })
        .await;
        phase(&remote, NotificationPhase::Listening).await;
        wait(&fixture.data, || {
            fixture.data.closed.load(Ordering::SeqCst) == 1
        })
        .await;
        {
            let subscriptions = fixture.data.subscriptions.lock().unwrap();
            assert_eq!(subscriptions[0].2, "Bearer alice");
            assert_eq!(subscriptions[1].2, "Bearer bob");
        }
        remote.close().await.unwrap();
        wait(&fixture.data, || {
            fixture.data.closed.load(Ordering::SeqCst) == 2
        })
        .await;
        if legacy {
            let records = fixture.data.transports.lock().unwrap();
            assert!(records.contains(&(
                "delete".into(),
                "Bearer alice".into(),
                "session-alice".into()
            )));
            assert!(records.contains(&(
                "delete".into(),
                "Bearer bob".into(),
                "session-bob".into()
            )));
        }
    }
}

#[tokio::test]
async fn unsupported_and_unacknowledged_streams_fail_locally_once_and_default_is_off() {
    for (legacy, ack, unsupported, expected) in [
        (true, true, true, "CapabilityUnavailable"),
        (false, false, false, "Timeout"),
    ] {
        let fixture = fixture(legacy, false, ack, unsupported).await;
        let remote = QueuedRemote::spawn_with(
            fixture.config.clone(),
            QueueOptions {
                http_notifications: true,
                ..Default::default()
            },
            || {
                Ok(Client::new()?
                    .with_bearer("alice".into())
                    .with_timeout(Duration::from_millis(100)))
            },
        )
        .await
        .unwrap();
        phase(&remote, NotificationPhase::Failed).await;
        assert_eq!(
            remote
                .notification_status()
                .unwrap()
                .await
                .unwrap()
                .unwrap()
                .error_code
                .as_deref(),
            Some(expected)
        );
        remote
            .execute_argv("echo", &["--message".into(), "after-listen-failure".into()])
            .unwrap()
            .await
            .unwrap();
        assert_eq!(
            fixture
                .data
                .transports
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.0 == "listen")
                .count(),
            1
        );
        remote.close().await.unwrap();
    }
    let fixture = fixture(false, false, true, false).await;
    let remote = QueuedRemote::spawn_with(fixture.config.clone(), QueueOptions::default(), || {
        Ok(Client::new()?.with_bearer("alice".into()))
    })
    .await
    .unwrap();
    assert!(
        remote
            .notification_status()
            .unwrap()
            .await
            .unwrap()
            .is_none()
    );
    assert!(fixture.data.subscriptions.lock().unwrap().is_empty());
    remote.close().await.unwrap();
}

#[tokio::test]
async fn stream_ends_are_observable_without_automatic_reopen_or_business_replay() {
    for legacy in [false, true] {
        let fixture = fixture(legacy, false, true, false).await;
        let remote = QueuedRemote::spawn_with(
            fixture.config.clone(),
            QueueOptions {
                http_notifications: true,
                ..Default::default()
            },
            || Ok(Client::new()?.with_bearer("alice".into())),
        )
        .await
        .unwrap();
        phase(&remote, NotificationPhase::Listening).await;
        let subscriptions = std::mem::take(&mut *fixture.data.subscriptions.lock().unwrap());
        if legacy {
            fixture.data.hold_reply.store(true, Ordering::SeqCst);
            subscriptions[0]
                .0
                .send(
                    Event::default()
                        .data(json!({"jsonrpc":"2.0","id":-7,"method":"roots/list"}).to_string()),
                )
                .unwrap();
        } else {
            let rpc = subscriptions[0].1.unwrap();
            subscriptions[0].0.send(Event::default().data(json!({"jsonrpc":"2.0","id":rpc,"result":{"resultType":"complete","_meta":{"io.modelcontextprotocol/subscriptionId":rpc}}}).to_string())).unwrap();
        }
        drop(subscriptions);
        phase(
            &remote,
            if legacy {
                NotificationPhase::Failed
            } else {
                NotificationPhase::Closed
            },
        )
        .await;
        wait(&fixture.data, || {
            fixture.data.closed.load(Ordering::SeqCst) == 1
        })
        .await;
        if legacy {
            assert_eq!(
                remote
                    .notification_status()
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap()
                    .error_code
                    .as_deref(),
                Some("SubscriptionDisconnected")
            );
        }
        if legacy {
            wait(&fixture.data, || {
                fixture.data.replies.load(Ordering::SeqCst) == 1
            })
            .await;
        }
        tokio::time::timeout(
            Duration::from_millis(250),
            remote
                .execute_argv("echo", &["--message".into(), "after-stream-end".into()])
                .unwrap(),
        )
        .await
        .expect("A pending EOF capability reply blocked business work")
        .unwrap();
        fixture.data.release_reply.notify_one();
        assert_eq!(
            fixture
                .data
                .transports
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.0 == "listen")
                .count(),
            1
        );
        assert_eq!(
            fixture
                .data
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r["method"] == "tools/call")
                .count(),
            1
        );
        remote.close().await.unwrap();
    }
}
