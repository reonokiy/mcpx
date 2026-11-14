use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use futures_util::stream;
use mcpx_core::{
    Credential, CredentialFuture, CredentialProvider, CredentialRequest, ExecutionCertainty,
    NotificationPhase, NotificationStatus,
};
use mcpx_native::{
    Client, NotificationRecoveryPolicy, ProtocolMode, QueueOptions, QueuedRemote, ServerConfig,
};
use serde_json::{Value, json};
use std::{
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, mpsc};

const CURSOR: &str = "游标/é";
struct Listen {
    at: Instant,
    auth: String,
    session: String,
    cursor: Option<String>,
    send: mpsc::UnboundedSender<Option<Vec<u8>>>,
}
struct Data {
    legacy: bool,
    retry: u64,
    flap: AtomicBool,
    resumed_status: AtomicUsize,
    opening_release: Notify,
    listens: Mutex<Vec<Listen>>,
    requests: Mutex<Vec<(Value, String, String)>>,
    replies: AtomicUsize,
    completed_replies: AtomicUsize,
    hold_reply: AtomicBool,
    release_reply: Notify,
    released: AtomicBool,
    release: Notify,
    identity: Mutex<(String, String)>,
    block_provider: AtomicBool,
    provider_entered: Notify,
    provider_release: Notify,
    provider_calls: AtomicUsize,
    provider_dropped: AtomicUsize,
    hold_metadata: AtomicBool,
    release_metadata: Notify,
}
struct Provider(Arc<Data>);
impl Drop for Provider {
    fn drop(&mut self) {
        self.0.provider_dropped.fetch_add(1, Ordering::SeqCst);
    }
}
impl CredentialProvider for Provider {
    fn resolve<'a>(&'a mut self, _: CredentialRequest<'a>) -> CredentialFuture<'a> {
        self.0.provider_calls.fetch_add(1, Ordering::SeqCst);
        let (identity, token) = self.0.identity.lock().unwrap().clone();
        let block = self.0.block_provider.swap(false, Ordering::SeqCst);
        Box::pin(async move {
            if block {
                self.0.provider_entered.notify_one();
                self.0.provider_release.notified().await;
            }
            Credential::bearer(token, identity)
        })
    }
}
fn field(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .map(|v| String::from_utf8(v.as_bytes().to_vec()).unwrap())
        .unwrap_or_default()
}
async fn listen(data: Arc<Data>, headers: HeaderMap, rpc: Option<u64>) -> Response {
    let (send, receive) = mpsc::unbounded_channel();
    let count = {
        let mut records = data.listens.lock().unwrap();
        records.push(Listen {
            at: Instant::now(),
            auth: field(&headers, "authorization"),
            session: field(&headers, "mcp-session-id"),
            cursor: headers
                .get("last-event-id")
                .map(|v| String::from_utf8(v.as_bytes().to_vec()).unwrap()),
            send: send.clone(),
        });
        records.len()
    };
    while count > 1 && data.resumed_status.load(Ordering::SeqCst) == 0 {
        let ready = data.opening_release.notified();
        if data.resumed_status.load(Ordering::SeqCst) != 0 {
            break;
        }
        ready.await;
    }
    let status = data.resumed_status.load(Ordering::SeqCst);
    if count > 1 && status != 200 {
        return StatusCode::from_u16(status as u16).unwrap().into_response();
    }
    if data.legacy {
        send.send(Some(
            format!("id: {CURSOR}\nretry: {}\ndata:\n\n", data.retry).into_bytes(),
        ))
        .unwrap();
    } else {
        send.send(Some(format!("data: {}\n\n",json!({"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":rpc},"notifications":{"toolsListChanged":true}}})).into_bytes())).unwrap();
    }
    if data.flap.load(Ordering::SeqCst) {
        send.send(None).unwrap();
    }
    let body = stream::unfold(receive, |mut receive| async move {
        receive
            .recv()
            .await
            .flatten()
            .map(|bytes| (Ok::<_, Infallible>(Bytes::from(bytes)), receive))
    });
    Response::builder()
        .header("content-type", "text/event-stream")
        .body(Body::from_stream(body))
        .unwrap()
}
async fn get(State(data): State<Arc<Data>>, headers: HeaderMap) -> Response {
    listen(data, headers, None).await
}
async fn rpc(
    State(data): State<Arc<Data>>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Response {
    let auth = field(&headers, "authorization");
    data.requests
        .lock()
        .unwrap()
        .push((request.clone(), auth, field(&headers, "last-event-id")));
    let method = request["method"].as_str().unwrap_or("reply");
    if method == "subscriptions/listen" {
        return listen(data, headers, request["id"].as_u64()).await;
    }
    if method == "reply" {
        assert_eq!(request["id"], -7);
        assert_eq!(request["error"]["code"], -32601);
        data.replies.fetch_add(1, Ordering::SeqCst);
        while data.hold_reply.load(Ordering::SeqCst) {
            let ready = data.release_reply.notified();
            if !data.hold_reply.load(Ordering::SeqCst) {
                break;
            }
            ready.await;
        }
        data.completed_replies.fetch_add(1, Ordering::SeqCst);
        return StatusCode::ACCEPTED.into_response();
    }
    if method.starts_with("notifications/") {
        return StatusCode::ACCEPTED.into_response();
    }
    if method == "server/discover" && data.legacy {
        return (StatusCode::BAD_REQUEST,Json(json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32601,"message":"initialize first"}}))).into_response();
    }
    let result = match method {
        "server/discover" => {
            json!({"supportedVersions":["2026-07-28"],"capabilities":{"tools":{"listChanged":true}}})
        }
        "initialize" => {
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
            json!({"content":[],"structuredContent":request["params"]["arguments"]})
        }
        _ => panic!("unexpected method"),
    };
    let mut response =
        Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result})).into_response();
    if method == "initialize" {
        response
            .headers_mut()
            .insert("mcp-session-id", "session-fixed".parse().unwrap());
    }
    response
}
async fn delete(State(data): State<Arc<Data>>, headers: HeaderMap) -> StatusCode {
    data.requests.lock().unwrap().push((
        json!({"method":"DELETE"}),
        field(&headers, "authorization"),
        field(&headers, "last-event-id"),
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
impl Fixture {
    async fn new(legacy: bool, auto: bool, retry: u64) -> Self {
        let data = Arc::new(Data {
            legacy,
            retry,
            flap: AtomicBool::new(false),
            resumed_status: AtomicUsize::new(200),
            opening_release: Notify::new(),
            listens: Mutex::new(Vec::new()),
            requests: Mutex::new(Vec::new()),
            replies: AtomicUsize::new(0),
            completed_replies: AtomicUsize::new(0),
            hold_reply: AtomicBool::new(false),
            release_reply: Notify::new(),
            released: AtomicBool::new(false),
            release: Notify::new(),
            identity: Mutex::new(("alice".into(), "original".into())),
            block_provider: AtomicBool::new(false),
            provider_entered: Notify::new(),
            provider_release: Notify::new(),
            provider_calls: AtomicUsize::new(0),
            provider_dropped: AtomicUsize::new(0),
            hold_metadata: AtomicBool::new(false),
            release_metadata: Notify::new(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = Router::new()
            .route("/mcp", post(rpc).get(get).delete(delete))
            .with_state(data.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            data,
            config: ServerConfig::Http {
                url: format!("http://{address}/mcp"),
                credential: None,
                protocol: if auto {
                    ProtocolMode::Auto
                } else if legacy {
                    ProtocolMode::Legacy
                } else {
                    ProtocolMode::Modern
                },
            },
            task,
        }
    }
    async fn connect(&self, options: QueueOptions) -> QueuedRemote {
        let provider = Provider(self.data.clone());
        QueuedRemote::spawn_with(self.config.clone(), options, move || {
            Ok(Client::new()?
                .with_credential_provider(provider)
                .with_timeout(Duration::from_secs(2)))
        })
        .await
        .unwrap()
    }
    fn options() -> QueueOptions {
        QueueOptions {
            http_notifications: true,
            ..Default::default()
        }
    }
    fn frame(&self, index: usize, bytes: impl Into<Vec<u8>>) {
        self.data.listens.lock().unwrap()[index]
            .send
            .send(Some(bytes.into()))
            .unwrap();
    }
    fn end(&self, index: usize, partial: bool) {
        if partial {
            self.frame(index, b"id: uncommitted\ndata: {".to_vec());
        }
        self.data.listens.lock().unwrap()[index]
            .send
            .send(None)
            .unwrap();
    }
    fn roots(&self, index: usize) {
        self.frame(
            index,
            b"data: {\"jsonrpc\":\"2.0\",\"id\":-7,\"method\":\"roots/list\"}\n\n".to_vec(),
        );
    }
    fn count(&self) -> usize {
        self.data.listens.lock().unwrap().len()
    }
    fn calls(&self) -> usize {
        self.data
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(r, _, _)| r["method"] == "tools/call")
            .count()
    }
    async fn wait(&self, condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn phase(&self, remote: &QueuedRemote, phase: NotificationPhase) -> NotificationStatus {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(status) = remote.notification_status().unwrap().await.unwrap()
                    && status.phase == phase
                {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap()
    }
}
fn policy(attempts: u8, timeout: Duration) -> NotificationRecoveryPolicy {
    NotificationRecoveryPolicy {
        max_attempts: attempts,
        backoff: Duration::from_millis(10),
        timeout,
    }
}

#[tokio::test]
async fn manual_resume_keeps_all_call_slots_and_waits_for_frozen_capability_replies() {
    for auto in [false, true] {
        let fixture = Fixture::new(true, auto, 40).await;
        let remote = fixture
            .connect(QueueOptions {
                capacity: 16,
                ..Fixture::options()
            })
            .await;
        let first = fixture.phase(&remote, NotificationPhase::Listening).await;
        let mut calls = Vec::new();
        for i in 0..16 {
            calls.push(
                remote
                    .execute("echo", &format!("--message held-{i}"))
                    .unwrap(),
            );
        }
        fixture.wait(|| fixture.calls() == 16).await;
        assert_eq!(
            remote
                .execute("echo", "--message overflow")
                .err()
                .unwrap()
                .code,
            "QueueFull"
        );
        fixture.data.hold_reply.store(true, Ordering::SeqCst);
        fixture.roots(0);
        fixture
            .wait(|| fixture.data.replies.load(Ordering::SeqCst) == 1)
            .await;
        fixture.end(0, true);
        fixture.phase(&remote, NotificationPhase::Failed).await;
        let providers = fixture.data.provider_calls.load(Ordering::SeqCst);
        let resumed = remote.resume_notifications(first.resource_id).unwrap();
        assert_eq!(
            remote
                .resume_notifications(first.resource_id)
                .err()
                .unwrap()
                .code,
            "QueueFull"
        );
        assert_eq!(
            remote
                .notification_status()
                .unwrap()
                .await
                .unwrap()
                .unwrap()
                .phase,
            NotificationPhase::Failed
        );
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(fixture.count(), 1);
        fixture.data.hold_reply.store(false, Ordering::SeqCst);
        fixture.data.release_reply.notify_waiters();
        let next = tokio::time::timeout(Duration::from_secs(1), resumed)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(next.resource_id, first.resource_id);
        fixture.phase(&remote, NotificationPhase::Listening).await;
        assert_eq!(
            fixture.data.provider_calls.load(Ordering::SeqCst),
            providers
        );
        fixture.data.released.store(true, Ordering::SeqCst);
        fixture.data.release.notify_waiters();
        for call in calls {
            call.await.unwrap();
        }
        fixture.data.identity.lock().unwrap().1 = "rotated".into();
        remote
            .execute("echo", "--message rotated")
            .unwrap()
            .await
            .unwrap();
        fixture.roots(1);
        fixture
            .wait(|| fixture.data.completed_replies.load(Ordering::SeqCst) == 2)
            .await;
        fixture.end(1, false);
        let interrupted = fixture.phase(&remote, NotificationPhase::Failed).await;
        remote
            .resume_notifications(interrupted.resource_id)
            .unwrap()
            .await
            .unwrap();
        fixture.phase(&remote, NotificationPhase::Listening).await;
        {
            let records = fixture.data.listens.lock().unwrap();
            assert_eq!(records.len(), 3);
            assert!(
                records
                    .iter()
                    .all(|r| r.auth == "Bearer original" && r.session == "session-fixed")
            );
            assert!(records[0].cursor.is_none());
            assert!(
                records[1..]
                    .iter()
                    .all(|r| r.cursor.as_deref() == Some(CURSOR))
            );
            assert!(records[1].at.duration_since(records[0].at) >= Duration::from_millis(40));
        }
        assert_eq!(fixture.calls(), 17);
        assert!(
            fixture
                .data
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(|(_, _, cursor)| cursor.is_empty())
        );
        assert!(
            fixture
                .data
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|(r, _, _)| r.get("method").is_none())
                .all(|(_, auth, _)| auth == "Bearer original")
        );
        remote.close().await.unwrap();
    }
}

#[tokio::test]
async fn resume_wait_can_expire_cancel_or_be_superseded_without_sending_a_get() {
    let fixture = Fixture::new(true, false, 250).await;
    let remote = fixture.connect(Fixture::options()).await;
    let first = fixture.phase(&remote, NotificationPhase::Listening).await;
    fixture.end(0, false);
    fixture.phase(&remote, NotificationPhase::Failed).await;
    let error = remote
        .resume_notifications_with_timeout(first.resource_id, Duration::from_millis(20))
        .unwrap()
        .await
        .unwrap_err();
    assert_eq!(error.code, "Timeout");
    assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
    let cancelled = remote.resume_notifications(first.resource_id).unwrap();
    cancelled.cancel();
    assert_eq!(cancelled.await.unwrap_err().code, "Cancelled");
    let waiting = remote.resume_notifications(first.resource_id).unwrap();
    remote.stop_notifications().unwrap().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap_err()
            .code,
        "Cancelled"
    );
    tokio::time::sleep(Duration::from_millis(280)).await;
    assert_eq!(fixture.count(), 1);
    assert_eq!(
        remote
            .resume_notifications(first.resource_id)
            .unwrap()
            .await
            .unwrap_err()
            .code,
        "ResumeUnavailable"
    );
    remote.start_notifications().unwrap().await.unwrap();
    fixture.phase(&remote, NotificationPhase::Listening).await;
    assert!(fixture.data.listens.lock().unwrap()[1].cursor.is_none());
    remote.close().await.unwrap();
}

#[tokio::test]
async fn resume_and_capability_writer_progress_while_one_provider_is_pending() {
    let fixture = Fixture::new(true, false, 25).await;
    let remote = fixture.connect(Fixture::options()).await;
    let first = fixture.phase(&remote, NotificationPhase::Listening).await;
    fixture.end(0, false);
    fixture.phase(&remote, NotificationPhase::Failed).await;
    fixture.data.block_provider.store(true, Ordering::SeqCst);
    let call = remote
        .execute("echo", "--message pending-provider")
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        fixture.data.provider_entered.notified(),
    )
    .await
    .unwrap();
    let providers = fixture.data.provider_calls.load(Ordering::SeqCst);
    remote
        .resume_notifications(first.resource_id)
        .unwrap()
        .await
        .unwrap();
    let second = fixture.phase(&remote, NotificationPhase::Listening).await;
    fixture.roots(1);
    fixture
        .wait(|| fixture.data.completed_replies.load(Ordering::SeqCst) == 1)
        .await;
    fixture.end(1, false);
    fixture.phase(&remote, NotificationPhase::Failed).await;
    remote
        .resume_notifications(second.resource_id)
        .unwrap()
        .await
        .unwrap();
    fixture.phase(&remote, NotificationPhase::Listening).await;
    assert_eq!(
        fixture.data.provider_calls.load(Ordering::SeqCst),
        providers
    );
    assert_eq!(fixture.calls(), 0);
    fixture.data.provider_release.notify_one();
    call.await.unwrap();
    assert_eq!(fixture.calls(), 1);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn automatic_flaps_spend_one_burst_budget_and_manual_resume_can_override_exhaustion() {
    let fixture = Fixture::new(true, true, 5).await;
    fixture.data.flap.store(true, Ordering::SeqCst);
    let remote = fixture
        .connect(QueueOptions {
            notification_recovery: Some(policy(2, Duration::from_secs(1))),
            ..Fixture::options()
        })
        .await;
    fixture.wait(|| fixture.count() == 3).await;
    let last = fixture.phase(&remote, NotificationPhase::Failed).await;
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(fixture.count(), 3);
    fixture.data.flap.store(false, Ordering::SeqCst);
    remote
        .resume_notifications(last.resource_id)
        .unwrap()
        .await
        .unwrap();
    fixture.phase(&remote, NotificationPhase::Listening).await;
    assert_eq!(fixture.count(), 4);
    assert!(
        fixture.data.listens.lock().unwrap()[1..]
            .iter()
            .all(|r| r.cursor.as_deref() == Some(CURSOR))
    );
    remote.close().await.unwrap();
}

#[tokio::test]
async fn automatic_recovery_respects_total_window_and_resets_after_a_healthy_interval() {
    let fixture = Fixture::new(true, false, 250).await;
    let remote = fixture
        .connect(QueueOptions {
            notification_recovery: Some(policy(8, Duration::from_millis(50))),
            ..Fixture::options()
        })
        .await;
    fixture.phase(&remote, NotificationPhase::Listening).await;
    fixture.end(0, false);
    fixture.phase(&remote, NotificationPhase::Failed).await;
    tokio::time::sleep(Duration::from_millis(90)).await;
    assert_eq!(fixture.count(), 1);
    remote.close().await.unwrap();
    let fixture = Fixture::new(true, false, 0).await;
    let remote = fixture
        .connect(QueueOptions {
            notification_recovery: Some(policy(1, Duration::from_millis(100))),
            ..Fixture::options()
        })
        .await;
    fixture.phase(&remote, NotificationPhase::Listening).await;
    fixture.end(0, false);
    fixture.wait(|| fixture.count() == 2).await;
    fixture.phase(&remote, NotificationPhase::Listening).await;
    tokio::time::sleep(Duration::from_millis(130)).await;
    fixture.end(1, false);
    fixture.wait(|| fixture.count() == 3).await;
    fixture.phase(&remote, NotificationPhase::Listening).await;
    remote.close().await.unwrap();
}

#[tokio::test]
async fn automatic_recovery_stops_on_auth_session_cursor_and_protocol_boundaries() {
    for mode in [
        "modern",
        "cursor",
        "malformed",
        "auth",
        "session",
        "unsupported",
        "reply",
    ] {
        let fixture = Fixture::new(mode != "modern", false, 0).await;
        let remote = fixture
            .connect(QueueOptions {
                notification_recovery: Some(policy(8, Duration::from_secs(1))),
                ..Fixture::options()
            })
            .await;
        fixture.phase(&remote, NotificationPhase::Listening).await;
        match mode {
            "cursor" => fixture.frame(0, b"id:\ndata:\n\n".to_vec()),
            "malformed" => fixture.frame(0, b"data: nope\n\n".to_vec()),
            "auth" => fixture.data.resumed_status.store(401, Ordering::SeqCst),
            "session" => fixture.data.resumed_status.store(404, Ordering::SeqCst),
            "unsupported" => fixture.data.resumed_status.store(405, Ordering::SeqCst),
            "reply" => {
                fixture.data.hold_reply.store(true, Ordering::SeqCst);
                fixture.roots(0);
                fixture
                    .wait(|| fixture.data.replies.load(Ordering::SeqCst) == 1)
                    .await;
            }
            _ => {}
        }
        if mode != "malformed" {
            fixture.end(0, false);
        }
        let expected = if matches!(mode, "auth" | "session" | "unsupported") {
            2
        } else {
            1
        };
        fixture.wait(|| fixture.count() == expected).await;
        fixture.phase(&remote, NotificationPhase::Failed).await;
        tokio::time::sleep(Duration::from_millis(if mode == "reply" {
            550
        } else {
            50
        }))
        .await;
        assert_eq!(fixture.count(), expected, "{mode}");
        if mode == "reply" {
            let id = remote
                .notification_status()
                .unwrap()
                .await
                .unwrap()
                .unwrap()
                .resource_id;
            assert_eq!(
                remote
                    .resume_notifications(id)
                    .unwrap()
                    .await
                    .unwrap_err()
                    .code,
                "ResumeUnavailable"
            );
            fixture.data.hold_reply.store(false, Ordering::SeqCst);
            fixture.data.release_reply.notify_waiters();
        }
        remote.close().await.unwrap();
    }
}

#[tokio::test]
async fn serial_metadata_wait_cannot_reset_a_flapping_streams_budget() {
    for after_resume in [false, true] {
        let fixture = Fixture::new(true, false, 0).await;
        let remote = fixture
            .connect(QueueOptions {
                notification_recovery: Some(policy(1, Duration::from_millis(100))),
                ..Fixture::options()
            })
            .await;
        fixture.phase(&remote, NotificationPhase::Listening).await;
        if after_resume {
            fixture.end(0, false);
            fixture.wait(|| fixture.count() == 2).await;
            fixture.phase(&remote, NotificationPhase::Listening).await;
        }
        fixture.data.hold_metadata.store(true, Ordering::SeqCst);
        let refresh = remote.refresh().unwrap();
        fixture
            .wait(|| {
                fixture
                    .data
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(r, _, _)| r["method"] == "tools/list")
                    .count()
                    == 2
            })
            .await;
        fixture.end(usize::from(after_resume), false);
        tokio::time::sleep(Duration::from_millis(140)).await;
        fixture.data.hold_metadata.store(false, Ordering::SeqCst);
        fixture.data.release_metadata.notify_waiters();
        refresh.await.unwrap();
        fixture.phase(&remote, NotificationPhase::Failed).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(fixture.count(), 1 + usize::from(after_resume));
        remote.close().await.unwrap();
    }
}

#[tokio::test]
async fn automatic_retry_timer_and_replies_progress_during_a_provider_wait() {
    let fixture = Fixture::new(true, false, 30).await;
    let remote = fixture
        .connect(QueueOptions {
            notification_recovery: Some(policy(2, Duration::from_secs(1))),
            ..Fixture::options()
        })
        .await;
    fixture.phase(&remote, NotificationPhase::Listening).await;
    fixture.data.block_provider.store(true, Ordering::SeqCst);
    let call = remote.execute("echo", "--message waiting").unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        fixture.data.provider_entered.notified(),
    )
    .await
    .unwrap();
    let providers = fixture.data.provider_calls.load(Ordering::SeqCst);
    fixture.end(0, false);
    fixture.wait(|| fixture.count() == 2).await;
    fixture.phase(&remote, NotificationPhase::Listening).await;
    fixture.roots(1);
    fixture
        .wait(|| fixture.data.completed_replies.load(Ordering::SeqCst) == 1)
        .await;
    assert_eq!(
        fixture.data.provider_calls.load(Ordering::SeqCst),
        providers
    );
    assert_eq!(fixture.calls(), 0);
    fixture.data.provider_release.notify_one();
    call.await.unwrap();
    assert_eq!(fixture.calls(), 1);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn automatic_opening_timeout_is_tightened_to_the_recovery_window() {
    let fixture = Fixture::new(true, false, 0).await;
    let remote = fixture
        .connect(QueueOptions {
            notification_recovery: Some(policy(8, Duration::from_millis(80))),
            ..Fixture::options()
        })
        .await;
    fixture.phase(&remote, NotificationPhase::Listening).await;
    fixture.data.resumed_status.store(0, Ordering::SeqCst);
    let started = Instant::now();
    fixture.end(0, false);
    fixture.wait(|| fixture.count() == 2).await;
    let status = fixture.phase(&remote, NotificationPhase::Failed).await;
    assert_eq!(status.error_code.as_deref(), Some("Timeout"));
    assert!(started.elapsed() < Duration::from_secs(1));
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(fixture.count(), 2);
    fixture.data.resumed_status.store(200, Ordering::SeqCst);
    fixture.data.opening_release.notify_waiters();
    remote
        .resume_notifications(status.resource_id)
        .unwrap()
        .await
        .unwrap();
    fixture.phase(&remote, NotificationPhase::Listening).await;
    assert_eq!(fixture.count(), 3);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn resumption_is_opt_in_and_a_policy_alone_does_not_open_a_listener() {
    for listening in [false, true] {
        let fixture = Fixture::new(true, false, 0).await;
        let remote = fixture
            .connect(QueueOptions {
                http_notifications: listening,
                notification_recovery: (!listening).then(|| policy(2, Duration::from_secs(1))),
                ..Default::default()
            })
            .await;
        if listening {
            fixture.phase(&remote, NotificationPhase::Listening).await;
            fixture.end(0, false);
            fixture.phase(&remote, NotificationPhase::Failed).await;
        }
        remote
            .execute("echo", "--message independent")
            .unwrap()
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(fixture.count(), usize::from(listening));
        remote.close().await.unwrap();
    }
}

#[tokio::test]
async fn resume_lifecycle_settles_once_and_retained_results_do_not_keep_the_provider_alive() {
    let fixture = Fixture::new(true, false, 100).await;
    let provider = Provider(fixture.data.clone());
    let (remote, mut events) = QueuedRemote::spawn_with_events(
        fixture.config.clone(),
        Fixture::options(),
        mcpx_core::EventLimits::default(),
        move || Ok(Client::new()?.with_credential_provider(provider)),
    )
    .await
    .unwrap();
    let first = fixture.phase(&remote, NotificationPhase::Listening).await;
    fixture.end(0, false);
    fixture.phase(&remote, NotificationPhase::Failed).await;
    let expired = remote
        .resume_notifications_with_timeout(first.resource_id, Duration::from_millis(10))
        .unwrap();
    let expired_id = expired.id();
    assert_eq!(expired.await.unwrap_err().code, "Timeout");
    let cancelled = remote.resume_notifications(first.resource_id).unwrap();
    let cancelled_id = cancelled.id();
    cancelled.cancel();
    assert_eq!(cancelled.await.unwrap_err().code, "Cancelled");
    let mut completed = Box::pin(remote.resume_notifications(first.resource_id).unwrap());
    let completed_id = completed.id();
    let result = completed.as_mut().await.unwrap();
    fixture.phase(&remote, NotificationPhase::Listening).await;
    fixture.end(1, false);
    fixture.phase(&remote, NotificationPhase::Failed).await;
    let closing = remote.resume_notifications(result.resource_id).unwrap();
    let closing_id = closing.id();
    remote.close().await.unwrap();
    assert_eq!(closing.await.unwrap_err().code, "TransportClosed");
    assert_eq!(fixture.data.provider_dropped.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.count(), 2);
    let mut records = Vec::new();
    while let Some(batch) = events.next_batch().await {
        records.extend(batch.events);
    }
    for id in [expired_id, cancelled_id, completed_id, closing_id] {
        let terminals: Vec<_> = records
            .iter()
            .filter(|e| {
                e.queue_operation_id == id
                    && !matches!(
                        e.kind,
                        mcpx_core::OperationEventKind::Queued
                            | mcpx_core::OperationEventKind::Started
                    )
            })
            .collect();
        assert_eq!(terminals.len(), 1);
    }
    assert_eq!(
        records
            .iter()
            .filter(|e| e.queue_operation_id == completed_id
                && e.kind == mcpx_core::OperationEventKind::Started)
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|e| e.queue_operation_id == expired_id
                && e.kind == mcpx_core::OperationEventKind::Started)
            .count(),
        0
    );
    assert_eq!(result.phase, NotificationPhase::Opening);
    drop(completed);
}

#[tokio::test]
async fn pending_resume_cannot_cross_identity_or_explicit_reconnect() {
    for identity in [false, true] {
        let fixture = Fixture::new(true, false, 300).await;
        let remote = fixture.connect(Fixture::options()).await;
        let first = fixture.phase(&remote, NotificationPhase::Listening).await;
        fixture.end(0, false);
        fixture.phase(&remote, NotificationPhase::Failed).await;
        let resume = remote.resume_notifications(first.resource_id).unwrap();
        if identity {
            *fixture.data.identity.lock().unwrap() = ("bob".into(), "bob-token".into());
            remote
                .execute("echo", "--message new-identity")
                .unwrap()
                .await
                .unwrap();
        } else {
            remote.reconnect().unwrap().await.unwrap();
        }
        let error = tokio::time::timeout(Duration::from_secs(1), resume)
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(
            error.code.as_str(),
            "ResumeUnavailable" | "InvalidHandle"
        ));
        fixture.phase(&remote, NotificationPhase::Listening).await;
        assert_eq!(fixture.count(), 2);
        assert!(fixture.data.listens.lock().unwrap()[1].cursor.is_none());
        if identity {
            assert_eq!(
                fixture.data.listens.lock().unwrap()[1].auth,
                "Bearer bob-token"
            );
        }
        remote.close().await.unwrap();
    }
}

#[tokio::test]
async fn invalid_automatic_recovery_limits_fail_before_connecting() {
    let fixture = Fixture::new(true, false, 0).await;
    let valid = policy(2, Duration::from_secs(1));
    for policy in [
        NotificationRecoveryPolicy {
            max_attempts: 0,
            ..valid
        },
        NotificationRecoveryPolicy {
            max_attempts: 9,
            ..valid
        },
        NotificationRecoveryPolicy {
            timeout: Duration::ZERO,
            ..valid
        },
        NotificationRecoveryPolicy {
            timeout: Duration::from_secs(601),
            ..valid
        },
        NotificationRecoveryPolicy {
            backoff: Duration::from_secs(61),
            ..valid
        },
    ] {
        assert_eq!(
            QueuedRemote::connect(
                fixture.config.clone(),
                QueueOptions {
                    notification_recovery: Some(policy),
                    ..Fixture::options()
                }
            )
            .await
            .err()
            .unwrap()
            .code,
            "ConfigurationError"
        );
    }
    assert_eq!(fixture.count(), 0);
    assert!(fixture.data.requests.lock().unwrap().is_empty());
}
