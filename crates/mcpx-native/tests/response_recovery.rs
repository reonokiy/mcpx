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
    CallOutcome, Credential, CredentialFuture, CredentialProvider, CredentialRequest,
    ExecutionCertainty,
};
use mcpx_native::{
    Client, ProtocolMode, QueueOptions, QueuedRemote, ResponseRecoveryPolicy, ServerConfig,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, mpsc};

struct Record {
    rpc: u64,
    message: String,
    auth: String,
    session: String,
}
struct Get {
    rpc: u64,
    at: Instant,
    auth: String,
    session: String,
    cursor: String,
}
struct Data {
    legacy: bool,
    retry: u64,
    mode: AtomicUsize,
    caps: AtomicBool,
    json: AtomicBool,
    no_cursor: AtomicBool,
    bad: AtomicBool,
    calls: Mutex<Vec<Record>>,
    gets: Mutex<Vec<Get>>,
    replies: Mutex<Vec<(u64, String, String)>>,
    acks: Mutex<Vec<(u64, Instant)>>,
    cancelled: Mutex<Vec<(u64, String, bool)>>,
    deleted: Mutex<Vec<String>>,
    initialized: AtomicUsize,
    released: AtomicBool,
    release: Notify,
    reply_delay: AtomicUsize,
    final_reply_delay: AtomicUsize,
    reply_status: AtomicUsize,
    streams: Mutex<BTreeMap<u64, mpsc::UnboundedSender<Option<Vec<u8>>>>>,
    live: AtomicUsize,
    second: AtomicBool,
    provider_calls: AtomicUsize,
    provider_drops: AtomicUsize,
    block: AtomicBool,
    entered: Notify,
    unblock: Notify,
}
struct Provider(Arc<Data>);
impl Drop for Provider {
    fn drop(&mut self) {
        self.0.provider_drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl CredentialProvider for Provider {
    fn resolve<'a>(&'a mut self, _: CredentialRequest<'a>) -> CredentialFuture<'a> {
        self.0.provider_calls.fetch_add(1, Ordering::SeqCst);
        let block = self.0.block.swap(false, Ordering::SeqCst);
        Box::pin(async move {
            if block {
                self.0.entered.notify_one();
                self.0.unblock.notified().await;
            }
            Credential::bearer(
                if self.0.second.load(Ordering::SeqCst) {
                    "second"
                } else {
                    "first"
                }
                .into(),
                "same-identity".into(),
            )
        })
    }
}
struct Live(Arc<Data>);
impl Drop for Live {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
    }
}
fn field(h: &HeaderMap, n: &str) -> String {
    h.get(n)
        .map(|v| String::from_utf8(v.as_bytes().to_vec()).unwrap())
        .unwrap_or_default()
}
fn frame(v: Value) -> Vec<u8> {
    format!("data: {v}\n\n").into_bytes()
}
fn completed(rpc: u64, message: &str) -> Vec<u8> {
    frame(
        json!({"jsonrpc":"2.0","id":rpc,"result":{"content":[],"structuredContent":{"message":message,"value":u64::MAX}}}),
    )
}
async fn sse(data: Arc<Data>, rpc: u64, get: usize) -> Response {
    data.live.fetch_add(1, Ordering::SeqCst);
    let live = Live(data.clone());
    let (tx, rx) = mpsc::unbounded_channel();
    if !data.no_cursor.load(Ordering::SeqCst) {
        tx.send(Some(
            format!("id: {rpc}/游标/{get}\nretry: {}\ndata:\n\n", data.retry).into_bytes(),
        ))
        .unwrap();
    }
    let mode = data.mode.load(Ordering::SeqCst);
    let capability = (data.caps.load(Ordering::SeqCst) && data.legacy).then(|| frame(json!({"jsonrpc":"2.0","id":format!("cap-{rpc}-{get}"),"method":"roots/list","params":{}})));
    if let Some(bytes) = &capability
        && !(get > 0 && mode == 0)
    {
        tx.send(Some(bytes.clone())).unwrap();
    }
    if data.bad.load(Ordering::SeqCst) {
        tx.send(Some(b"data: {bad}\n\n".to_vec())).unwrap();
        tx.send(None).unwrap();
    } else if get == 0 || mode == 1 {
        tx.send(None).unwrap();
    } else if mode == 0 {
        let message = data
            .calls
            .lock()
            .unwrap()
            .iter()
            .find(|c| c.rpc == rpc)
            .unwrap()
            .message
            .clone();
        // One chunk fixes the final-result-before-reply-ACK ordering in tests.
        let mut bytes = capability.unwrap_or_default();
        bytes.extend(completed(rpc, &message));
        tx.send(Some(bytes)).unwrap();
        tx.send(None).unwrap();
    }
    data.streams.lock().unwrap().insert(rpc, tx);
    let body = Body::from_stream(stream::unfold((rx, live), |(mut rx, live)| async move {
        match rx.recv().await {
            Some(Some(bytes)) => Some((Ok::<_, Infallible>(Bytes::from(bytes)), (rx, live))),
            _ => None,
        }
    }));
    Response::builder()
        .header("content-type", "text/event-stream")
        .body(body)
        .unwrap()
}
async fn handler(
    State(data): State<Arc<Data>>,
    headers: HeaderMap,
    Json(rpc): Json<Value>,
) -> Response {
    assert!(
        headers.get("last-event-id").is_none(),
        "cursor leaked onto POST"
    );
    if rpc.get("method").is_none() {
        let id = rpc["id"].as_str().unwrap();
        let original = id
            .strip_prefix("cap-")
            .unwrap()
            .split('-')
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert_eq!(rpc["error"]["code"], -32601);
        data.replies.lock().unwrap().push((
            original,
            field(&headers, "authorization"),
            field(&headers, "mcp-session-id"),
        ));
        let final_delay = data.final_reply_delay.load(Ordering::SeqCst);
        let delay = if !id.ends_with("-0") && final_delay > 0 {
            final_delay
        } else {
            data.reply_delay.load(Ordering::SeqCst)
        };
        tokio::time::sleep(Duration::from_millis(delay as u64)).await;
        data.acks.lock().unwrap().push((original, Instant::now()));
        return StatusCode::from_u16(data.reply_status.load(Ordering::SeqCst) as u16)
            .unwrap()
            .into_response();
    }
    let id = rpc["id"].as_u64().unwrap_or(0);
    let method = rpc["method"].as_str().unwrap();
    if method == "notifications/cancelled" {
        data.cancelled.lock().unwrap().push((
            rpc["params"]["requestId"].as_u64().unwrap(),
            field(&headers, "authorization"),
            headers.contains_key("last-event-id"),
        ));
        return StatusCode::ACCEPTED.into_response();
    }
    if method == "notifications/initialized" {
        return StatusCode::ACCEPTED.into_response();
    }
    if method == "server/discover" && data.legacy {
        return (StatusCode::BAD_REQUEST,Json(json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"initialize first"}}))).into_response();
    }
    if method == "tools/call" {
        let message = rpc["params"]["arguments"]["message"]
            .as_str()
            .unwrap()
            .to_string();
        data.calls.lock().unwrap().push(Record {
            rpc: id,
            message: message.clone(),
            auth: field(&headers, "authorization"),
            session: field(&headers, "mcp-session-id"),
        });
        while !data.released.load(Ordering::SeqCst) {
            let wake = data.release.notified();
            if data.released.load(Ordering::SeqCst) {
                break;
            }
            wake.await;
        }
        if data.json.load(Ordering::SeqCst) {
            return Json(json!({"jsonrpc":"2.0","id":id,"result":{"content":[],"structuredContent":{"message":message}}})).into_response();
        }
        return sse(data, id, 0).await;
    }
    let result = match method {
        "initialize" => {
            data.initialized.fetch_add(1, Ordering::SeqCst);
            json!({"protocolVersion":"2025-11-25"})
        }
        "server/discover" => json!({"supportedVersions":["2026-07-28"]}),
        "tools/list" => {
            json!({"tools":[{"name":"echo","inputSchema":{"type":"object","properties":{"message":{"type":"string"}},"required":["message"]}}]})
        }
        _ => panic!("unexpected method {method}"),
    };
    let mut response = Json(json!({"jsonrpc":"2.0","id":id,"result":result})).into_response();
    if method == "initialize" {
        response
            .headers_mut()
            .insert("mcp-session-id", "response-session".parse().unwrap());
    }
    response
}
async fn get(State(data): State<Arc<Data>>, headers: HeaderMap) -> Response {
    let cursor = field(&headers, "last-event-id");
    let rpc = cursor.split('/').next().unwrap().parse().unwrap();
    let n = {
        let mut gets = data.gets.lock().unwrap();
        let n = gets.iter().filter(|g| g.rpc == rpc).count() + 1;
        gets.push(Get {
            rpc,
            at: Instant::now(),
            auth: field(&headers, "authorization"),
            session: field(&headers, "mcp-session-id"),
            cursor,
        });
        n
    };
    let mode = data.mode.load(Ordering::SeqCst);
    if mode >= 400 {
        return StatusCode::from_u16(mode as u16).unwrap().into_response();
    }
    if mode == 3 {
        data.live.fetch_add(1, Ordering::SeqCst);
        let _live = Live(data.clone());
        std::future::pending::<()>().await;
        unreachable!();
    }
    sse(data, rpc, n).await
}
async fn delete(State(data): State<Arc<Data>>, headers: HeaderMap) -> Response {
    data.deleted
        .lock()
        .unwrap()
        .push(field(&headers, "authorization"));
    StatusCode::NO_CONTENT.into_response()
}
struct Fixture {
    data: Arc<Data>,
    config: ServerConfig,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn new(legacy: bool, auto: bool, retry: u64) -> Self {
        let data = Arc::new(Data {
            legacy,
            retry,
            mode: AtomicUsize::new(0),
            caps: AtomicBool::new(false),
            json: AtomicBool::new(false),
            no_cursor: AtomicBool::new(false),
            bad: AtomicBool::new(false),
            calls: Mutex::new(vec![]),
            gets: Mutex::new(vec![]),
            replies: Mutex::new(vec![]),
            acks: Mutex::new(vec![]),
            cancelled: Mutex::new(vec![]),
            deleted: Mutex::new(vec![]),
            initialized: AtomicUsize::new(0),
            released: AtomicBool::new(true),
            release: Notify::new(),
            reply_delay: AtomicUsize::new(0),
            final_reply_delay: AtomicUsize::new(0),
            reply_status: AtomicUsize::new(202),
            streams: Mutex::new(BTreeMap::new()),
            live: AtomicUsize::new(0),
            second: AtomicBool::new(false),
            provider_calls: AtomicUsize::new(0),
            provider_drops: AtomicUsize::new(0),
            block: AtomicBool::new(false),
            entered: Notify::new(),
            unblock: Notify::new(),
        });
        let router = Router::new()
            .route("/mcp", post(handler).get(get).delete(delete))
            .with_state(data.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            data,
            config: ServerConfig::Http {
                url: endpoint,
                protocol: if auto {
                    ProtocolMode::Auto
                } else if legacy {
                    ProtocolMode::Legacy
                } else {
                    ProtocolMode::Modern
                },
                credential: None,
            },
            server,
        }
    }
    fn policy() -> ResponseRecoveryPolicy {
        ResponseRecoveryPolicy {
            max_attempts: 2,
            backoff: Duration::from_millis(10),
            timeout: Duration::from_secs(2),
        }
    }
    async fn queued(
        &self,
        policy: Option<ResponseRecoveryPolicy>,
        capacity: usize,
    ) -> QueuedRemote {
        let data = self.data.clone();
        QueuedRemote::spawn_with(
            self.config.clone(),
            QueueOptions {
                response_recovery: policy,
                capacity,
                timeout: Duration::from_secs(5),
                ..Default::default()
            },
            move || {
                Ok(Client::new()?
                    .with_timeout(Duration::from_secs(5))
                    .with_credential_provider(Provider(data)))
            },
        )
        .await
        .unwrap()
    }
    async fn direct(
        &self,
        policy: Option<ResponseRecoveryPolicy>,
        timeout: Duration,
    ) -> mcpx_native::Remote {
        let mut client = Client::new()
            .unwrap()
            .with_timeout(timeout)
            .with_credential_provider(Provider(self.data.clone()));
        if let Some(policy) = policy {
            client = client.with_response_recovery(policy).unwrap();
        }
        self.config.connect(client, None).await.unwrap()
    }
    fn release(&self) {
        self.data.released.store(true, Ordering::SeqCst);
        self.data.release.notify_waiters();
    }
    async fn until(&self, predicate: impl Fn(&Data) -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !predicate(&self.data) {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn closed(&self) {
        self.until(|d| d.live.load(Ordering::SeqCst) == 0).await;
    }
}
fn check(outcome: CallOutcome, message: &str) {
    let CallOutcome::Completed(response) = outcome else {
        panic!()
    };
    assert_eq!(response.raw["structuredContent"]["message"], message);
    assert_eq!(
        response.raw["structuredContent"]["value"].as_u64(),
        Some(u64::MAX)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutable_client_recovers_explicit_and_auto_with_original_ids_headers_and_replies() {
    for auto in [false, true] {
        let f = Fixture::new(true, auto, 25).await;
        f.data.caps.store(true, Ordering::SeqCst);
        f.data.reply_delay.store(30, Ordering::SeqCst);
        let mut remote = f
            .direct(Some(Fixture::policy()), Duration::from_secs(5))
            .await;
        let binding = remote.fresh_tool("echo").await.unwrap();
        let before = f.data.provider_calls.load(Ordering::SeqCst);
        check(
            remote
                .call_json(&binding, json!({"message":"direct"}))
                .await
                .unwrap(),
            "direct",
        );
        assert_eq!(f.data.provider_calls.load(Ordering::SeqCst), before + 1);
        assert_eq!(f.data.calls.lock().unwrap().len(), 1);
        assert_eq!(f.data.calls.lock().unwrap()[0].session, "response-session");
        {
            let gets = f.data.gets.lock().unwrap();
            assert_eq!(gets.len(), 1);
            assert_eq!(gets[0].cursor, format!("{}/游标/0", gets[0].rpc));
            assert_eq!(gets[0].auth, "Bearer first");
            assert_eq!(gets[0].session, "response-session");
            assert!(gets[0].at >= f.data.acks.lock().unwrap()[0].1);
        }
        assert_eq!(f.data.replies.lock().unwrap().len(), 2);
        remote.close().await.unwrap();
        f.closed().await;
        assert_eq!(f.data.initialized.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn sixteen_queued_calls_recover_in_parallel_without_releasing_business_credits() {
    let f = Fixture::new(true, false, 5).await;
    f.data.caps.store(true, Ordering::SeqCst);
    f.data.reply_delay.store(60, Ordering::SeqCst);
    f.data.released.store(false, Ordering::SeqCst);
    let remote = f.queued(Some(Fixture::policy()), 16).await;
    let binding = remote.tool("echo").await.unwrap();
    let mut calls = Vec::new();
    for n in 0..16 {
        calls.push(
            binding
                .submit_json(json!({"message":format!("parallel-{n}")}))
                .unwrap(),
        );
    }
    f.until(|d| d.calls.lock().unwrap().len() == 16).await;
    assert_eq!(
        binding
            .submit_json(json!({"message":"overflow"}))
            .err()
            .unwrap()
            .code,
        "QueueFull"
    );
    f.release();
    for (n, call) in calls.into_iter().enumerate() {
        check(call.await.unwrap(), &format!("parallel-{n}"));
    }
    assert_eq!(f.data.calls.lock().unwrap().len(), 16);
    assert_eq!(f.data.gets.lock().unwrap().len(), 16);
    assert_eq!(f.data.replies.lock().unwrap().len(), 32);
    remote.close().await.unwrap();
    f.closed().await;
}
#[tokio::test]
async fn pending_provider_keeps_response_readers_writers_and_get_timers_running_on_frozen_credentials()
 {
    let f = Fixture::new(true, false, 10).await;
    f.data.caps.store(true, Ordering::SeqCst);
    f.data.reply_delay.store(40, Ordering::SeqCst);
    f.data.released.store(false, Ordering::SeqCst);
    let remote = f.queued(Some(Fixture::policy()), 20).await;
    let binding = remote.tool("echo").await.unwrap();
    let call = binding.submit_json(json!({"message":"frozen"})).unwrap();
    f.until(|d| d.calls.lock().unwrap().len() == 1).await;
    f.data.second.store(true, Ordering::SeqCst);
    f.data.block.store(true, Ordering::SeqCst);
    let peer = binding
        .submit_json(json!({"message":"blocked-provider"}))
        .unwrap();
    f.data.entered.notified().await;
    let before = f.data.provider_calls.load(Ordering::SeqCst);
    f.release();
    check(
        tokio::time::timeout(Duration::from_secs(1), call)
            .await
            .unwrap()
            .unwrap(),
        "frozen",
    );
    assert_eq!(f.data.provider_calls.load(Ordering::SeqCst), before);
    assert!(
        f.data
            .replies
            .lock()
            .unwrap()
            .iter()
            .all(|(_, auth, session)| auth == "Bearer first" && session == "response-session")
    );
    assert_eq!(f.data.gets.lock().unwrap()[0].auth, "Bearer first");
    peer.cancellation_handle().cancel();
    assert_eq!(peer.await.unwrap_err().code, "Cancelled");
    f.data.unblock.notify_waiters();
    let call = binding.submit_json(json!({"message":"new-token"})).unwrap();
    check(call.await.unwrap(), "new-token");
    assert_eq!(f.data.calls.lock().unwrap()[1].auth, "Bearer second");
    remote.close().await.unwrap();
    f.closed().await;
}
#[tokio::test]
async fn eof_flaps_spend_one_attempt_budget_without_reinitializing_or_replaying_the_post() {
    for queued in [false, true] {
        let f = Fixture::new(true, false, 0).await;
        f.data.mode.store(1, Ordering::SeqCst);
        let error = if queued {
            let r = f.queued(Some(Fixture::policy()), 16).await;
            let b = r.tool("echo").await.unwrap();
            let e = b
                .submit_json(json!({"message":"flap"}))
                .unwrap()
                .await
                .unwrap_err();
            r.close().await.unwrap();
            e
        } else {
            let mut r = f
                .direct(Some(Fixture::policy()), Duration::from_secs(5))
                .await;
            let b = r.fresh_tool("echo").await.unwrap();
            let e = r
                .call_json(&b, json!({"message":"flap"}))
                .await
                .unwrap_err();
            r.close().await.unwrap();
            e
        };
        assert_eq!(error.code, "ResponseRecoveryExhausted");
        assert_eq!(error.execution_certainty, ExecutionCertainty::Unknown);
        assert_eq!(f.data.gets.lock().unwrap().len(), 2);
        assert_eq!(f.data.calls.lock().unwrap().len(), 1);
        assert_eq!(f.data.initialized.load(Ordering::SeqCst), 1);
        f.closed().await;
    }
}
#[tokio::test]
async fn server_retry_and_reply_drain_spend_the_same_bounded_window() {
    for slow_reply in [false, true] {
        let f = Fixture::new(true, false, if slow_reply { 0 } else { 300 }).await;
        if slow_reply {
            f.data.caps.store(true, Ordering::SeqCst);
            f.data.reply_delay.store(200, Ordering::SeqCst);
        }
        let policy = ResponseRecoveryPolicy {
            timeout: Duration::from_millis(80),
            ..Fixture::policy()
        };
        let r = f.queued(Some(policy), 16).await;
        let b = r.tool("echo").await.unwrap();
        let error = tokio::time::timeout(
            Duration::from_millis(600),
            b.submit_json(json!({"message":"window"})).unwrap(),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.code, "Timeout");
        assert_eq!(error.execution_certainty, ExecutionCertainty::Unknown);
        assert_eq!(f.data.gets.lock().unwrap().len(), 0);
        assert_eq!(f.data.calls.lock().unwrap().len(), 1);
        r.close().await.unwrap();
        f.closed().await;
    }
}
#[tokio::test]
async fn opening_and_reading_after_resume_are_inside_the_original_recovery_window() {
    for mode in [2, 3] {
        let f = Fixture::new(true, false, 0).await;
        f.data.mode.store(mode, Ordering::SeqCst);
        let r = f
            .queued(
                Some(ResponseRecoveryPolicy {
                    timeout: Duration::from_millis(100),
                    ..Fixture::policy()
                }),
                16,
            )
            .await;
        let b = r.tool("echo").await.unwrap();
        let error = tokio::time::timeout(
            Duration::from_millis(800),
            b.submit_json(json!({"message":"hang"})).unwrap(),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.code, "Timeout");
        assert_eq!(error.execution_certainty, ExecutionCertainty::Unknown);
        assert_eq!(f.data.gets.lock().unwrap().len(), 1);
        r.close().await.unwrap();
        f.closed().await;
    }
}
#[tokio::test]
async fn disabled_modern_json_bad_frames_and_missing_cursors_never_open_a_get() {
    for mode in 0..5 {
        let f = Fixture::new(mode != 1, false, 0).await;
        if mode == 2 {
            f.data.json.store(true, Ordering::SeqCst);
        }
        if mode == 3 {
            f.data.bad.store(true, Ordering::SeqCst);
        }
        if mode == 4 {
            f.data.no_cursor.store(true, Ordering::SeqCst);
        }
        let r = f.queued((mode != 0).then_some(Fixture::policy()), 16).await;
        let b = r.tool("echo").await.unwrap();
        let result = b
            .submit_json(json!({"message":"local-boundary"}))
            .unwrap()
            .await;
        assert_eq!(result.is_ok(), mode == 2);
        assert_eq!(f.data.gets.lock().unwrap().len(), 0);
        assert_eq!(f.data.calls.lock().unwrap().len(), 1);
        r.close().await.unwrap();
        f.closed().await;
    }
}
#[tokio::test]
async fn authentication_session_endpoint_and_capability_writer_rejections_are_not_retried() {
    for status in [401, 404, 405, 503] {
        let f = Fixture::new(true, false, 0).await;
        f.data.mode.store(status, Ordering::SeqCst);
        let r = f.queued(Some(Fixture::policy()), 16).await;
        let b = r.tool("echo").await.unwrap();
        let error = b
            .submit_json(json!({"message":"reject"}))
            .unwrap()
            .await
            .unwrap_err();
        assert_eq!(error.execution_certainty, ExecutionCertainty::Unknown);
        assert_eq!(f.data.gets.lock().unwrap().len(), 1);
        assert_eq!(f.data.calls.lock().unwrap().len(), 1);
        r.close().await.unwrap();
        f.closed().await;
    }
    let f = Fixture::new(true, false, 0).await;
    f.data.caps.store(true, Ordering::SeqCst);
    f.data.reply_status.store(503, Ordering::SeqCst);
    let r = f.queued(Some(Fixture::policy()), 16).await;
    let b = r.tool("echo").await.unwrap();
    assert!(
        b.submit_json(json!({"message":"writer"}))
            .unwrap()
            .await
            .is_err()
    );
    assert_eq!(f.data.gets.lock().unwrap().len(), 0);
    r.close().await.unwrap();
    f.closed().await;
}
#[tokio::test]
async fn cancellation_and_deadlines_close_a_resumed_reader_and_keep_the_original_cancel_rpc() {
    for cancel in [false, true] {
        let f = Fixture::new(true, false, 0).await;
        f.data.mode.store(2, Ordering::SeqCst);
        let r = f.queued(Some(Fixture::policy()), 16).await;
        let b = r.tool("echo").await.unwrap();
        let call = if cancel {
            b.submit_json(json!({"message":"cancel"})).unwrap()
        } else {
            b.submit_with_timeout(
                &b.args_from(json!({"message":"timeout"})).unwrap(),
                Duration::from_millis(150),
            )
            .unwrap()
        };
        f.until(|d| d.gets.lock().unwrap().len() == 1).await;
        if cancel {
            call.cancellation_handle().cancel();
        }
        let error = call.await.unwrap_err();
        assert_eq!(error.code, if cancel { "Cancelled" } else { "Timeout" });
        assert_eq!(error.execution_certainty, ExecutionCertainty::Unknown);
        let original = f.data.calls.lock().unwrap()[0].rpc;
        {
            let cancelled = f.data.cancelled.lock().unwrap();
            assert_eq!(cancelled.len(), 1);
            assert_eq!(cancelled[0], (original, "Bearer first".into(), false));
        }
        r.close().await.unwrap();
        f.closed().await;
    }
}

#[tokio::test]
async fn invalid_policy_fails_before_the_queue_factory_or_any_network_io() {
    let f = Fixture::new(true, false, 0).await;
    for policy in [
        ResponseRecoveryPolicy {
            max_attempts: 0,
            ..Fixture::policy()
        },
        ResponseRecoveryPolicy {
            max_attempts: 9,
            ..Fixture::policy()
        },
        ResponseRecoveryPolicy {
            backoff: Duration::from_secs(61),
            ..Fixture::policy()
        },
        ResponseRecoveryPolicy {
            timeout: Duration::ZERO,
            ..Fixture::policy()
        },
        ResponseRecoveryPolicy {
            timeout: Duration::from_secs(601),
            ..Fixture::policy()
        },
    ] {
        assert_eq!(
            Client::new()
                .unwrap()
                .with_response_recovery(policy)
                .err()
                .unwrap()
                .code,
            "ConfigurationError"
        );
        let called = Arc::new(AtomicBool::new(false));
        let copy = called.clone();
        let result = QueuedRemote::spawn_with(
            f.config.clone(),
            QueueOptions {
                response_recovery: Some(policy),
                ..Default::default()
            },
            move || {
                copy.store(true, Ordering::SeqCst);
                Client::new()
            },
        )
        .await;
        assert_eq!(result.err().unwrap().code, "ConfigurationError");
        assert!(!called.load(Ordering::SeqCst));
    }
    for policy in [
        ResponseRecoveryPolicy {
            max_attempts: 1,
            backoff: Duration::ZERO,
            timeout: Duration::from_nanos(1),
        },
        ResponseRecoveryPolicy {
            max_attempts: 8,
            backoff: Duration::from_secs(60),
            timeout: Duration::from_secs(600),
        },
    ] {
        policy.validate().unwrap();
    }
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn queued_client_inherits_the_factory_policy_for_string_and_typed_calls() {
    let f = Fixture::new(true, true, 0).await;
    let remote = QueuedRemote::spawn_with(f.config.clone(), QueueOptions::default(), || {
        Client::new()?.with_response_recovery(Fixture::policy())
    })
    .await
    .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    #[derive(serde::Serialize)]
    struct Args<'a> {
        message: &'a str,
    }
    check(
        binding
            .submit_typed(&Args { message: "typed" })
            .unwrap()
            .await
            .unwrap(),
        "typed",
    );
    let output = remote
        .execute("echo", "--message 'command text'")
        .unwrap()
        .await
        .unwrap();
    let mcpx_command::Execution::Outcome(outcome) = output else {
        panic!("expected tool result")
    };
    check(outcome, "command text");
    assert_eq!(f.data.calls.lock().unwrap().len(), 2);
    assert_eq!(f.data.gets.lock().unwrap().len(), 2);
    remote.close().await.unwrap();
    f.closed().await;
}

#[tokio::test]
async fn mutable_cancellation_closes_the_get_and_the_next_call_still_works() {
    let f = Fixture::new(true, false, 0).await;
    f.data.mode.store(2, Ordering::SeqCst);
    let mut remote = f
        .direct(Some(Fixture::policy()), Duration::from_secs(5))
        .await;
    let binding = remote.fresh_tool("echo").await.unwrap();
    let cancel = remote.cancellation_handle();
    let (result, ()) = tokio::join!(
        remote.call_json(&binding, json!({"message":"cancel-direct"})),
        async {
            f.until(|d| d.gets.lock().unwrap().len() == 1 && d.live.load(Ordering::SeqCst) > 0)
                .await;
            cancel.cancel();
        }
    );
    let error = result.unwrap_err();
    assert_eq!(error.code, "Cancelled");
    assert_eq!(error.execution_certainty, ExecutionCertainty::Unknown);
    f.closed().await;
    assert_eq!(
        f.data.cancelled.lock().unwrap()[0].0,
        f.data.calls.lock().unwrap()[0].rpc
    );
    f.data.mode.store(0, Ordering::SeqCst);
    check(
        remote
            .call_json(&binding, json!({"message":"after-cancel"}))
            .await
            .unwrap(),
        "after-cancel",
    );
    remote.close().await.unwrap();
    f.closed().await;
}

#[tokio::test]
async fn dropping_mutable_call_future_aborts_io_before_explicit_core_cleanup() {
    let f = Fixture::new(true, false, 0).await;
    f.data.mode.store(2, Ordering::SeqCst);
    let mut remote = f
        .direct(Some(Fixture::policy()), Duration::from_secs(5))
        .await;
    let binding = remote.fresh_tool("echo").await.unwrap();
    {
        let future = remote.call_json(&binding, json!({"message":"dropped"}));
        tokio::pin!(future);
        tokio::select! {result=&mut future=>panic!("unexpected completion: {result:?}"),()=f.until(|d|d.gets.lock().unwrap().len()==1&&d.live.load(Ordering::SeqCst)>0)=>{}}
    }
    // The dropped borrow cannot leave a detached HTTP reader until cancel_active.
    f.closed().await;
    remote.cancel_active().await;
    assert_eq!(f.data.cancelled.lock().unwrap().len(), 1);
    f.data.mode.store(0, Ordering::SeqCst);
    check(
        remote
            .call_json(&binding, json!({"message":"after-drop"}))
            .await
            .unwrap(),
        "after-drop",
    );
    remote.close().await.unwrap();
    f.closed().await;
}

#[tokio::test]
async fn a_known_result_survives_deadline_and_cancellation_during_bounded_reply_cleanup() {
    for queued in [false, true] {
        let f = Fixture::new(true, false, 0).await;
        f.data.caps.store(true, Ordering::SeqCst);
        f.data.final_reply_delay.store(250, Ordering::SeqCst);
        if queued {
            let remote = f.queued(Some(Fixture::policy()), 16).await;
            let binding = remote.tool("echo").await.unwrap();
            let call = binding
                .submit_with_timeout(
                    &binding.args_from(json!({"message":"known"})).unwrap(),
                    Duration::from_millis(180),
                )
                .unwrap();
            f.until(|d| d.replies.lock().unwrap().len() == 2).await;
            call.cancellation_handle().cancel();
            check(call.await.unwrap(), "known");
            remote.close().await.unwrap();
        } else {
            let mut remote = f
                .direct(Some(Fixture::policy()), Duration::from_millis(180))
                .await;
            let binding = remote.fresh_tool("echo").await.unwrap();
            let cancel = remote.cancellation_handle();
            let (result, ()) = tokio::join!(
                remote.call_json(&binding, json!({"message":"known"})),
                async {
                    f.until(|d| d.replies.lock().unwrap().len() == 2).await;
                    cancel.cancel();
                }
            );
            check(result.unwrap(), "known");
            remote.close().await.unwrap();
        }
        assert_eq!(f.data.acks.lock().unwrap().len(), 2);
        assert!(f.data.cancelled.lock().unwrap().is_empty());
        f.closed().await;
    }
}

#[tokio::test]
async fn closing_during_backoff_or_a_get_releases_io_and_retained_operation_providers() {
    for reading in [false, true] {
        let f = Fixture::new(true, false, if reading { 0 } else { 500 }).await;
        f.data.mode.store(2, Ordering::SeqCst);
        let remote = f.queued(Some(Fixture::policy()), 16).await;
        let binding = remote.tool("echo").await.unwrap();
        let mut call = binding.submit_json(json!({"message":"close"})).unwrap();
        if reading {
            f.until(|d| d.gets.lock().unwrap().len() == 1 && d.live.load(Ordering::SeqCst) > 0)
                .await;
        } else {
            f.until(|d| d.streams.lock().unwrap().len() == 1 && d.live.load(Ordering::SeqCst) == 0)
                .await;
        }
        remote.close().await.unwrap();
        assert_eq!((&mut call).await.unwrap_err().code, "TransportClosed");
        f.closed().await;
        assert_eq!(f.data.gets.lock().unwrap().len(), usize::from(reading));
        f.until(|d| d.provider_drops.load(Ordering::SeqCst) == 1)
            .await;
        assert_eq!(f.data.calls.lock().unwrap().len(), 1);
        assert_eq!(f.data.cancelled.lock().unwrap().len(), 1);
        drop((binding, call));
    }
}

#[tokio::test]
async fn owner_close_preserves_a_known_result_and_joins_pending_capability_io() {
    let f = Fixture::new(true, false, 0).await;
    f.data.caps.store(true, Ordering::SeqCst);
    f.data.final_reply_delay.store(2000, Ordering::SeqCst);
    let remote = f.queued(Some(Fixture::policy()), 16).await;
    let binding = remote.tool("echo").await.unwrap();
    let mut call = binding
        .submit_json(json!({"message":"known-before-close"}))
        .unwrap();
    f.until(|d| d.replies.lock().unwrap().len() == 2).await;
    tokio::time::timeout(Duration::from_millis(700), remote.close())
        .await
        .unwrap()
        .unwrap();
    check((&mut call).await.unwrap(), "known-before-close");
    assert!(f.data.cancelled.lock().unwrap().is_empty());
    assert_eq!(f.data.acks.lock().unwrap().len(), 1);
    f.closed().await;
    f.until(|d| d.provider_drops.load(Ordering::SeqCst) == 1)
        .await;
    drop((binding, call));
}
