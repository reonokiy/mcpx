use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use mcpx_core::{
    CallOutcome, Credential, CredentialFuture, CredentialProvider, CredentialRequest,
    ExecutionCertainty,
};
use mcpx_native::{
    Client, Execution, HttpSessionRecoveryPolicy, ProtocolMode, QueueOptions, QueuedRemote,
    ServerConfig,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Notify;
fn consume(counter: &AtomicUsize) -> bool {
    let mut value = counter.load(Ordering::SeqCst);
    while value > 0 {
        match counter.compare_exchange_weak(value, value - 1, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(current) => value = current,
        }
    }
    false
}

#[derive(Default)]
struct Data {
    legacy: bool,
    stateless: bool,
    initialized: AtomicUsize,
    calls: Mutex<Vec<Value>>,
    requests: Mutex<Vec<(String, String, String)>>,
    owners: Mutex<BTreeMap<String, String>>,
    integer: AtomicBool,
    block_peer: AtomicBool,
    release_peer: Notify,
    expire_status: AtomicUsize,
    hold_list: AtomicBool,
    release_list: Notify,
    list_entered: Notify,
    flap: AtomicBool,
    second: AtomicBool,
    block_provider: AtomicBool,
    provider_entered: Notify,
    release_provider: Notify,
    resolves: AtomicUsize,
    provider_drops: AtomicUsize,
    notification_404: AtomicBool,
    expire_next_list: AtomicBool,
    zero_ttl: AtomicBool,
    hold_expire: AtomicBool,
    release_expire: Notify,
    response_recovery: AtomicBool,
    always_notification_404: AtomicBool,
    provider_error: AtomicBool,
    interrupt_list: AtomicBool,
    initialized_at: Mutex<Vec<Instant>>,
    expire_lists: AtomicUsize,
    expire_initialized: AtomicBool,
    list_failure_status: AtomicUsize,
    expired_body_pending: AtomicBool,
    hold_recovery_list: AtomicBool,
    rotate_on_expiry: AtomicBool,
    block_provider_on: AtomicUsize,
    hold_delete: AtomicBool,
    delete_entered: Notify,
    release_delete: Notify,
    fail_deletes: AtomicUsize,
    paginate_expiry: AtomicBool,
    list_cursors: Mutex<Vec<Option<String>>>,
    callback_drops: AtomicUsize,
}
struct CallbackGuard(Arc<Data>);
impl Drop for CallbackGuard {
    fn drop(&mut self) {
        self.0.callback_drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct Provider(Arc<Data>);
impl Drop for Provider {
    fn drop(&mut self) {
        self.0.provider_drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl CredentialProvider for Provider {
    fn resolve<'a>(&'a mut self, _: CredentialRequest<'a>) -> CredentialFuture<'a> {
        let ordinal = self.0.resolves.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(async move {
            if self.0.provider_error.swap(false, Ordering::SeqCst) {
                return Err(mcpx_core::Error::local(
                    "SessionExpired",
                    "provider failure is not HTTP evidence",
                ));
            }
            if self.0.block_provider.swap(false, Ordering::SeqCst)
                || self.0.block_provider_on.load(Ordering::SeqCst) == ordinal
            {
                self.0.provider_entered.notify_one();
                self.0.release_provider.notified().await;
            }
            Credential::bearer(
                if self.0.second.load(Ordering::SeqCst) {
                    "second"
                } else {
                    "first"
                }
                .into(),
                "same".into(),
            )
        })
    }
}
fn header(h: &HeaderMap, n: &str) -> String {
    h.get(n)
        .map(|v| v.to_str().unwrap().into())
        .unwrap_or_default()
}
async fn handle(State(data): State<Arc<Data>>, headers: HeaderMap, body: Bytes) -> Response {
    let rpc: Value = serde_json::from_slice(&body).unwrap();
    let method = rpc["method"].as_str().unwrap();
    let session = header(&headers, "mcp-session-id");
    let auth = header(&headers, "authorization");
    data.requests
        .lock()
        .unwrap()
        .push((method.into(), session.clone(), auth.clone()));
    let mut response_headers = HeaderMap::new();
    let result=match method {
        "server/discover" if data.legacy => return (StatusCode::BAD_REQUEST,Json(json!({"jsonrpc":"2.0","id":rpc["id"],"error":{"code":-32601,"message":"initialize first"}}))).into_response(),
        "server/discover" => json!({"supportedVersions":["2026-07-28"],"capabilities":{"tools":{"listChanged":true}}}),
        "initialize" => {
            let n=data.initialized.fetch_add(1,Ordering::SeqCst)+1;
            data.initialized_at.lock().unwrap().push(Instant::now());
            if !data.stateless {
                let session=format!("session-{n}"); data.owners.lock().unwrap().insert(session.clone(),auth);
                response_headers.insert("mcp-session-id",session.parse().unwrap());
            }
            json!({"protocolVersion":"2025-11-25","capabilities":{"tools":{"listChanged":true}}})
        }
        "notifications/initialized" => {
            if data.expire_initialized.swap(false,Ordering::SeqCst) {
                if data.rotate_on_expiry.load(Ordering::SeqCst) {data.second.store(true,Ordering::SeqCst);}
                return StatusCode::NOT_FOUND.into_response();
            }
            return StatusCode::ACCEPTED.into_response();
        }
        "notifications/cancelled" => return StatusCode::ACCEPTED.into_response(),
        "tools/list"=>{
            data.list_cursors.lock().unwrap().push(rpc["params"]["cursor"].as_str().map(str::to_owned));
            if data.paginate_expiry.load(Ordering::SeqCst) && data.initialized.load(Ordering::SeqCst)==1 && rpc["params"]["cursor"].is_null() {
                return Json(json!({"jsonrpc":"2.0","id":rpc["id"],"result":{"tools":[{"name":"old-partial","inputSchema":{"type":"object"}}],"nextCursor":"old-page"}})).into_response();
            }
            if consume(&data.expire_lists) && (!data.paginate_expiry.load(Ordering::SeqCst) || !rpc["params"]["cursor"].is_null()) {
                if data.rotate_on_expiry.load(Ordering::SeqCst) {data.second.store(true,Ordering::SeqCst);}
                let value=data.list_failure_status.load(Ordering::SeqCst);let status=StatusCode::from_u16(if value==0 {404}else{value as u16}).unwrap();
                if data.expired_body_pending.load(Ordering::SeqCst) {
                    return (status,[("content-type","application/json")],Body::from_stream(futures_util::stream::pending::<Result<Bytes,std::convert::Infallible>>())).into_response();
                }
                return status.into_response();
            }
            if data.interrupt_list.swap(false,Ordering::SeqCst) {
                return ([("content-type","application/json")],Body::from_stream(futures_util::stream::once(async {Err::<Bytes,_>(std::io::Error::other("fixture interrupted"))}))).into_response();
            }
            if data.expire_next_list.swap(false,Ordering::SeqCst) { return StatusCode::NOT_FOUND.into_response(); }
            if data.hold_list.load(Ordering::SeqCst) || data.hold_recovery_list.load(Ordering::SeqCst) && data.initialized.load(Ordering::SeqCst)>1 {data.list_entered.notify_one();data.release_list.notified().await;}
            if data.flap.load(Ordering::SeqCst) && data.initialized.load(Ordering::SeqCst)>1 {return StatusCode::NOT_FOUND.into_response();}
            let ty=if data.integer.load(Ordering::SeqCst){"integer"}else{"string"};
            json!({"tools":[{"name":"echo","inputSchema":{"type":"object","properties":{"message":{"type":ty}},"required":["message"]},"outputSchema":{"type":"object","properties":{"message":{"type":ty}},"required":["message"]}}]})
        }
        "tools/call"=>{
            data.calls.lock().unwrap().push(rpc.clone());
            if rpc["params"]["arguments"]["message"]=="recover-stream" {
                return ([("content-type","text/event-stream")],"id: call-cursor\nretry: 0\ndata:\n\n").into_response();
            }
            if rpc["params"]["arguments"]["message"]=="expire" {
                if data.hold_expire.load(Ordering::SeqCst) {data.release_expire.notified().await;}
                return StatusCode::from_u16(data.expire_status.load(Ordering::SeqCst) as u16).unwrap().into_response();
            }
            if rpc["params"]["arguments"]["message"]=="peer" && data.block_peer.load(Ordering::SeqCst) {data.release_peer.notified().await;}
            json!({"content":[],"structuredContent":rpc["params"]["arguments"]})
        }
        _=>panic!("unexpected method {method}")
    };
    (
        response_headers,
        Json(json!({"jsonrpc":"2.0","id":rpc["id"],"result":result})),
    )
        .into_response()
}
async fn delete(State(data): State<Arc<Data>>, headers: HeaderMap) -> StatusCode {
    let session = header(&headers, "mcp-session-id");
    let auth = header(&headers, "authorization");
    assert_eq!(data.owners.lock().unwrap().get(&session), Some(&auth));
    data.requests
        .lock()
        .unwrap()
        .push(("DELETE".into(), session, auth));
    if data.hold_delete.load(Ordering::SeqCst) {
        data.delete_entered.notify_one();
        data.release_delete.notified().await;
    }
    if consume(&data.fail_deletes) {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    StatusCode::NO_CONTENT
}
async fn listen(State(data): State<Arc<Data>>, headers: HeaderMap) -> Response {
    data.requests.lock().unwrap().push((
        "GET".into(),
        header(&headers, "mcp-session-id"),
        header(&headers, "authorization"),
    ));
    if headers.contains_key("last-event-id") {
        return StatusCode::NOT_FOUND.into_response();
    }
    if data.notification_404.swap(false, Ordering::SeqCst)
        || data.always_notification_404.load(Ordering::SeqCst)
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    let body = Body::from_stream(
        futures_util::stream::once(async {
            Ok::<_, std::convert::Infallible>(Bytes::from_static(
                b"id: listener\nretry: 0\ndata:\n\n",
            ))
        })
        .chain(futures_util::stream::pending()),
    );
    ([("content-type", "text/event-stream")], body).into_response()
}
use futures_util::StreamExt;
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
    fn client(&self, policy: Option<HttpSessionRecoveryPolicy>) -> Client {
        let mut client = Client::new()
            .unwrap()
            .with_timeout(Duration::from_secs(3))
            .with_credential_provider(Provider(self.data.clone()));
        if let Some(policy) = policy {
            client = client.with_http_session_recovery(policy).unwrap();
        }
        client
    }
    async fn new(legacy: bool, auto: bool, stateless: bool) -> Self {
        let data = Arc::new(Data {
            legacy,
            stateless,
            expire_status: AtomicUsize::new(404),
            ..Default::default()
        });
        let app = Router::new()
            .route("/mcp", post(handle).delete(delete).get(listen))
            .with_state(data.clone());
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp", socket.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(socket, app).await.unwrap();
        });
        let config = ServerConfig::Http {
            url,
            protocol: if auto {
                ProtocolMode::Auto
            } else if legacy {
                ProtocolMode::Legacy
            } else {
                ProtocolMode::Modern
            },
            credential: None,
        };
        Self {
            data,
            config,
            server,
        }
    }
    async fn owner(
        &self,
        policy: Option<HttpSessionRecoveryPolicy>,
        notifications: bool,
    ) -> QueuedRemote {
        let data = self.data.clone();
        QueuedRemote::spawn_with(
            self.config.clone(),
            QueueOptions {
                http_notifications: notifications,
                ..Default::default()
            },
            move || {
                let ttl = if data.zero_ttl.load(Ordering::SeqCst) {
                    Duration::ZERO
                } else {
                    Duration::from_secs(60)
                };
                let response_recovery = data.response_recovery.load(Ordering::SeqCst);
                let mut client = Client::new()?
                    .with_timeout(Duration::from_secs(3))
                    .with_catalog_ttl(ttl)
                    .with_credential_provider(Provider(data));
                if response_recovery {
                    client =
                        client.with_response_recovery(mcpx_native::ResponseRecoveryPolicy {
                            max_attempts: 2,
                            backoff: Duration::ZERO,
                            timeout: Duration::from_secs(1),
                        })?;
                }
                if let Some(policy) = policy {
                    client = client.with_http_session_recovery(policy)?;
                }
                Ok(client)
            },
        )
        .await
        .unwrap()
    }
    async fn until(&self, f: impl Fn(&Data) -> bool) {
        tokio::time::timeout(Duration::from_secs(4), async {
            while !f(&self.data) {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
    }
}
fn policy() -> HttpSessionRecoveryPolicy {
    HttpSessionRecoveryPolicy {
        max_attempts: 2,
        backoff: Duration::from_millis(15),
        timeout: Duration::from_secs(2),
    }
}
fn message(value: CallOutcome) -> Value {
    let CallOutcome::Completed(r) = value else {
        panic!()
    };
    r.raw["structuredContent"]["message"].clone()
}
fn execution(value: Execution) -> Value {
    let Execution::Outcome(r) = value else {
        panic!()
    };
    message(r)
}

#[tokio::test]
async fn renews_only_expired_legacy_sessions_and_never_replays_the_failed_tool() {
    for auto in [false, true] {
        let f = Fixture::new(true, auto, false).await;
        let owner = f.owner(Some(policy()), false).await;
        let binding = owner.tool("echo").await.unwrap();
        let failure = binding
            .submit_json(json!({"message":"expire"}))
            .unwrap()
            .await
            .err()
            .unwrap();
        assert_eq!(failure.code, "SessionExpired");
        assert_eq!(failure.execution_certainty, ExecutionCertainty::Unknown);
        assert_eq!(
            execution(
                owner
                    .execute("echo", "--message after")
                    .unwrap()
                    .await
                    .unwrap()
            ),
            "after"
        );
        let stale = binding
            .submit_json(json!({"message":"never"}))
            .unwrap()
            .await
            .err()
            .unwrap();
        assert_eq!(stale.code, "BindingStale");
        assert_eq!(stale.execution_certainty, ExecutionCertainty::NotSent);
        assert_eq!(f.data.initialized.load(Ordering::SeqCst), 2);
        assert_eq!(f.data.calls.lock().unwrap().len(), 2);
        owner.close().await.unwrap();
        let rows = f.data.requests.lock().unwrap();
        let sessions: Vec<_> = rows
            .iter()
            .filter(|r| r.0 == "DELETE")
            .map(|r| r.1.as_str())
            .collect();
        assert_eq!(sessions, ["session-1", "session-2"]);
    }
}
#[tokio::test]
async fn disabled_modern_stateless_and_other_http_failures_do_not_renew() {
    for (legacy, stateless, enabled, status) in [
        (true, false, false, 404),
        (false, false, true, 404),
        (true, true, true, 404),
        (true, false, true, 401),
        (true, false, true, 503),
    ] {
        let f = Fixture::new(legacy, false, stateless).await;
        f.data.expire_status.store(status, Ordering::SeqCst);
        let owner = f.owner(enabled.then(policy), false).await;
        let b = owner.tool("echo").await.unwrap();
        let failure = b
            .submit_json(json!({"message":"expire"}))
            .unwrap()
            .await
            .err()
            .unwrap();
        assert_eq!(
            failure.code,
            if status == 401 {
                "AuthFailed"
            } else if legacy && !stateless && status == 404 {
                "SessionExpired"
            } else {
                "HttpError"
            }
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(
            f.data.initialized.load(Ordering::SeqCst),
            usize::from(legacy)
        );
        owner.close().await.unwrap();
    }
}
#[tokio::test]
async fn drains_old_calls_then_binds_waiting_commands_to_the_new_schema() {
    let f = Fixture::new(true, true, false).await;
    let owner = f.owner(Some(policy()), false).await;
    let b = owner.tool("echo").await.unwrap();
    f.data.block_peer.store(true, Ordering::SeqCst);
    let mut peers = Vec::new();
    for _ in 0..15 {
        peers.push(b.submit_json(json!({"message":"peer"})).unwrap());
    }
    f.until(|d| d.calls.lock().unwrap().len() == 15).await;
    let failed = b.submit_json(json!({"message":"expire"})).unwrap();
    let old = b.submit_json(json!({"message":"never-queued"})).unwrap();
    let named = owner.execute("echo", "--message 42").unwrap();
    assert_eq!(failed.await.err().unwrap().code, "SessionExpired");
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 1);
    f.data.integer.store(true, Ordering::SeqCst);
    f.data.block_peer.store(false, Ordering::SeqCst);
    f.data.release_peer.notify_waiters();
    for peer in peers {
        assert_eq!(message(peer.await.unwrap()), "peer");
    }
    assert_eq!(old.await.err().unwrap().code, "BindingStale");
    assert_eq!(execution(named.await.unwrap()), 42);
    assert_eq!(f.data.calls.lock().unwrap().len(), 17);
    owner.close().await.unwrap();
}
#[tokio::test]
async fn recovery_window_bounds_peer_drain_and_does_not_reset_waiting_deadlines() {
    let f = Fixture::new(true, false, false).await;
    let owner = f
        .owner(
            Some(HttpSessionRecoveryPolicy {
                timeout: Duration::from_millis(180),
                ..policy()
            }),
            false,
        )
        .await;
    let b = owner.tool("echo").await.unwrap();
    f.data.block_peer.store(true, Ordering::SeqCst);
    let peer = b.submit_json(json!({"message":"peer"})).unwrap();
    f.until(|d| d.calls.lock().unwrap().len() == 1).await;
    let expire = b.submit_json(json!({"message":"expire"})).unwrap();
    let start = Instant::now();
    assert_eq!(expire.await.err().unwrap().code, "SessionExpired");
    let waiting = owner.execute("echo", "--message never").unwrap();
    assert_eq!(waiting.await.err().unwrap().code, "TransportClosed");
    assert_eq!(
        peer.await.err().unwrap().execution_certainty,
        ExecutionCertainty::Unknown
    );
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 1);
    owner.close().await.unwrap();
}
#[tokio::test]
async fn metadata_flaps_spend_one_round_and_time_budget() {
    let f = Fixture::new(true, true, false).await;
    let owner = f.owner(Some(policy()), false).await;
    let b = owner.tool("echo").await.unwrap();
    f.data.flap.store(true, Ordering::SeqCst);
    assert_eq!(
        b.submit_json(json!({"message":"expire"}))
            .unwrap()
            .await
            .err()
            .unwrap()
            .code,
        "SessionExpired"
    );
    assert_eq!(
        owner
            .execute("echo", "--message never")
            .unwrap()
            .await
            .err()
            .unwrap()
            .code,
        "TransportClosed"
    );
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 3);
    assert_eq!(f.data.calls.lock().unwrap().len(), 1);
    owner.close().await.unwrap();
}
#[tokio::test]
async fn original_cleanup_credentials_precede_current_provider_and_shutdown_drops_it() {
    let f = Fixture::new(true, false, false).await;
    let owner = f.owner(Some(policy()), false).await;
    let b = owner.tool("echo").await.unwrap();
    f.data.second.store(true, Ordering::SeqCst);
    f.data.hold_list.store(true, Ordering::SeqCst);
    assert_eq!(
        b.submit_json(json!({"message":"expire"}))
            .unwrap()
            .await
            .err()
            .unwrap()
            .code,
        "SessionExpired"
    );
    f.data.list_entered.notified().await;
    let rows = f.data.requests.lock().unwrap().clone();
    let deleted = rows.iter().position(|r| r.0 == "DELETE").unwrap();
    let initialized = rows.iter().rposition(|r| r.0 == "initialize").unwrap();
    assert!(deleted < initialized);
    assert_eq!(rows[deleted].2, "Bearer first");
    assert_eq!(rows[initialized].2, "Bearer second");
    let waiting = owner.execute("echo", "--message never").unwrap();
    owner.close().await.unwrap();
    assert_eq!(waiting.await.err().unwrap().code, "TransportClosed");
    assert_eq!(f.data.provider_drops.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn idle_notification_session_expiry_also_renews_without_a_tool_retry() {
    let f = Fixture::new(true, false, false).await;
    f.data.notification_404.store(true, Ordering::SeqCst);
    let owner = f.owner(Some(policy()), true).await;
    owner.tool("echo").await.unwrap();
    f.until(|d| d.initialized.load(Ordering::SeqCst) == 2).await;
    assert_eq!(
        execution(
            owner
                .execute("echo", "--message after")
                .unwrap()
                .await
                .unwrap()
        ),
        "after"
    );
    assert_eq!(f.data.calls.lock().unwrap().len(), 1);
    owner.close().await.unwrap();
}
#[tokio::test]
async fn policy_limits_are_validated_before_connecting() {
    for p in [
        HttpSessionRecoveryPolicy {
            max_attempts: 0,
            ..policy()
        },
        HttpSessionRecoveryPolicy {
            max_attempts: 9,
            ..policy()
        },
        HttpSessionRecoveryPolicy {
            backoff: Duration::from_secs(61),
            ..policy()
        },
        HttpSessionRecoveryPolicy {
            timeout: Duration::ZERO,
            ..policy()
        },
        HttpSessionRecoveryPolicy {
            timeout: Duration::from_secs(601),
            ..policy()
        },
    ] {
        let e = Client::new()
            .unwrap()
            .with_http_session_recovery(p)
            .err()
            .unwrap();
        assert_eq!(e.code, "ConfigurationError");
        assert_eq!(e.execution_certainty, ExecutionCertainty::NotSent);
    }
}

#[tokio::test]
async fn unsent_command_metadata_expiry_rebinds_without_dispatching_a_failed_tool() {
    let f = Fixture::new(true, false, false).await;
    f.data.zero_ttl.store(true, Ordering::SeqCst);
    let owner = f.owner(Some(policy()), false).await;
    owner.tool("echo").await.unwrap();
    f.data.expire_next_list.store(true, Ordering::SeqCst);
    assert_eq!(
        execution(
            owner
                .execute("echo", "--message after")
                .unwrap()
                .await
                .unwrap()
        ),
        "after"
    );
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 2);
    assert_eq!(f.data.calls.lock().unwrap().len(), 1);
    owner.close().await.unwrap();
}

#[tokio::test]
async fn pending_provider_is_cancelled_and_unsent_command_keeps_one_lifecycle() {
    use mcpx_core::{EventLimits, OperationEventKind};
    let f = Fixture::new(true, false, false).await;
    let data = f.data.clone();
    let (owner, mut events) = QueuedRemote::spawn_with_events(
        f.config.clone(),
        QueueOptions::default(),
        EventLimits::default(),
        move || {
            Client::new()?
                .with_credential_provider(Provider(data))
                .with_http_session_recovery(policy())
        },
    )
    .await
    .unwrap();
    let b = owner.tool("echo").await.unwrap();
    f.data.hold_expire.store(true, Ordering::SeqCst);
    let failed = b.submit_json(json!({"message":"expire"})).unwrap();
    f.until(|d| d.calls.lock().unwrap().len() == 1).await;
    f.data.block_provider.store(true, Ordering::SeqCst);
    let waiting = owner.execute("echo", "--message after").unwrap();
    let id = waiting.id();
    f.data.provider_entered.notified().await;
    f.data.release_expire.notify_one();
    assert_eq!(failed.await.err().unwrap().code, "SessionExpired");
    assert_eq!(execution(waiting.await.unwrap()), "after");
    let mut records = Vec::new();
    while let Some(batch) = events.try_next_batch() {
        records.extend(batch.events);
    }
    assert_eq!(
        records
            .iter()
            .filter(|e| e.queue_operation_id == id && e.kind == OperationEventKind::Started)
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|e| e.queue_operation_id == id && e.kind == OperationEventKind::Completed)
            .count(),
        1
    );
    assert!(!records.iter().any(|e| e.queue_operation_id == id
        && matches!(
            e.kind,
            OperationEventKind::Failed | OperationEventKind::Cancelled
        )));
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 2);
    owner.close().await.unwrap();
}

#[tokio::test]
async fn resumed_tool_get_session_expiry_renews_without_replaying_its_post() {
    let f = Fixture::new(true, false, false).await;
    f.data.response_recovery.store(true, Ordering::SeqCst);
    let owner = f.owner(Some(policy()), false).await;
    let b = owner.tool("echo").await.unwrap();
    let e = b
        .submit_json(json!({"message":"recover-stream"}))
        .unwrap()
        .await
        .err()
        .unwrap();
    assert_eq!(e.code, "SessionExpired");
    assert_eq!(e.execution_certainty, ExecutionCertainty::Unknown);
    assert_eq!(
        execution(
            owner
                .execute("echo", "--message after")
                .unwrap()
                .await
                .unwrap()
        ),
        "after"
    );
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 2);
    assert_eq!(f.data.calls.lock().unwrap().len(), 2);
    owner.close().await.unwrap();
}

#[tokio::test]
async fn waiting_cancel_and_deadline_are_serviced_during_session_discovery() {
    let f = Fixture::new(true, false, false).await;
    let owner = f.owner(Some(policy()), false).await;
    let b = owner.tool("echo").await.unwrap();
    f.data.hold_list.store(true, Ordering::SeqCst);
    assert_eq!(
        b.submit_json(json!({"message":"expire"}))
            .unwrap()
            .await
            .err()
            .unwrap()
            .code,
        "SessionExpired"
    );
    f.data.list_entered.notified().await;
    let cancelled = b.submit_json(json!({"message":"never-cancelled"})).unwrap();
    cancelled.cancel();
    let args = b.args_from(json!({"message":"never-expired"})).unwrap();
    let expired = b
        .submit_with_timeout(&args, Duration::from_millis(60))
        .unwrap();
    let e = cancelled.await.err().unwrap();
    assert_eq!(e.code, "Cancelled");
    assert_eq!(e.execution_certainty, ExecutionCertainty::NotSent);
    let e = expired.await.err().unwrap();
    assert_eq!(e.code, "Timeout");
    assert_eq!(e.execution_certainty, ExecutionCertainty::NotSent);
    f.data.hold_list.store(false, Ordering::SeqCst);
    f.data.release_list.notify_one();
    assert_eq!(
        execution(
            owner
                .execute("echo", "--message after")
                .unwrap()
                .await
                .unwrap()
        ),
        "after"
    );
    assert_eq!(f.data.calls.lock().unwrap().len(), 2);
    owner.close().await.unwrap();
}

#[tokio::test]
async fn pending_recovery_provider_spends_the_same_total_window() {
    let f = Fixture::new(true, false, false).await;
    let owner = f
        .owner(
            Some(HttpSessionRecoveryPolicy {
                timeout: Duration::from_millis(160),
                ..policy()
            }),
            false,
        )
        .await;
    let b = owner.tool("echo").await.unwrap();
    f.data.hold_expire.store(true, Ordering::SeqCst);
    let failed = b.submit_json(json!({"message":"expire"})).unwrap();
    f.until(|d| d.calls.lock().unwrap().len() == 1).await;
    f.data.block_provider.store(true, Ordering::SeqCst);
    let start = Instant::now();
    f.data.release_expire.notify_one();
    assert_eq!(failed.await.err().unwrap().code, "SessionExpired");
    f.data.provider_entered.notified().await;
    let waiting = owner.execute("echo", "--message never").unwrap();
    assert_eq!(waiting.await.err().unwrap().code, "TransportClosed");
    owner.close().await.unwrap();
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(f.data.provider_drops.load(Ordering::SeqCst), 1);
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn successful_tool_results_reset_a_one_round_session_budget() {
    let f = Fixture::new(true, true, false).await;
    let owner = f
        .owner(
            Some(HttpSessionRecoveryPolicy {
                max_attempts: 1,
                ..policy()
            }),
            false,
        )
        .await;
    for n in 1..=4 {
        let b = owner.tool("echo").await.unwrap();
        assert_eq!(
            b.submit_json(json!({"message":"expire"}))
                .unwrap()
                .await
                .err()
                .unwrap()
                .code,
            "SessionExpired"
        );
        assert_eq!(
            execution(
                owner
                    .execute("echo", "--message after")
                    .unwrap()
                    .await
                    .unwrap()
            ),
            "after"
        );
        assert_eq!(f.data.initialized.load(Ordering::SeqCst), n + 1);
    }
    assert_eq!(f.data.calls.lock().unwrap().len(), 8);
    owner.close().await.unwrap();
}

#[tokio::test]
async fn repeated_idle_notification_expiry_cannot_reinitialize_forever() {
    let f = Fixture::new(true, false, false).await;
    f.data.always_notification_404.store(true, Ordering::SeqCst);
    let owner = f.owner(Some(policy()), true).await;
    f.until(|d| d.provider_drops.load(Ordering::SeqCst) == 1)
        .await;
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 3);
    assert!(f.data.calls.lock().unwrap().is_empty());
    owner.close().await.unwrap();
}

#[tokio::test]
async fn a_provider_error_code_cannot_impersonate_observed_http_session_expiry() {
    let f = Fixture::new(true, false, false).await;
    let owner = f.owner(Some(policy()), false).await;
    owner.tool("echo").await.unwrap();
    f.data.provider_error.store(true, Ordering::SeqCst);
    assert_eq!(
        owner
            .execute("echo", "--message never")
            .unwrap()
            .await
            .err()
            .unwrap()
            .code,
        "SessionExpired"
    );
    assert_eq!(
        execution(
            owner
                .execute("echo", "--message after")
                .unwrap()
                .await
                .unwrap()
        ),
        "after"
    );
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 1);
    assert_eq!(f.data.calls.lock().unwrap().len(), 1);
    owner.close().await.unwrap();
}

#[tokio::test]
async fn interrupted_discovery_retries_with_backoff_and_closes_each_created_session() {
    let f = Fixture::new(true, false, false).await;
    let owner = f.owner(Some(policy()), false).await;
    let b = owner.tool("echo").await.unwrap();
    f.data.interrupt_list.store(true, Ordering::SeqCst);
    assert_eq!(
        b.submit_json(json!({"message":"expire"}))
            .unwrap()
            .await
            .err()
            .unwrap()
            .code,
        "SessionExpired"
    );
    assert_eq!(
        execution(
            owner
                .execute("echo", "--message after")
                .unwrap()
                .await
                .unwrap()
        ),
        "after"
    );
    {
        let times = f.data.initialized_at.lock().unwrap();
        assert_eq!(times.len(), 3);
        assert!(times[2].duration_since(times[1]) >= policy().backoff);
    }
    owner.close().await.unwrap();
    let requests = f.data.requests.lock().unwrap();
    let deleted: Vec<_> = requests
        .iter()
        .filter(|r| r.0 == "DELETE")
        .map(|r| r.1.as_str())
        .collect();
    assert_eq!(deleted, ["session-1", "session-2", "session-3"]);
}

#[tokio::test]
async fn a_stop_queued_during_renewal_keeps_the_new_session_listener_disabled() {
    let f = Fixture::new(true, false, false).await;
    let owner = f.owner(Some(policy()), true).await;
    let b = owner.tool("echo").await.unwrap();
    f.until(|d| d.requests.lock().unwrap().iter().any(|r| r.0 == "GET"))
        .await;
    f.data.hold_list.store(true, Ordering::SeqCst);
    assert_eq!(
        b.submit_json(json!({"message":"expire"}))
            .unwrap()
            .await
            .err()
            .unwrap()
            .code,
        "SessionExpired"
    );
    f.data.list_entered.notified().await;
    let stop = owner.stop_notifications().unwrap();
    f.data.hold_list.store(false, Ordering::SeqCst);
    f.data.release_list.notify_one();
    stop.await.unwrap();
    assert_eq!(
        execution(
            owner
                .execute("echo", "--message after")
                .unwrap()
                .await
                .unwrap()
        ),
        "after"
    );
    assert_eq!(
        f.data
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.0 == "GET")
            .count(),
        1
    );
    owner.close().await.unwrap();
}

#[tokio::test]
async fn an_idle_healthy_interval_resets_the_budget_without_a_successful_tool() {
    let f = Fixture::new(true, false, false).await;
    f.data.notification_404.store(true, Ordering::SeqCst);
    let owner = f
        .owner(
            Some(HttpSessionRecoveryPolicy {
                max_attempts: 1,
                timeout: Duration::from_millis(120),
                ..policy()
            }),
            true,
        )
        .await;
    f.until(|d| {
        d.requests
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.0 == "GET" && r.1 == "session-2")
    })
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        owner
            .execute("echo", "--message expire")
            .unwrap()
            .await
            .err()
            .unwrap()
            .code,
        "SessionExpired"
    );
    f.until(|d| {
        d.requests
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.0 == "GET" && r.1 == "session-3")
    })
    .await;
    assert_eq!(f.data.calls.lock().unwrap().len(), 1);
    owner.close().await.unwrap();
}

#[tokio::test]
async fn startup_session_expiry_recovers_mutable_and_queued_clients_before_returning() {
    for initial_ack in [false, true] {
        for auto in [false, true] {
            for queued in [false, true] {
                let f = Fixture::new(true, auto, false).await;
                if initial_ack {
                    f.data.expire_initialized.store(true, Ordering::SeqCst);
                } else {
                    f.data.expire_lists.store(1, Ordering::SeqCst);
                }
                f.data.rotate_on_expiry.store(true, Ordering::SeqCst);
                if queued {
                    let owner = f.owner(Some(policy()), false).await;
                    assert_eq!(
                        execution(
                            owner
                                .execute("echo", "--message after")
                                .unwrap()
                                .await
                                .unwrap()
                        ),
                        "after"
                    );
                    owner.close().await.unwrap();
                } else {
                    let mut remote = f
                        .config
                        .connect(f.client(Some(policy())), None)
                        .await
                        .unwrap();
                    assert_eq!(
                        execution(remote.execute("echo", "--message after").await.unwrap()),
                        "after"
                    );
                    remote.close().await.unwrap();
                }
                assert_eq!(f.data.initialized.load(Ordering::SeqCst), 2);
                assert_eq!(f.data.calls.lock().unwrap().len(), 1);
                let records = f.data.requests.lock().unwrap();
                let old = records.iter().position(|r| r.0 == "DELETE").unwrap();
                let new = records.iter().rposition(|r| r.0 == "initialize").unwrap();
                assert!(old < new);
                assert_eq!(records[old].2, "Bearer first");
                assert_eq!(records[new].2, "Bearer second");
                assert_eq!(f.data.provider_drops.load(Ordering::SeqCst), 1);
            }
        }
    }
}

#[tokio::test]
async fn startup_ignores_disabled_stateless_modern_and_auth_or_server_errors() {
    for (legacy, stateless, enabled, status) in [
        (true, false, false, 404),
        (false, false, true, 404),
        (true, true, true, 404),
        (true, false, true, 401),
        (true, false, true, 503),
    ] {
        let f = Fixture::new(legacy, false, stateless).await;
        f.data.expire_lists.store(1, Ordering::SeqCst);
        f.data.list_failure_status.store(status, Ordering::SeqCst);
        let e = f
            .config
            .connect(f.client(enabled.then(policy)), None)
            .await
            .err()
            .unwrap();
        assert_eq!(
            e.code,
            if status == 401 {
                "AuthFailed"
            } else if legacy && !stateless && status == 404 {
                "SessionExpired"
            } else {
                "HttpError"
            }
        );
        assert_eq!(e.execution_certainty, ExecutionCertainty::NotSent);
        assert_eq!(
            f.data.initialized.load(Ordering::SeqCst),
            usize::from(legacy)
        );
        assert_eq!(f.data.provider_drops.load(Ordering::SeqCst), 1);
        assert!(f.data.calls.lock().unwrap().is_empty());
    }
    let f = Fixture::new(true, false, false).await;
    f.data.provider_error.store(true, Ordering::SeqCst);
    assert_eq!(
        f.config
            .connect(f.client(Some(policy())), None)
            .await
            .err()
            .unwrap()
            .code,
        "SessionExpired"
    );
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 0);
    let f = Fixture::new(true, false, false).await;
    f.data.interrupt_list.store(true, Ordering::SeqCst);
    assert_eq!(
        f.config
            .connect(f.client(Some(policy())), None)
            .await
            .err()
            .unwrap()
            .code,
        "TransportError"
    );
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn startup_flaps_share_rounds_and_backoff_and_cleanup_every_observed_session() {
    let f = Fixture::new(true, true, false).await;
    f.data.expire_lists.store(10, Ordering::SeqCst);
    assert_eq!(
        f.config
            .connect(f.client(Some(policy())), None)
            .await
            .err()
            .unwrap()
            .code,
        "SessionExpired"
    );
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 3);
    {
        let times = f.data.initialized_at.lock().unwrap();
        assert!(times[2].duration_since(times[1]) >= policy().backoff);
    }
    let requests = f.data.requests.lock().unwrap();
    let deleted: Vec<_> = requests
        .iter()
        .filter(|r| r.0 == "DELETE")
        .map(|r| r.1.as_str())
        .collect();
    assert_eq!(deleted, ["session-1", "session-2", "session-3"]);
    assert_eq!(f.data.provider_drops.load(Ordering::SeqCst), 1);
    assert!(f.data.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn startup_404_headers_do_not_wait_for_an_unfinished_error_body() {
    let f = Fixture::new(true, false, false).await;
    f.data.expire_lists.store(1, Ordering::SeqCst);
    f.data.expired_body_pending.store(true, Ordering::SeqCst);
    let start = Instant::now();
    let mut remote = f
        .config
        .connect(
            f.client(Some(HttpSessionRecoveryPolicy {
                timeout: Duration::from_millis(180),
                ..policy()
            })),
            None,
        )
        .await
        .unwrap();
    assert!(start.elapsed() < Duration::from_millis(400));
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 2);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn startup_failure_on_a_later_page_discards_the_partial_catalog() {
    let f = Fixture::new(true, false, false).await;
    f.data.paginate_expiry.store(true, Ordering::SeqCst);
    f.data.expire_lists.store(1, Ordering::SeqCst);
    f.data.integer.store(true, Ordering::SeqCst);
    let mut remote = f
        .config
        .connect(f.client(Some(policy())), None)
        .await
        .unwrap();
    assert!(remote.tool("old-partial").is_err());
    assert_eq!(
        execution(remote.execute("echo", "--message 42").await.unwrap()),
        42
    );
    assert_eq!(
        *f.data.list_cursors.lock().unwrap(),
        vec![None, Some("old-page".into()), None]
    );
    remote.close().await.unwrap();
}

#[tokio::test]
async fn startup_cleanup_failure_is_terminal_and_final_cleanup_retains_the_snapshot() {
    let f = Fixture::new(true, false, false).await;
    f.data.expire_lists.store(1, Ordering::SeqCst);
    f.data.fail_deletes.store(1, Ordering::SeqCst);
    assert_eq!(
        f.config
            .connect(f.client(Some(policy())), None)
            .await
            .err()
            .unwrap()
            .code,
        "HttpError"
    );
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 1);
    assert_eq!(f.data.resolves.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.data
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.0 == "DELETE" && r.1 == "session-1" && r.2 == "Bearer first")
            .count(),
        2
    );
    assert_eq!(f.data.provider_drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn initial_recovery_window_includes_delete_and_provider_and_new_metadata() {
    for phase in ["delete", "provider", "metadata"] {
        let f = Fixture::new(true, false, false).await;
        f.data.expire_lists.store(1, Ordering::SeqCst);
        if phase == "delete" {
            f.data.hold_delete.store(true, Ordering::SeqCst);
        }
        if phase == "provider" {
            f.data.block_provider_on.store(2, Ordering::SeqCst);
        }
        if phase == "metadata" {
            f.data.hold_recovery_list.store(true, Ordering::SeqCst);
        }
        let start = Instant::now();
        let e = f
            .config
            .connect(
                f.client(Some(HttpSessionRecoveryPolicy {
                    timeout: Duration::from_millis(120),
                    ..policy()
                })),
                None,
            )
            .await
            .err()
            .unwrap();
        assert_eq!(e.code, "Timeout");
        assert_eq!(e.execution_certainty, ExecutionCertainty::NotSent);
        assert!(start.elapsed() < Duration::from_secs(1));
        assert_eq!(f.data.provider_drops.load(Ordering::SeqCst), 1);
        assert!(f.data.calls.lock().unwrap().is_empty());
        assert_eq!(
            f.data.initialized.load(Ordering::SeqCst),
            if phase == "metadata" { 2 } else { 1 }
        );
    }
}

#[tokio::test]
async fn explicit_startup_cancellation_stops_a_recovery_provider() {
    let f = Fixture::new(true, false, false).await;
    f.data.expire_lists.store(1, Ordering::SeqCst);
    f.data.block_provider_on.store(2, Ordering::SeqCst);
    let client = f.client(Some(policy()));
    let cancel = client.cancellation_handle();
    let (result, ()) = tokio::join!(f.config.connect(client, None), async {
        f.data.provider_entered.notified().await;
        cancel.cancel();
    });
    assert_eq!(result.err().unwrap().code, "Cancelled");
    assert_eq!(f.data.provider_drops.load(Ordering::SeqCst), 1);
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 1);
    assert!(f.data.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn dropping_queued_startup_cancels_long_recovery_backoff() {
    let f = Fixture::new(true, false, false).await;
    f.data.expire_lists.store(2, Ordering::SeqCst);
    let config = f.config.clone();
    let data = f.data.clone();
    let task = tokio::spawn(async move {
        QueuedRemote::spawn_with(config, QueueOptions::default(), move || {
            Client::new()?
                .with_credential_provider(Provider(data))
                .with_http_session_recovery(HttpSessionRecoveryPolicy {
                    max_attempts: 3,
                    backoff: Duration::from_secs(30),
                    timeout: Duration::from_secs(45),
                })
        })
        .await
    });
    f.until(|d| {
        d.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.0 == "tools/list")
            .count()
            == 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 2);
    let start = Instant::now();
    task.abort();
    assert!(task.await.err().unwrap().is_cancelled());
    f.until(|d| {
        d.provider_drops.load(Ordering::SeqCst) == 1
            && d.requests
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.0 == "DELETE")
                .count()
                == 2
    })
    .await;
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(
        f.data
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.0 == "DELETE")
            .count(),
        2
    );
}

#[tokio::test]
async fn startup_metadata_success_does_not_replenish_live_queue_attempts() {
    let f = Fixture::new(true, false, false).await;
    f.data.expire_lists.store(1, Ordering::SeqCst);
    f.data.always_notification_404.store(true, Ordering::SeqCst);
    let owner = f
        .owner(
            Some(HttpSessionRecoveryPolicy {
                max_attempts: 1,
                ..policy()
            }),
            true,
        )
        .await;
    f.until(|d| d.provider_drops.load(Ordering::SeqCst) == 1)
        .await;
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 2);
    assert!(f.data.calls.lock().unwrap().is_empty());
    owner.close().await.unwrap();
}

#[tokio::test]
async fn live_recovery_inherits_the_initial_observation_deadline() {
    let f = Fixture::new(true, false, false).await;
    f.data.expire_lists.store(1, Ordering::SeqCst);
    f.data.hold_recovery_list.store(true, Ordering::SeqCst);
    f.data.notification_404.store(true, Ordering::SeqCst);
    f.data.block_provider_on.store(3, Ordering::SeqCst);
    let (owner, ()) = tokio::join!(
        f.owner(
            Some(HttpSessionRecoveryPolicy {
                timeout: Duration::from_millis(300),
                ..policy()
            }),
            true
        ),
        async {
            f.data.list_entered.notified().await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            f.data.hold_recovery_list.store(false, Ordering::SeqCst);
            f.data.release_list.notify_one();
        }
    );
    let start = Instant::now();
    f.data.provider_entered.notified().await;
    let waiting = owner.execute("echo", "--message never").unwrap();
    assert_eq!(waiting.await.err().unwrap().code, "TransportClosed");
    owner.close().await.unwrap();
    assert!(start.elapsed() < Duration::from_millis(220));
    assert_eq!(f.data.resolves.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn dropping_queued_startup_interrupts_each_recovery_phase() {
    for phase in ["delete", "provider", "metadata"] {
        let f = Fixture::new(true, false, false).await;
        f.data.expire_lists.store(1, Ordering::SeqCst);
        if phase == "delete" {
            f.data.hold_delete.store(true, Ordering::SeqCst);
        }
        if phase == "provider" {
            f.data.block_provider_on.store(2, Ordering::SeqCst);
        }
        if phase == "metadata" {
            f.data.hold_recovery_list.store(true, Ordering::SeqCst);
        }
        let config = f.config.clone();
        let data = f.data.clone();
        let task = tokio::spawn(async move {
            QueuedRemote::spawn_with(config, QueueOptions::default(), move || {
                let guard = CallbackGuard(data.clone());
                Ok(Client::new()?
                    .with_credential_provider(Provider(data))
                    .with_http_session_recovery(policy())?
                    .with_event_handler(move |_| {
                        let _ = &guard;
                    }))
            })
            .await
        });
        match phase {
            "delete" => f.data.delete_entered.notified().await,
            "provider" => f.data.provider_entered.notified().await,
            _ => f.data.list_entered.notified().await,
        }
        // Existing DELETE stays held; a final cleanup request can complete.
        f.data.hold_delete.store(false, Ordering::SeqCst);
        let start = Instant::now();
        task.abort();
        assert!(task.await.err().unwrap().is_cancelled());
        f.until(|d| {
            d.provider_drops.load(Ordering::SeqCst) == 1
                && d.callback_drops.load(Ordering::SeqCst) == 1
        })
        .await;
        assert!(start.elapsed() < Duration::from_secs(1));
        f.data.release_delete.notify_waiters();
        f.data.release_list.notify_waiters();
        assert!(f.data.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn mutable_next_operation_recovers_once_and_rejects_old_typed_bindings() {
    for auto in [false, true] {
        let f = Fixture::new(true, auto, false).await;
        let mut remote = f
            .config
            .connect(f.client(Some(policy())), None)
            .await
            .unwrap();
        let old = remote.tool("echo").unwrap();
        let e = remote
            .call_json(&old, json!({"message":"expire"}))
            .await
            .unwrap_err();
        assert_eq!(e.code, "SessionExpired");
        assert_eq!(e.execution_certainty, ExecutionCertainty::Unknown);
        assert_eq!(
            f.data.initialized.load(Ordering::SeqCst),
            1,
            "failed call triggered hidden retry"
        );
        f.data.integer.store(true, Ordering::SeqCst);
        f.data.second.store(true, Ordering::SeqCst);
        assert_eq!(
            execution(remote.execute("echo", "--message 42").await.unwrap()),
            42
        );
        assert!(!remote.is_suspended());
        assert_eq!(f.data.initialized.load(Ordering::SeqCst), 2);
        assert_eq!(f.data.calls.lock().unwrap().len(), 2);
        let e = remote
            .call_json(&old, json!({"message":"never-old"}))
            .await
            .unwrap_err();
        assert_eq!(e.code, "BindingStale");
        assert_eq!(e.execution_certainty, ExecutionCertainty::NotSent);
        let rows = f.data.requests.lock().unwrap().clone();
        let deleted = rows.iter().position(|r| r.0 == "DELETE").unwrap();
        let initialized = rows.iter().rposition(|r| r.0 == "initialize").unwrap();
        assert!(deleted < initialized);
        assert_eq!(rows[deleted].2, "Bearer first");
        assert_eq!(rows[initialized].2, "Bearer second");
        remote.close().await.unwrap();
    }
}

#[tokio::test]
async fn mutable_unsent_metadata_expiry_builds_the_tool_arguments_only_after_renewal() {
    use std::ops::ControlFlow;
    let f = Fixture::new(true, false, false).await;
    let client = f.client(Some(policy())).with_catalog_ttl(Duration::ZERO);
    let mut remote = f.config.connect(client, None).await.unwrap();
    f.data.expire_next_list.store(true, Ordering::SeqCst);
    let mut builds = 0;
    let result = remote
        .call_tool_with("echo", |binding| {
            builds += 1;
            Ok(ControlFlow::<(), _>::Continue(
                binding.args_from(json!({"message":"after"}))?,
            ))
        })
        .await
        .unwrap();
    let ControlFlow::Continue(outcome) = result else {
        panic!()
    };
    assert_eq!(message(outcome), "after");
    assert_eq!(builds, 1);
    assert_eq!(f.data.calls.lock().unwrap().len(), 1);
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 2);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn mutable_metadata_success_does_not_replenish_rounds_but_a_tool_result_does() {
    let f = Fixture::new(true, false, false).await;
    let mut remote = f
        .config
        .connect(
            f.client(Some(HttpSessionRecoveryPolicy {
                max_attempts: 1,
                ..policy()
            })),
            None,
        )
        .await
        .unwrap();
    let old = remote.tool("echo").unwrap();
    remote
        .call_json(&old, json!({"message":"expire"}))
        .await
        .unwrap_err();
    let b = remote.fresh_tool("echo").await.unwrap();
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 2);
    remote
        .call_json(&b, json!({"message":"expire"}))
        .await
        .unwrap_err();
    assert_eq!(
        remote
            .execute("echo", "--message never")
            .await
            .err()
            .unwrap()
            .code,
        "ReconnectExhausted"
    );
    assert!(remote.is_suspended());
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 2);
    remote.reconnect().await.unwrap();
    assert_eq!(
        execution(remote.execute("echo", "--message after").await.unwrap()),
        "after"
    );
    let b = remote.tool("echo").unwrap();
    remote
        .call_json(&b, json!({"message":"expire"}))
        .await
        .unwrap_err();
    assert_eq!(
        execution(remote.execute("echo", "--message after").await.unwrap()),
        "after"
    );
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 4);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn mutable_recovery_cancellation_and_drop_suspend_until_explicit_reconnect() {
    for dropped in [false, true] {
        let f = Fixture::new(true, false, false).await;
        let mut remote = f
            .config
            .connect(f.client(Some(policy())), None)
            .await
            .unwrap();
        let b = remote.tool("echo").unwrap();
        remote
            .call_json(&b, json!({"message":"expire"}))
            .await
            .unwrap_err();
        f.data.block_provider.store(true, Ordering::SeqCst);
        let cancel = remote.cancellation_handle();
        {
            let operation = remote.execute("echo", "--message never");
            tokio::pin!(operation);
            tokio::select! {result=&mut operation=>panic!("unexpected recovery result: {}",result.is_ok()),_=f.data.provider_entered.notified()=>{}}
            if !dropped {
                cancel.cancel();
                assert_eq!(operation.await.err().unwrap().code, "Cancelled");
            }
        }
        assert!(remote.is_suspended());
        assert_eq!(
            remote
                .execute("echo", "--message never-suspended")
                .await
                .err()
                .unwrap()
                .code,
            "TransportClosed"
        );
        remote.reconnect().await.unwrap();
        assert_eq!(
            execution(remote.execute("echo", "--message after").await.unwrap()),
            "after"
        );
        assert_eq!(f.data.calls.lock().unwrap().len(), 2);
        remote.close().await.unwrap();
    }
}

#[tokio::test]
async fn mutable_caller_timeout_covers_metadata_and_the_first_new_tool_dispatch() {
    let f = Fixture::new(true, false, false).await;
    let mut remote = f
        .config
        .connect(
            f.client(Some(policy()))
                .with_timeout(Duration::from_millis(240)),
            None,
        )
        .await
        .unwrap();
    let b = remote.tool("echo").unwrap();
    remote
        .call_json(&b, json!({"message":"expire"}))
        .await
        .unwrap_err();
    f.data.hold_recovery_list.store(true, Ordering::SeqCst);
    f.data.block_peer.store(true, Ordering::SeqCst);
    let start = Instant::now();
    let (result, ()) = tokio::join!(remote.execute("echo", "--message peer"), async {
        f.data.list_entered.notified().await;
        tokio::time::sleep(Duration::from_millis(160)).await;
        f.data.hold_recovery_list.store(false, Ordering::SeqCst);
        f.data.release_list.notify_one();
    });
    let e = result.err().unwrap();
    assert_eq!(e.code, "Timeout");
    assert_eq!(e.execution_certainty, ExecutionCertainty::Unknown);
    assert!(start.elapsed() < Duration::from_millis(340));
    assert_eq!(f.data.calls.lock().unwrap().len(), 2);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn mutable_retry_backoff_and_prepared_catalog_share_the_original_cancellation() {
    use mcpx_core::Driver;
    let f = Fixture::new(true, false, false).await;
    let mut remote = f
        .config
        .connect(
            f.client(Some(HttpSessionRecoveryPolicy {
                max_attempts: 3,
                backoff: Duration::from_secs(30),
                timeout: Duration::from_secs(45),
            })),
            None,
        )
        .await
        .unwrap();
    let b = remote.tool("echo").unwrap();
    remote
        .call_json(&b, json!({"message":"expire"}))
        .await
        .unwrap_err();
    f.data.expire_next_list.store(true, Ordering::SeqCst);
    let cancel = remote.cancellation_handle();
    let (result, ()) = tokio::join!(remote.fresh_tool("echo"), async {
        f.until(|d| {
            d.requests
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.0 == "tools/list")
                .count()
                == 2
        })
        .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancel.cancel();
    });
    assert_eq!(result.err().unwrap().code, "Cancelled");
    assert!(remote.is_suspended());
    remote.reconnect().await.unwrap();
    assert_eq!(
        execution(remote.execute("echo", "--message after").await.unwrap()),
        "after"
    );
    let b = remote.tool("echo").unwrap();
    remote
        .call_json(&b, json!({"message":"expire"}))
        .await
        .unwrap_err();
    let context = remote.driver_mut().prepare().await.unwrap();
    let name = remote
        .with_prepared_catalog(context, |m| {
            Ok(m.catalog().tools().next().unwrap().name.clone())
        })
        .await
        .unwrap();
    assert_eq!(name, "echo");
    remote.close().await.unwrap();
}

#[tokio::test]
async fn mutable_disabled_modern_stateless_and_non_session_errors_do_not_renew() {
    for (legacy, stateless, enabled, status) in [
        (true, false, false, 404),
        (false, false, true, 404),
        (true, true, true, 404),
        (true, false, true, 401),
        (true, false, true, 503),
    ] {
        let f = Fixture::new(legacy, false, stateless).await;
        f.data.expire_status.store(status, Ordering::SeqCst);
        let mut remote = f
            .config
            .connect(f.client(enabled.then(policy)), None)
            .await
            .unwrap();
        let b = remote.tool("echo").unwrap();
        remote
            .call_json(&b, json!({"message":"expire"}))
            .await
            .unwrap_err();
        assert_eq!(
            execution(remote.execute("echo", "--message after").await.unwrap()),
            "after"
        );
        assert_eq!(
            f.data.initialized.load(Ordering::SeqCst),
            usize::from(legacy)
        );
        remote.close().await.unwrap();
    }
    let f = Fixture::new(true, false, false).await;
    let mut remote = f
        .config
        .connect(f.client(Some(policy())), None)
        .await
        .unwrap();
    f.data.provider_error.store(true, Ordering::SeqCst);
    assert_eq!(
        remote
            .execute("echo", "--message never")
            .await
            .err()
            .unwrap()
            .code,
        "SessionExpired"
    );
    assert_eq!(
        execution(remote.execute("echo", "--message after").await.unwrap()),
        "after"
    );
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 1);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn mutable_recovery_phase_failures_leave_new_business_not_sent() {
    for phase in ["delete", "provider", "metadata", "reject"] {
        let f = Fixture::new(true, false, false).await;
        let mut remote = f
            .config
            .connect(
                f.client(Some(HttpSessionRecoveryPolicy {
                    timeout: Duration::from_millis(130),
                    ..policy()
                })),
                None,
            )
            .await
            .unwrap();
        let b = remote.tool("echo").unwrap();
        remote
            .call_json(&b, json!({"message":"expire"}))
            .await
            .unwrap_err();
        if phase == "delete" {
            f.data.hold_delete.store(true, Ordering::SeqCst);
        }
        if phase == "provider" {
            f.data.block_provider.store(true, Ordering::SeqCst);
        }
        if phase == "metadata" {
            f.data.hold_recovery_list.store(true, Ordering::SeqCst);
        }
        if phase == "reject" {
            f.data.fail_deletes.store(1, Ordering::SeqCst);
        }
        let e = remote
            .execute("echo", "--message never")
            .await
            .err()
            .unwrap();
        assert_eq!(
            e.code,
            if phase == "reject" {
                "HttpError"
            } else {
                "Timeout"
            }
        );
        assert_eq!(e.execution_certainty, ExecutionCertainty::NotSent);
        assert!(remote.is_suspended());
        assert_eq!(f.data.calls.lock().unwrap().len(), 1);
        f.data.hold_delete.store(false, Ordering::SeqCst);
        f.data.hold_recovery_list.store(false, Ordering::SeqCst);
        f.data.release_delete.notify_waiters();
        f.data.release_list.notify_waiters();
        remote.reconnect().await.unwrap();
        assert_eq!(
            execution(remote.execute("echo", "--message after").await.unwrap()),
            "after"
        );
        remote.close().await.unwrap();
    }
}

#[tokio::test]
async fn mutable_startup_and_later_recovery_share_attempts() {
    let f = Fixture::new(true, false, false).await;
    f.data.expire_lists.store(1, Ordering::SeqCst);
    let mut remote = f
        .config
        .connect(
            f.client(Some(HttpSessionRecoveryPolicy {
                max_attempts: 1,
                ..policy()
            })),
            None,
        )
        .await
        .unwrap();
    let b = remote.tool("echo").unwrap();
    remote
        .call_json(&b, json!({"message":"expire"}))
        .await
        .unwrap_err();
    assert_eq!(
        remote.fresh_tool("echo").await.err().unwrap().code,
        "ReconnectExhausted"
    );
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 2);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn mutable_healthy_idle_resets_rounds_without_a_successful_tool() {
    let f = Fixture::new(true, false, false).await;
    let mut remote = f
        .config
        .connect(
            f.client(Some(HttpSessionRecoveryPolicy {
                max_attempts: 1,
                timeout: Duration::from_millis(120),
                ..policy()
            })),
            None,
        )
        .await
        .unwrap();
    let b = remote.tool("echo").unwrap();
    remote
        .call_json(&b, json!({"message":"expire"}))
        .await
        .unwrap_err();
    let b = remote.fresh_tool("echo").await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    remote
        .call_json(&b, json!({"message":"expire"}))
        .await
        .unwrap_err();
    remote.fresh_tool("echo").await.unwrap();
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 3);
    assert_eq!(f.data.calls.lock().unwrap().len(), 2);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn mutable_low_level_concurrent_work_blocks_reset_and_old_results_do_not_reset_budget() {
    use mcpx_core::Driver;
    let f = Fixture::new(true, false, false).await;
    let mut remote = f
        .config
        .connect(
            f.client(Some(HttpSessionRecoveryPolicy {
                max_attempts: 1,
                ..policy()
            })),
            None,
        )
        .await
        .unwrap();
    let b = remote.tool("echo").unwrap();
    remote
        .call_json(&b, json!({"message":"expire"}))
        .await
        .unwrap_err();
    let b = remote.fresh_tool("echo").await.unwrap();
    let expired = remote
        .begin_concurrent_call(&b, &b.args_from(json!({"message":"expire"})).unwrap())
        .await
        .unwrap();
    let peer = remote
        .begin_concurrent_call(&b, &b.args_from(json!({"message":"after"})).unwrap())
        .await
        .unwrap();
    let actions = remote.host_runtime_mut().take_actions();
    assert_eq!(actions.len(), 2);
    let mut actions = actions.into_iter();
    {
        let (rt, driver) = remote.host_parts_mut();
        assert_eq!(
            driver
                .execute(actions.next().unwrap(), rt)
                .await
                .unwrap_err()
                .code,
            "SessionExpired"
        );
    }
    assert_eq!(remote.fresh_tool("echo").await.err().unwrap().code, "Busy");
    assert!(!remote.is_suspended());
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 2);
    {
        let (rt, driver) = remote.host_parts_mut();
        driver.execute(actions.next().unwrap(), rt).await.unwrap();
    }
    assert_eq!(
        message(remote.take_concurrent_completion(peer).unwrap().unwrap()),
        "after"
    );
    assert_eq!(
        remote.fresh_tool("echo").await.err().unwrap().code,
        "ReconnectExhausted"
    );
    let e = remote
        .take_concurrent_completion(expired)
        .unwrap()
        .unwrap_err();
    assert_eq!(e.code, "SessionExpired");
    assert_eq!(e.operation_id, Some(expired));
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 2);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn mutable_cancel_before_recovery_does_not_change_the_epoch() {
    let f = Fixture::new(true, false, false).await;
    let mut remote = f
        .config
        .connect(f.client(Some(policy())), None)
        .await
        .unwrap();
    let b = remote.tool("echo").unwrap();
    remote
        .call_json(&b, json!({"message":"expire"}))
        .await
        .unwrap_err();
    let revision = remote.catalog_revision();
    remote.cancellation_handle().cancel();
    assert_eq!(
        remote.fresh_tool("echo").await.err().unwrap().code,
        "Cancelled"
    );
    assert!(!remote.is_suspended());
    assert_eq!(remote.catalog_revision(), revision);
    assert_eq!(
        execution(remote.execute("echo", "--message after").await.unwrap()),
        "after"
    );
    remote.close().await.unwrap();
}

#[tokio::test]
async fn reading_an_old_retained_result_after_renewal_does_not_replenish_the_burst() {
    use mcpx_core::Driver;
    let f = Fixture::new(true, false, false).await;
    let mut remote = f
        .config
        .connect(f.client(Some(policy())), None)
        .await
        .unwrap();
    let b = remote.tool("echo").unwrap();
    let old = remote
        .begin_concurrent_call(&b, &b.args_from(json!({"message":"retained"})).unwrap())
        .await
        .unwrap();
    let action = remote.host_runtime_mut().take_actions().pop().unwrap();
    {
        let (rt, driver) = remote.host_parts_mut();
        driver.execute(action, rt).await.unwrap();
    }
    remote
        .call_json(&b, json!({"message":"expire"}))
        .await
        .unwrap_err();
    let b = remote.fresh_tool("echo").await.unwrap();
    assert_eq!(
        message(remote.take_concurrent_completion(old).unwrap().unwrap()),
        "retained"
    );
    remote
        .call_json(&b, json!({"message":"expire"}))
        .await
        .unwrap_err();
    let b = remote.fresh_tool("echo").await.unwrap();
    remote
        .call_json(&b, json!({"message":"expire"}))
        .await
        .unwrap_err();
    assert_eq!(
        remote.fresh_tool("echo").await.err().unwrap().code,
        "ReconnectExhausted"
    );
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 3);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn prepared_mutable_admission_refreshes_context_and_keeps_typed_handles_stale() {
    use mcpx_core::Driver;
    use std::ops::ControlFlow;
    let f = Fixture::new(true, false, false).await;
    let mut remote = f
        .config
        .connect(f.client(Some(policy())), None)
        .await
        .unwrap();
    let old = remote.tool("echo").unwrap();
    remote
        .call_json(&old, json!({"message":"expire"}))
        .await
        .unwrap_err();
    let context = remote.driver_mut().prepare().await.unwrap();
    let args = old.args_from(json!({"message":"never-old"})).unwrap();
    let e = remote
        .admit_prepared_call(context, &old, &args)
        .await
        .unwrap_err();
    assert_eq!(e.code, "BindingStale");
    assert_eq!(e.execution_certainty, ExecutionCertainty::NotSent);
    let b = remote.tool("echo").unwrap();
    remote
        .call_json(&b, json!({"message":"expire"}))
        .await
        .unwrap_err();
    f.data.integer.store(true, Ordering::SeqCst);
    let context = remote.driver_mut().prepare().await.unwrap();
    let mut builds = 0;
    let ControlFlow::Continue(operation) = remote
        .admit_prepared_tool(context, "echo", |b| {
            builds += 1;
            Ok(ControlFlow::<(), _>::Continue(
                b.args_from(json!({"message":42}))?,
            ))
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(builds, 1);
    let action = remote.host_runtime_mut().take_actions().pop().unwrap();
    {
        let (rt, driver) = remote.host_parts_mut();
        driver.execute(action, rt).await.unwrap();
    }
    assert_eq!(
        message(
            remote
                .take_concurrent_completion(operation)
                .unwrap()
                .unwrap()
        ),
        42
    );
    assert_eq!(f.data.calls.lock().unwrap().len(), 3);
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 3);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn unconsumed_serial_result_blocks_renewal_without_suspending_the_owner() {
    use mcpx_core::Driver;
    let f = Fixture::new(true, false, false).await;
    let mut remote = f
        .config
        .connect(f.client(Some(policy())), None)
        .await
        .unwrap();
    let binding = remote.tool("echo").unwrap();
    remote.driver_mut().prepare().await.unwrap();
    remote
        .host_runtime_mut()
        .call_bound(
            &binding,
            &binding.args_from(json!({"message":"retained"})).unwrap(),
        )
        .unwrap();
    let action = remote.host_runtime_mut().take_actions().pop().unwrap();
    {
        let (runtime, driver) = remote.host_parts_mut();
        driver.execute(action, runtime).await.unwrap();
        driver.finish();
    }
    f.data.notification_404.store(true, Ordering::SeqCst);
    let request = remote.host_runtime_mut().start_notifications().unwrap();
    let mut get = reqwest::Client::new().get(&request.url);
    for (name, value) in &request.headers {
        get = get.header(name, value);
    }
    let response = get.send().await.unwrap();
    assert_eq!(response.status().as_u16(), 404);
    {
        let (runtime, driver) = remote.host_parts_mut();
        runtime.advance_time(driver.now_ms()).unwrap();
        assert_eq!(
            runtime
                .begin_notifications(request.resource_id, 404, "")
                .unwrap_err()
                .code,
            "SessionExpired"
        );
    }
    let revision = remote.catalog_revision();
    assert_eq!(remote.fresh_tool("echo").await.err().unwrap().code, "Busy");
    assert!(!remote.is_suspended());
    assert_eq!(remote.catalog_revision(), revision);
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 1);
    assert_eq!(
        message(remote.host_runtime_mut().take_outcome().unwrap()),
        "retained"
    );
    remote.fresh_tool("echo").await.unwrap();
    assert_eq!(f.data.initialized.load(Ordering::SeqCst), 2);
    assert_eq!(f.data.calls.lock().unwrap().len(), 1);
    remote.close().await.unwrap();
}
