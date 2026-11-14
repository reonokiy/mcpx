use mcpx_core::{
    CallOutcome, Credential, CredentialFuture, CredentialProvider, CredentialRequest, Dialect,
    Driver,
};
use mcpx_native::Client;
use serde_json::{Value, json};
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::{JoinHandle, JoinSet},
};

#[derive(Default)]
struct State {
    calls: AtomicUsize,
    lists: AtomicUsize,
    deletes: AtomicUsize,
    fail_delete: AtomicBool,
    deletion_headers: Mutex<Vec<String>>,
    integer_schema: AtomicBool,
    zero_ttl: AtomicBool,
    slow_initialize: AtomicBool,
    started: Notify,
    disconnected: Notify,
    requests: Mutex<Vec<(Value, String)>>,
}
struct Server {
    endpoint: String,
    state: Arc<State>,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn server() -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
    let state = Arc::new(State::default());
    let shared = state.clone();
    let task = tokio::spawn(async move {
        let mut handlers = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {let (socket, _) = accepted.unwrap(); let state = shared.clone(); handlers.spawn(async move {let _ = serve(socket, state).await;});}
                Some(_) = handlers.join_next(), if !handlers.is_empty() => {}
            }
        }
    });
    Server {
        endpoint,
        state,
        task,
    }
}

async fn serve(mut socket: TcpStream, state: Arc<State>) -> std::io::Result<()> {
    let mut bytes = Vec::new();
    let (end, length, headers) = loop {
        let mut part = [0; 4096];
        let count = socket.read(&mut part).await?;
        if count == 0 {
            return Ok(());
        }
        bytes.extend_from_slice(&part[..count]);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]).into_owned();
            let length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .map(|v| v.parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            break (end + 4, length, headers);
        }
    };
    if headers.starts_with("DELETE ") {
        state.deletes.fetch_add(1, Ordering::SeqCst);
        state.deletion_headers.lock().unwrap().push(headers.clone());
        let status = if state.fail_delete.swap(false, Ordering::SeqCst) {
            b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n".as_slice()
        } else {
            b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n".as_slice()
        };
        socket.write_all(status).await?;
        return Ok(());
    }
    while bytes.len() < end + length {
        let mut part = [0; 4096];
        let count = socket.read(&mut part).await?;
        if count == 0 {
            return Ok(());
        }
        bytes.extend_from_slice(&part[..count]);
    }
    let request: Value = serde_json::from_slice(&bytes[end..end + length]).unwrap();
    let bearer = headers
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(key, _)| key.eq_ignore_ascii_case("authorization"))
                .map(|(_, value)| value.trim().to_string())
        })
        .unwrap_or_default();
    state
        .requests
        .lock()
        .unwrap()
        .push((request.clone(), bearer));
    let method = request["method"].as_str().unwrap();
    if method == "initialize" && state.slow_initialize.load(Ordering::SeqCst) {
        state.started.notify_one();
        let mut byte = [0];
        let _ = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte)).await;
        return Ok(());
    }
    if matches!(
        method,
        "notifications/initialized" | "notifications/cancelled"
    ) {
        socket
            .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await?;
        return Ok(());
    }
    let result = match method {
        "server/discover" => json!({"supportedVersions":["2026-07-28"]}),
        "initialize" => json!({"protocolVersion":"2025-11-25","capabilities":{"tools":{}}}),
        "tools/list" => {
            state.lists.fetch_add(1, Ordering::SeqCst);
            json!({"ttlMs":if state.zero_ttl.load(Ordering::SeqCst) {0} else {60000},"tools":[{"name":"echo","inputSchema":{"type":"object","properties":{"message":{"type":if state.integer_schema.load(Ordering::SeqCst) {"integer"} else {"string"}}},"required":["message"]}}]})
        }
        "tools/call" => {
            state.calls.fetch_add(1, Ordering::SeqCst);
            if request["params"]["arguments"]["message"] == "slow" {
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await?;
                let event = "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progress\":1}}\n\n";
                socket
                    .write_all(format!("{:x}\r\n{event}\r\n", event.len()).as_bytes())
                    .await?;
                state.started.notify_one();
                let mut byte = [0];
                if matches!(
                    tokio::time::timeout(Duration::from_secs(5), socket.read(&mut byte)).await,
                    Ok(Ok(0))
                ) {
                    state.disconnected.notify_one();
                }
                return Ok(());
            }
            json!({"content":[],"structuredContent":request["params"]["arguments"]})
        }
        _ => panic!("unexpected fixture method"),
    };
    let body = json!({"jsonrpc":"2.0","id":request["id"],"result":result}).to_string();
    let session = if method == "initialize" {
        "Mcp-Session-Id: fixture-session\r\n"
    } else {
        ""
    };
    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n{session}Content-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_closes_modern_stream_and_old_handle_cannot_cancel_next_call() {
    let server = server().await;
    let mut remote = Client::new()
        .unwrap()
        .connect(&server.endpoint, Dialect::Modern)
        .await
        .unwrap();
    let binding = remote.tool("echo").unwrap();
    let args = binding
        .args()
        .set("message", "slow")
        .unwrap()
        .finish()
        .unwrap();
    let cancel = remote.cancellation_handle();
    let retained = cancel.clone();
    let cancel_task = async {
        server.state.started.notified().await;
        cancel.cancel();
    };
    let (result, ()) = tokio::join!(remote.call(&binding, &args), cancel_task);
    assert_eq!(result.unwrap_err().code, "Cancelled");
    tokio::time::timeout(Duration::from_secs(1), server.state.disconnected.notified())
        .await
        .unwrap();
    retained.cancel();
    let CallOutcome::Completed(response) = remote
        .call_json(&binding, json!({"message":"fast"}))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(response.raw["structuredContent"]["message"], "fast");
    assert_eq!(server.state.calls.load(Ordering::SeqCst), 2);
    assert!(
        !server
            .state
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|(r, _)| r["method"] == "notifications/cancelled")
    );
    assert_eq!(
        remote
            .drain_events()
            .events
            .iter()
            .filter(|e| e.kind == "cancelled")
            .count(),
        1
    );
    remote.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn legacy_cancellation_notifies_request_and_close_deletes_session() {
    let server = server().await;
    let mut remote = Client::new()
        .unwrap()
        .connect(&server.endpoint, Dialect::Legacy)
        .await
        .unwrap();
    let binding = remote.tool("echo").unwrap();
    let args = binding
        .args()
        .set("message", "slow")
        .unwrap()
        .finish()
        .unwrap();
    let cancel = remote.cancellation_handle();
    let (result, ()) = tokio::join!(remote.call(&binding, &args), async {
        server.state.started.notified().await;
        cancel.cancel();
    });
    assert_eq!(result.unwrap_err().code, "Cancelled");
    {
        let requests = server.state.requests.lock().unwrap();
        let call = requests
            .iter()
            .find(|(r, _)| r["method"] == "tools/call")
            .unwrap();
        let notification = requests
            .iter()
            .find(|(r, _)| r["method"] == "notifications/cancelled")
            .unwrap();
        assert_eq!(notification.0["params"]["requestId"], call.0["id"]);
    }
    remote.close().await.unwrap();
    assert_eq!(server.state.deletes.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn pre_cancelled_operation_never_reaches_the_server_and_does_not_poison_reuse() {
    let server = server().await;
    let mut remote = Client::new()
        .unwrap()
        .connect(&server.endpoint, Dialect::Modern)
        .await
        .unwrap();
    let binding = remote.tool("echo").unwrap();
    remote.cancellation_handle().cancel();
    assert_eq!(
        remote
            .call_json(&binding, json!({"message":"not sent"}))
            .await
            .unwrap_err()
            .code,
        "Cancelled"
    );
    assert_eq!(server.state.calls.load(Ordering::SeqCst), 0);
    remote
        .call_json(&binding, json!({"message":"sent"}))
        .await
        .unwrap();
    assert_eq!(server.state.calls.load(Ordering::SeqCst), 1);
}

struct MutableProvider(Rc<RefCell<Credential>>);
impl CredentialProvider for MutableProvider {
    fn resolve<'a>(&'a mut self, _request: CredentialRequest<'a>) -> CredentialFuture<'a> {
        Box::pin(async move { Ok(self.0.borrow().clone()) })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn old_session_cleanup_uses_its_original_credential_after_identity_invalidation() {
    for reconnect in [false, true] {
        let server = server().await;
        let credential = Rc::new(RefCell::new(
            Credential::bearer("fake-A".into(), "user-A".into()).unwrap(),
        ));
        let mut remote = Client::new()
            .unwrap()
            .with_credential_provider(MutableProvider(credential.clone()))
            .connect(&server.endpoint, Dialect::Legacy)
            .await
            .unwrap();
        let binding = remote.tool("echo").unwrap();
        *credential.borrow_mut() = Credential::bearer("fake-B".into(), "user-B".into()).unwrap();
        assert_eq!(
            remote
                .call_json(&binding, json!({"message":"stale"}))
                .await
                .unwrap_err()
                .code,
            "BindingStale"
        );
        assert!(remote.machine().session_id().is_none());
        if reconnect {
            remote.refresh().await.unwrap();
        }
        remote.close().await.unwrap();
        let headers = server.state.deletion_headers.lock().unwrap();
        assert_eq!(headers.len(), if reconnect { 2 } else { 1 });
        let tokens: Vec<_> = headers
            .iter()
            .map(|headers| {
                headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(name, _)| name.eq_ignore_ascii_case("authorization"))
                            .map(|(_, value)| value.trim())
                    })
                    .unwrap()
            })
            .collect();
        assert_eq!(tokens[0], "Bearer fake-A");
        if reconnect {
            assert_eq!(tokens[1], "Bearer fake-B");
        }
    }
}

#[tokio::test]
async fn concurrent_legacy_http_cancellation_preserves_other_calls_and_the_session() {
    let server = server().await;
    let remote = mcpx_native::QueuedRemote::connect(
        mcpx_native::ServerConfig::Http {
            url: server.endpoint.clone(),
            protocol: mcpx_native::ProtocolMode::Legacy,
            credential: None,
        },
        mcpx_native::QueueOptions::default(),
    )
    .await
    .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let slow = binding.submit_json(json!({"message":"slow"})).unwrap();
    let fast = binding.submit_json(json!({"message":"fast"})).unwrap();
    server.state.started.notified().await;
    assert!(matches!(fast.await.unwrap(), CallOutcome::Completed(_)));
    slow.cancel();
    assert_eq!(slow.await.unwrap_err().code, "Cancelled");
    tokio::time::timeout(Duration::from_secs(1), server.state.disconnected.notified())
        .await
        .unwrap();
    assert_eq!(server.state.deletes.load(Ordering::SeqCst), 0);
    {
        let requests = server.state.requests.lock().unwrap();
        let slow_id = requests
            .iter()
            .find(|(rpc, _)| {
                rpc["method"] == "tools/call" && rpc["params"]["arguments"]["message"] == "slow"
            })
            .unwrap()
            .0["id"]
            .clone();
        let cancelled = requests
            .iter()
            .find(|(rpc, _)| rpc["method"] == "notifications/cancelled")
            .unwrap();
        assert_eq!(cancelled.0["params"]["requestId"], slow_id);
    }
    remote.close().await.unwrap();
    assert_eq!(server.state.deletes.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn admitted_concurrent_request_keeps_its_credential_after_an_identity_change() {
    let server = server().await;
    let credential = Rc::new(RefCell::new(
        Credential::bearer("fake-A".into(), "user-A".into()).unwrap(),
    ));
    let mut remote = Client::new()
        .unwrap()
        .with_credential_provider(MutableProvider(credential.clone()))
        .connect(&server.endpoint, Dialect::Modern)
        .await
        .unwrap();
    let binding = remote.tool("echo").unwrap();
    let args = binding
        .validate_json(json!({"message":"admitted as A"}))
        .unwrap();
    let operation = remote.begin_concurrent_call(&binding, &args).await.unwrap();
    *credential.borrow_mut() = Credential::bearer("fake-B".into(), "user-B".into()).unwrap();
    assert_eq!(
        remote
            .begin_concurrent_call(&binding, &args)
            .await
            .unwrap_err()
            .code,
        "BindingStale"
    );
    let mut actions = remote.host_runtime_mut().take_actions();
    assert_eq!(actions.len(), 1);
    let (runtime, driver) = remote.host_parts_mut();
    driver.execute(actions.remove(0), runtime).await.unwrap();
    assert!(matches!(
        remote.take_concurrent_completion(operation),
        Some(Ok(CallOutcome::Completed(_)))
    ));
    assert_eq!(
        remote
            .begin_concurrent_call(&binding, &args)
            .await
            .unwrap_err()
            .code,
        "BindingStale"
    );
    remote.refresh().await.unwrap();
    let binding = remote.tool("echo").unwrap();
    remote
        .call_json(&binding, json!({"message":"new call as B"}))
        .await
        .unwrap();
    {
        let requests = server.state.requests.lock().unwrap();
        let calls: Vec<_> = requests
            .iter()
            .filter(|(request, _)| request["method"] == "tools/call")
            .collect();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].1, "Bearer fake-A");
        assert_eq!(calls[1].1, "Bearer fake-B");
    }
    remote.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn changed_authorization_clears_catalog_and_old_bindings_before_network_calls() {
    let server = server().await;
    let credential = Rc::new(RefCell::new(
        Credential::bearer("fake-A".into(), "user-A".into()).unwrap(),
    ));
    let mut remote = Client::new()
        .unwrap()
        .with_credential_provider(MutableProvider(credential.clone()))
        .connect(&server.endpoint, Dialect::Modern)
        .await
        .unwrap();
    let binding = remote.tool("echo").unwrap();
    *credential.borrow_mut() = Credential::bearer("fake-B".into(), "user-B".into()).unwrap();
    assert_eq!(
        remote
            .call_json(&binding, json!({"message":"old"}))
            .await
            .unwrap_err()
            .code,
        "BindingStale"
    );
    assert_eq!(server.state.requests.lock().unwrap().len(), 2);
    assert!(remote.tool("echo").is_err());
    remote.refresh().await.unwrap();
    let fresh = remote.tool("echo").unwrap();
    remote
        .call_json(&fresh, json!({"message":"new"}))
        .await
        .unwrap();
    let requests = server.state.requests.lock().unwrap();
    let calls: Vec<_> = requests
        .iter()
        .filter(|(r, _)| r["method"] == "tools/call")
        .collect();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].1, "Bearer fake-B");
}

#[tokio::test(flavor = "current_thread")]
async fn expired_catalog_refresh_rejects_old_schema_without_reinitializing() {
    let server = server().await;
    server.state.zero_ttl.store(true, Ordering::SeqCst);
    let mut remote = Client::new()
        .unwrap()
        .connect(&server.endpoint, Dialect::Modern)
        .await
        .unwrap();
    let binding = remote.tool("echo").unwrap();
    server.state.integer_schema.store(true, Ordering::SeqCst);
    assert_eq!(
        remote
            .call_json(&binding, json!({"message":"old"}))
            .await
            .unwrap_err()
            .code,
        "BindingStale"
    );
    assert_eq!(server.state.calls.load(Ordering::SeqCst), 0);
    let binding = remote.tool("echo").unwrap();
    remote
        .call_json(&binding, json!({"message":10}))
        .await
        .unwrap();
    assert_eq!(server.state.lists.load(Ordering::SeqCst), 3);
    assert_eq!(
        server
            .state
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(r, _)| r["method"] == "server/discover")
            .count(),
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn event_stream_preserves_terminal_records_and_applies_backpressure_before_io() {
    use mcpx_native::EventStreamItem;
    let server = server().await;
    let (client, mut events) = Client::new().unwrap().with_event_stream(4).unwrap();
    let mut remote = client
        .connect(&server.endpoint, Dialect::Modern)
        .await
        .unwrap();
    let binding = remote.tool("echo").unwrap();
    for _ in 0..2 {
        remote
            .call_json(&binding, json!({"message":"ok"}))
            .await
            .unwrap();
    }
    assert_eq!(
        remote
            .call_json(&binding, json!({"message":"not sent"}))
            .await
            .unwrap_err()
            .code,
        "ConsumerLagged"
    );
    assert_eq!(server.state.calls.load(Ordering::SeqCst), 2);
    let mut kinds = vec![];
    while let Some(item) = events.try_next() {
        let EventStreamItem::Event(event) = item else {
            panic!("terminal-only input cannot lose records")
        };
        kinds.push(event.kind);
    }
    assert_eq!(kinds, ["catalog_updated", "completed", "completed"]);
    remote
        .call_json(&binding, json!({"message":"resumed"}))
        .await
        .unwrap();
    remote.close().await.unwrap();
    assert!(matches!(
        events.next().await,
        Some(EventStreamItem::Event(_))
    ));
    assert!(events.next().await.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn asynchronous_event_consumer_is_send_and_terminates_on_close() {
    let server = server().await;
    let (client, mut events) = Client::new().unwrap().with_event_stream(16).unwrap();
    let consumer = tokio::spawn(async move {
        let mut count = 0;
        while events.next().await.is_some() {
            count += 1;
        }
        count
    });
    let mut remote = client
        .connect(&server.endpoint, Dialect::Modern)
        .await
        .unwrap();
    let binding = remote.tool("echo").unwrap();
    remote
        .call_json(&binding, json!({"message":"ok"}))
        .await
        .unwrap();
    remote.close().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), consumer)
            .await
            .unwrap()
            .unwrap(),
        2
    );
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_event_consumer_explicitly_disables_backpressure() {
    let server = server().await;
    let (client, events) = Client::new().unwrap().with_event_stream(4).unwrap();
    drop(events);
    let mut remote = client
        .connect(&server.endpoint, Dialect::Modern)
        .await
        .unwrap();
    let binding = remote.tool("echo").unwrap();
    for _ in 0..8 {
        remote
            .call_json(&binding, json!({"message":"ok"}))
            .await
            .unwrap();
    }
    assert_eq!(server.state.calls.load(Ordering::SeqCst), 8);
}

struct ReferenceProvider;
impl CredentialProvider for ReferenceProvider {
    fn resolve<'a>(&'a mut self, request: CredentialRequest<'a>) -> CredentialFuture<'a> {
        Box::pin(async move {
            assert_eq!(request.reference, Some("team/github"));
            Credential::bearer("fake-reference-token".into(), "reference-user".into())
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn named_config_credentials_are_resolved_by_the_supplied_provider() {
    let server = server().await;
    let config = mcpx_native::Config::parse(&format!(
        "[servers.named]\ntransport='http'\nurl='{}'\ncredential='team/github'",
        server.endpoint
    ))
    .unwrap();
    let client = Client::new()
        .unwrap()
        .with_credential_provider(ReferenceProvider);
    let mut remote = config
        .server("named")
        .unwrap()
        .connect(client, None)
        .await
        .unwrap();
    let binding = remote.tool("echo").unwrap();
    remote
        .call_json(&binding, json!({"message":"resolved"}))
        .await
        .unwrap();
    assert!(
        server
            .state
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|(_, token)| token == "Bearer fake-reference-token")
    );
    let error = config
        .server("named")
        .unwrap()
        .connect(Client::new().unwrap(), None)
        .await
        .err()
        .unwrap();
    assert_eq!(error.code, "CredentialUnavailable");
}

struct WaitingProvider {
    entered: Arc<Notify>,
}
impl CredentialProvider for WaitingProvider {
    fn resolve<'a>(&'a mut self, _request: CredentialRequest<'a>) -> CredentialFuture<'a> {
        Box::pin(async move {
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn connection_can_cancel_credential_resolution_before_any_network_request() {
    let server = server().await;
    let entered = Arc::new(Notify::new());
    let client = Client::new()
        .unwrap()
        .with_credential_provider(WaitingProvider {
            entered: entered.clone(),
        });
    let cancel = client.cancellation_handle();
    let (result, ()) = tokio::join!(client.connect(&server.endpoint, Dialect::Modern), async {
        entered.notified().await;
        cancel.cancel();
    });
    assert_eq!(result.err().unwrap().code, "Cancelled");
    assert!(server.state.requests.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_connection_never_sends_a_protocol_cancellation_for_initialize() {
    let server = server().await;
    server.state.slow_initialize.store(true, Ordering::SeqCst);
    let client = Client::new().unwrap();
    let cancel = client.cancellation_handle();
    let (result, ()) = tokio::join!(client.connect(&server.endpoint, Dialect::Legacy), async {
        server.state.started.notified().await;
        cancel.cancel();
    });
    assert_eq!(result.err().unwrap().code, "Cancelled");
    let requests = server.state.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].0["method"], "initialize");
}

#[tokio::test(flavor = "current_thread")]
async fn out_of_range_timeout_returns_a_configuration_error_before_io() {
    let server = server().await;
    let error = Client::new()
        .unwrap()
        .with_timeout(Duration::MAX)
        .connect(&server.endpoint, Dialect::Modern)
        .await
        .err()
        .unwrap();
    assert_eq!(error.code, "ConfigurationError");
    assert!(server.state.requests.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_http_reconnect_preserves_same_identity_resources_and_cleans_old_credentials() {
    let server = server().await;
    let credential = Rc::new(RefCell::new(
        Credential::bearer("fake-reconnect-A".into(), "owner-A".into()).unwrap(),
    ));
    let registry = mcpx_core::SchemaRegistry::new([(
        "https://schemas.example/reconnect".into(),
        json!({"type":"string"}),
    )])
    .unwrap();
    let mut remote = Client::new()
        .unwrap()
        .with_schema_registry(registry)
        .with_credential_provider(MutableProvider(credential.clone()))
        .connect(&server.endpoint, Dialect::Legacy)
        .await
        .unwrap();
    let old = remote.tool("echo").unwrap();
    remote
        .call_json(&old, json!({"message":"one"}))
        .await
        .unwrap();
    remote.reconnect().await.unwrap();
    assert_eq!(remote.schema_registry_stats().resources, 1);
    assert_eq!(
        remote
            .call_json(&old, json!({"message":"never"}))
            .await
            .unwrap_err()
            .code,
        "BindingStale"
    );
    let second = remote.tool("echo").unwrap();
    remote
        .call_json(&second, json!({"message":"two"}))
        .await
        .unwrap();
    *credential.borrow_mut() =
        Credential::bearer("fake-reconnect-B".into(), "owner-B".into()).unwrap();
    server.state.integer_schema.store(true, Ordering::SeqCst);
    remote.reconnect().await.unwrap();
    assert_eq!(remote.schema_registry_stats().resources, 0);
    let fresh = remote.tool("echo").unwrap();
    remote
        .call_json(&fresh, json!({"message":3}))
        .await
        .unwrap();
    remote.close().await.unwrap();
    assert_eq!(server.state.calls.load(Ordering::SeqCst), 3);
    let headers = server.state.deletion_headers.lock().unwrap();
    assert_eq!(headers.len(), 3);
    assert!(headers[0].contains("Bearer fake-reconnect-A"));
    assert!(headers[1].contains("Bearer fake-reconnect-A"));
    assert!(headers[2].contains("Bearer fake-reconnect-B"));
    let ids: Vec<_> = server
        .state
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(body, _)| body["id"].as_u64())
        .collect();
    assert_eq!(
        ids.len(),
        ids.iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
    );
}
#[tokio::test(flavor = "current_thread")]
async fn failed_native_session_cleanup_retries_and_releases_provider_without_reopening() {
    let server = server().await;
    let credential = Rc::new(RefCell::new(
        Credential::bearer("fake-cleanup".into(), "cleanup-owner".into()).unwrap(),
    ));
    let weak = Rc::downgrade(&credential);
    let mut remote = Client::new()
        .unwrap()
        .with_credential_provider(MutableProvider(credential.clone()))
        .connect(&server.endpoint, Dialect::Legacy)
        .await
        .unwrap();
    drop(credential);
    server.state.fail_delete.store(true, Ordering::SeqCst);
    assert_eq!(remote.close().await.unwrap_err().code, "HttpError");
    assert!(weak.upgrade().is_none());
    assert_eq!(
        remote.reconnect().await.unwrap_err().code,
        "TransportClosed"
    );
    remote.close().await.unwrap();
    remote.close().await.unwrap();
    assert_eq!(server.state.deletes.load(Ordering::SeqCst), 2);
}
