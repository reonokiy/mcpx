use axum::{Json, extract::State, response::IntoResponse};
use mcpx_core::{CallOutcome, Credential, CredentialFuture, CredentialProvider, CredentialRequest};
use mcpx_native::{
    Client, Execution, ProtocolMode, QueueOptions, QueuedBinding, QueuedRemote, ServerConfig,
};
use serde_json::{Value, json};
use std::{
    cell::Cell,
    collections::BTreeMap,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

fn serial_options() -> QueueOptions {
    QueueOptions {
        max_in_flight: 1,
        ..Default::default()
    }
}

async fn wait_calls(data: &Data, count: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let started = data.started.notified();
            if data.calls.lock().unwrap().len() >= count {
                break;
            }
            started.await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn default_http_queue_reaches_sixteen_requests_before_any_response_and_queues_the_seventeenth()
 {
    let server = server().await;
    let events = Arc::new(Mutex::new(Vec::<mcpx_core::Event>::new()));
    let sink = events.clone();
    let remote =
        QueuedRemote::spawn_with(server.config.clone(), QueueOptions::default(), move || {
            Ok(Client::new()?.with_event_handler(move |event| {
                let mcpx_native::EventStreamItem::Event(event) = event else {
                    panic!("unexpected core loss in fixture")
                };
                sink.lock().unwrap().push(event.clone());
            }))
        })
        .await
        .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let mut calls = Vec::new();
    let mut ids = std::collections::BTreeSet::new();
    for _ in 0..16 {
        let call = binding.submit(&args(&binding, "hold")).unwrap();
        ids.insert(call.id());
        calls.push(call);
    }
    let mut last = binding.submit(&args(&binding, "seventeenth")).unwrap();
    ids.insert(last.id());
    wait_calls(&server.data, 16).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(40), &mut last)
            .await
            .is_err()
    );
    assert_eq!(server.data.calls.lock().unwrap().len(), 16);
    server.data.release.notify_waiters();
    for call in calls {
        assert_eq!(message(call.await.unwrap()), "hold");
    }
    assert_eq!(message(last.await.unwrap()), "seventeenth");
    assert_eq!(server.data.calls.lock().unwrap().len(), 17);
    remote.close().await.unwrap();
    let events = events.lock().unwrap();
    let completed: Vec<_> = events
        .iter()
        .filter(|event| event.kind == "completed")
        .collect();
    assert_eq!(completed.len(), 17);
    assert_eq!(
        completed
            .iter()
            .filter_map(|event| event.queue_operation_id)
            .collect::<std::collections::BTreeSet<_>>(),
        ids
    );
}

#[tokio::test]
async fn parallel_http_cancel_timeout_and_bad_stream_are_isolated() {
    let server = server().await;
    let remote = QueuedRemote::connect(server.config.clone(), QueueOptions::default())
        .await
        .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let cancelled = binding.submit(&args(&binding, "hold")).unwrap();
    let expired = binding
        .submit_with_timeout(&args(&binding, "hold"), Duration::from_millis(80))
        .unwrap();
    let invalid = binding.submit(&args(&binding, "invalid-stream")).unwrap();
    let valid = binding.submit(&args(&binding, "independent")).unwrap();
    wait_calls(&server.data, 4).await;
    cancelled.cancel();
    let expected = [
        (cancelled.id(), "Cancelled"),
        (expired.id(), "Timeout"),
        (invalid.id(), "ProtocolError"),
    ];
    for (operation, (id, code)) in [cancelled, expired, invalid].into_iter().zip(expected) {
        let error = operation.await.unwrap_err();
        assert_eq!(error.code, code);
        assert_eq!(error.queue_operation_id, Some(id));
        assert!(error.operation_id.is_some());
        assert_eq!(
            error.execution_certainty,
            mcpx_core::ExecutionCertainty::Unknown
        );
        assert_eq!(error.retry_advice, mcpx_core::RetryAdvice::InspectExecution);
    }
    let valid_id = valid.id();
    let outcome = valid.await.unwrap();
    if let CallOutcome::Completed(response) = &outcome {
        let error = response.deserialize::<Vec<String>>().unwrap_err();
        assert_eq!(error.queue_operation_id, Some(valid_id));
        assert!(error.operation_id.is_some());
        assert_eq!(
            error.execution_certainty,
            mcpx_core::ExecutionCertainty::ResultReceived
        );
        assert_eq!(error.retry_advice, mcpx_core::RetryAdvice::InspectResult);
    } else {
        panic!("expected completed result");
    }
    assert_eq!(message(outcome), "independent");
    assert_eq!(
        message(binding.call(&args(&binding, "reused")).await.unwrap()),
        "reused"
    );
    server.data.release.notify_waiters();
    remote.close().await.unwrap();
}

#[tokio::test]
async fn parallel_http_respects_the_native_driver_deadline_in_addition_to_the_queue_deadline() {
    let server = server().await;
    let remote = QueuedRemote::spawn_with(server.config.clone(), QueueOptions::default(), || {
        Ok(Client::new()?.with_timeout(Duration::from_millis(300)))
    })
    .await
    .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let hold = binding.submit(&args(&binding, "hold")).unwrap();
    wait_calls(&server.data, 1).await;
    assert_eq!(
        error(
            tokio::time::timeout(Duration::from_secs(2), hold)
                .await
                .unwrap()
        ),
        "Timeout"
    );
    assert_eq!(
        message(binding.call(&args(&binding, "next")).await.unwrap()),
        "next"
    );
    server.data.release.notify_waiters();
    remote.close().await.unwrap();
}

#[tokio::test]
async fn closing_parallel_http_joins_active_io_and_never_dispatches_pending_calls() {
    let server = server().await;
    let remote = QueuedRemote::connect(
        server.config.clone(),
        QueueOptions {
            max_in_flight: 4,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let mut operations = Vec::new();
    for _ in 0..6 {
        operations.push(binding.submit(&args(&binding, "hold")).unwrap());
    }
    wait_calls(&server.data, 4).await;
    tokio::time::timeout(Duration::from_secs(2), remote.close())
        .await
        .unwrap()
        .unwrap();
    for operation in operations {
        assert_eq!(error(operation.await), "TransportClosed");
    }
    assert_eq!(server.data.calls.lock().unwrap().len(), 4);
    server.data.release.notify_waiters();
}

struct FourthProvider {
    calls: Arc<AtomicUsize>,
    started: Option<tokio::sync::oneshot::Sender<()>>,
    release: Option<tokio::sync::oneshot::Receiver<()>>,
}
impl CredentialProvider for FourthProvider {
    fn resolve<'a>(&'a mut self, _: CredentialRequest<'a>) -> CredentialFuture<'a> {
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 3 {
                let _ = self.started.take().unwrap().send(());
                let _ = self.release.take().unwrap().await;
            }
            Credential::bearer("frozen".into(), "one scope".into())
        })
    }
}
#[tokio::test]
async fn credential_wait_does_not_block_other_responses_events_or_restart_the_provider() {
    let server = server().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let (started, waiting) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let (event, completed_event) = tokio::sync::oneshot::channel();
    let counter = calls.clone();
    let remote =
        QueuedRemote::spawn_with(server.config.clone(), QueueOptions::default(), move || {
            let mut event = Some(event);
            Ok(Client::new()?
                .with_credential_provider(FourthProvider {
                    calls: counter,
                    started: Some(started),
                    release: Some(gate),
                })
                .with_event_handler(move |record| {
                    let mcpx_native::EventStreamItem::Event(record) = record else {
                        panic!("unexpected core loss in fixture")
                    };
                    if record.kind == "completed"
                        && let Some(event) = event.take()
                    {
                        let _ = event.send(record.queue_operation_id);
                    }
                }))
        })
        .await
        .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let first = binding.submit(&args(&binding, "hold")).unwrap();
    let first_id = first.id();
    wait_calls(&server.data, 1).await;
    let second = binding.submit(&args(&binding, "after provider")).unwrap();
    tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap()
        .unwrap();
    server.data.release.notify_one();
    assert_eq!(
        message(
            tokio::time::timeout(Duration::from_secs(1), first)
                .await
                .unwrap()
                .unwrap()
        ),
        "hold"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), completed_event)
            .await
            .unwrap()
            .unwrap(),
        Some(first_id)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    release.send(()).unwrap();
    assert_eq!(message(second.await.unwrap()), "after provider");
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    remote.close().await.unwrap();
}

#[derive(Default)]
struct Data {
    calls: Mutex<Vec<Value>>,
    authorization: Mutex<Vec<String>>,
    started: Notify,
    release: Notify,
    changed: AtomicBool,
    external_schemas: AtomicBool,
    list_calls: AtomicUsize,
    discoveries: AtomicUsize,
    invalidation: Notify,
    hold_next_list: AtomicBool,
    fail_next_list: AtomicBool,
    list_started: Notify,
    list_release: Notify,
}

#[tokio::test]
async fn queued_refreshes_share_publication_but_keep_independent_credentials_and_terminals() {
    let server = server().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let (remote, mut events) = QueuedRemote::spawn_with_events(
        server.config.clone(),
        QueueOptions::default(),
        mcpx_core::EventLimits::default(),
        move || Ok(Client::new()?.with_credential_provider(CountProvider(counter))),
    )
    .await
    .unwrap();
    let baseline = calls.load(Ordering::SeqCst);
    let lists = server.data.list_calls.load(Ordering::SeqCst);
    let mut watcher = remote.subscribe_catalog().unwrap();
    let revision = remote.catalog_revision();
    server.data.hold_next_list.store(true, Ordering::SeqCst);
    let first = remote.refresh().unwrap();
    tokio::time::timeout(Duration::from_secs(2), server.data.list_started.notified())
        .await
        .unwrap();
    let mut operations = vec![first];
    for _ in 1..16 {
        operations.push(remote.refresh().unwrap());
    }
    let cancelled = remote.refresh().unwrap();
    let cancelled_id = cancelled.id();
    let expired = remote
        .refresh_with_timeout(Duration::from_millis(40))
        .unwrap();
    let expired_id = expired.id();
    cancelled.cancel();
    assert_eq!(error(cancelled.await), "Cancelled");
    assert_eq!(
        error(
            tokio::time::timeout(Duration::from_secs(2), expired)
                .await
                .unwrap()
        ),
        "Timeout"
    );
    assert_eq!(remote.catalog_revision(), revision);
    server.data.list_release.notify_one();
    let mut ids = std::collections::BTreeSet::from([cancelled_id, expired_id]);
    for operation in operations {
        assert!(ids.insert(operation.id()));
        operation.await.unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), baseline + 16);
    assert_eq!(server.data.list_calls.load(Ordering::SeqCst), lists + 1);
    let change = tokio::time::timeout(Duration::from_secs(2), watcher.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(change.published && !change.invalidated);
    assert_eq!(change.revision, remote.catalog_revision());
    remote.refresh().unwrap().await.unwrap();
    assert_eq!(server.data.list_calls.load(Ordering::SeqCst), lists + 2);
    remote.close().await.unwrap();
    assert!(watcher.changed().await.unwrap().published);
    assert!(watcher.changed().await.is_none());
    let mut terminals = std::collections::BTreeMap::<u64, usize>::new();
    while let Some(batch) = events.next_batch().await {
        for event in batch.events {
            if !matches!(
                event.kind,
                mcpx_core::OperationEventKind::Queued | mcpx_core::OperationEventKind::Started
            ) && ids.contains(&event.queue_operation_id)
            {
                *terminals.entry(event.queue_operation_id).or_default() += 1;
            }
        }
    }
    assert_eq!(terminals.len(), 18);
    assert!(terminals.values().all(|count| *count == 1));
}

#[tokio::test]
async fn failed_queued_refresh_is_not_shared_and_does_not_notify_a_publication() {
    let server = server().await;
    let remote = QueuedRemote::connect(server.config.clone(), QueueOptions::default())
        .await
        .unwrap();
    let mut watcher = remote.subscribe_catalog().unwrap();
    let revision = remote.catalog_revision();
    let lists = server.data.list_calls.load(Ordering::SeqCst);
    server.data.hold_next_list.store(true, Ordering::SeqCst);
    server.data.fail_next_list.store(true, Ordering::SeqCst);
    let first = remote.refresh().unwrap();
    tokio::time::timeout(Duration::from_secs(2), server.data.list_started.notified())
        .await
        .unwrap();
    let second = remote.refresh().unwrap();
    server.data.list_release.notify_one();
    assert!(first.await.is_err());
    second.await.unwrap();
    assert_ne!(remote.catalog_revision(), revision);
    assert_eq!(server.data.list_calls.load(Ordering::SeqCst), lists + 2);
    assert!(watcher.changed().await.unwrap().published);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), watcher.changed())
            .await
            .is_err()
    );
    remote.close().await.unwrap();
    assert!(watcher.changed().await.is_none());
}

#[tokio::test]
async fn queued_refresh_does_not_reuse_publication_across_registry_barriers() {
    let server = server().await;
    let remote = QueuedRemote::connect(server.config.clone(), QueueOptions::default())
        .await
        .unwrap();
    let lists = server.data.list_calls.load(Ordering::SeqCst);
    let mut watcher = remote.subscribe_catalog().unwrap();
    server.data.hold_next_list.store(true, Ordering::SeqCst);
    let first = remote.refresh().unwrap();
    tokio::time::timeout(Duration::from_secs(2), server.data.list_started.notified())
        .await
        .unwrap();
    let registry = remote
        .set_schema_registry(mcpx_core::SchemaRegistry::default())
        .unwrap();
    let after_registry = remote.refresh().unwrap();
    server.data.list_release.notify_one();
    first.await.unwrap();
    registry.await.unwrap();
    after_registry.await.unwrap();
    assert_eq!(server.data.list_calls.load(Ordering::SeqCst), lists + 2);
    let change = watcher.changed().await.unwrap();
    assert!(change.published && change.invalidated);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn cancelling_leading_refresh_cannot_satisfy_a_waiting_refresh() {
    let server = server().await;
    let remote = QueuedRemote::connect(server.config.clone(), QueueOptions::default())
        .await
        .unwrap();
    let lists = server.data.list_calls.load(Ordering::SeqCst);
    let revision = remote.catalog_revision();
    let mut watcher = remote.subscribe_catalog().unwrap();
    server.data.hold_next_list.store(true, Ordering::SeqCst);
    let first = remote.refresh().unwrap();
    tokio::time::timeout(Duration::from_secs(2), server.data.list_started.notified())
        .await
        .unwrap();
    let second = remote.refresh().unwrap();
    first.cancel();
    assert_eq!(error(first.await), "Cancelled");
    tokio::time::timeout(Duration::from_secs(2), second)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(server.data.list_calls.load(Ordering::SeqCst), lists + 2);
    assert_ne!(remote.catalog_revision(), revision);
    assert!(watcher.changed().await.unwrap().published);
    server.data.list_release.notify_one();
    remote.close().await.unwrap();
}

#[tokio::test]
async fn queued_completion_reads_fresh_schema_and_is_a_cancellable_local_catalog_barrier() {
    let server = server().await;
    let remote = QueuedRemote::connect(server.config.clone(), serial_options())
        .await
        .unwrap();
    let initial_lists = server.data.list_calls.load(Ordering::SeqCst);
    let completion = remote.complete_line("ec", 2).unwrap().await.unwrap();
    assert_eq!(completion.candidates[0].value, "echo");
    let spec = remote.command_spec("echo").unwrap().await.unwrap();
    assert_eq!(spec.parameters[0].value_kind, "string");
    assert_eq!(server.data.list_calls.load(Ordering::SeqCst), initial_lists);
    assert!(server.data.calls.lock().unwrap().is_empty());
    assert_eq!(error(remote.complete_line("雪", 1)), "InvalidCursor");
    let binding = remote.tool("echo").await.unwrap();
    let running = binding.submit(&args(&binding, "hold")).unwrap();
    wait_calls(&server.data, 1).await;
    let pending = remote.complete_line("echo --m", 8).unwrap();
    pending.cancel();
    let pending_id = pending.id();
    let error = pending.await.unwrap_err();
    assert_eq!(error.code, "Cancelled");
    assert_eq!(error.queue_operation_id, Some(pending_id));
    assert!(error.operation_id.is_none());
    assert_eq!(
        error.execution_certainty,
        mcpx_core::ExecutionCertainty::NotSent
    );
    server.data.release.notify_one();
    running.await.unwrap();
    server.data.changed.store(true, Ordering::SeqCst);
    remote.refresh().unwrap().await.unwrap();
    let spec = remote.command_spec("echo").unwrap().await.unwrap();
    assert_eq!(spec.parameters[0].value_kind, "integer");
    assert_eq!(server.data.calls.lock().unwrap().len(), 1);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn queued_schema_replacement_is_atomic_cancellable_and_invalidates_old_bindings() {
    let server = server().await;
    let make = |name: &str| {
        mcpx_core::SchemaRegistry::new([(
            format!("https://schemas.example/{name}"),
            json!({"type":"object"}),
        )])
        .unwrap()
    };
    let initial = make("initial");
    let remote = QueuedRemote::spawn_with(server.config.clone(), serial_options(), move || {
        Ok(Client::new()?.with_schema_registry(initial))
    })
    .await
    .unwrap();
    assert_eq!(
        remote
            .schema_registry_stats()
            .unwrap()
            .await
            .unwrap()
            .resources,
        1
    );
    let binding = remote.tool("echo").await.unwrap();
    let running = binding.submit(&args(&binding, "hold")).unwrap();
    wait_calls(&server.data, 1).await;
    let cancelled = remote.set_schema_registry(make("cancelled")).unwrap();
    cancelled.cancel();
    assert_eq!(error(cancelled.await), "Cancelled");
    server.data.release.notify_one();
    running.await.unwrap();
    assert_eq!(
        message(
            binding
                .call(&args(&binding, "still-current"))
                .await
                .unwrap()
        ),
        "still-current"
    );
    remote
        .set_schema_registry(make("replacement"))
        .unwrap()
        .await
        .unwrap();
    assert_eq!(
        error(
            binding
                .submit(&args(&binding, "never-dispatched"))
                .unwrap()
                .await
        ),
        "BindingStale"
    );
    assert_eq!(server.data.calls.lock().unwrap().len(), 2);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn async_event_stream_reserves_terminal_capacity_for_each_concurrent_admission() {
    let server = server().await;
    let ServerConfig::Http { url, .. } = &server.config else {
        panic!()
    };
    let (client, mut events) = Client::new().unwrap().with_event_stream(4).unwrap();
    let mut remote = client
        .connect(url, mcpx_core::Dialect::Modern)
        .await
        .unwrap();
    while events.try_next().is_some() {}
    let binding = remote.tool("echo").unwrap();
    let args = binding
        .validate_json(json!({"message":"reserved"}))
        .unwrap();
    let first = remote.begin_concurrent_call(&binding, &args).await.unwrap();
    let second = remote.begin_concurrent_call(&binding, &args).await.unwrap();
    assert_eq!(
        remote
            .begin_concurrent_call(&binding, &args)
            .await
            .unwrap_err()
            .code,
        "ConsumerLagged"
    );
    assert!(server.data.calls.lock().unwrap().is_empty());
    remote.host_runtime_mut().cancel_operation(first).unwrap();
    assert_eq!(
        remote
            .take_concurrent_completion(first)
            .unwrap()
            .unwrap_err()
            .code,
        "Cancelled"
    );
    assert_eq!(
        remote
            .begin_concurrent_call(&binding, &args)
            .await
            .unwrap_err()
            .code,
        "ConsumerLagged"
    );
    assert!(events.try_next().is_some());
    let third = remote.begin_concurrent_call(&binding, &args).await.unwrap();
    remote.close().await.unwrap();
    for operation in [second, third] {
        assert_eq!(
            remote
                .take_concurrent_completion(operation)
                .unwrap()
                .unwrap_err()
                .code,
            "Cancelled"
        );
    }
    let mut terminal = 0;
    while let Some(event) = events.next().await {
        if let mcpx_native::EventStreamItem::Event(event) = event {
            terminal += usize::from(event.is_terminal());
        }
    }
    assert_eq!(terminal, 2);
}
struct Server {
    config: ServerConfig,
    data: Arc<Data>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn handler(
    State(data): State<Arc<Data>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<Value>,
) -> axum::response::Response {
    let result = match request["method"].as_str().unwrap() {
        "server/discover" => {
            data.discoveries.fetch_add(1, Ordering::SeqCst);
            json!({"supportedVersions":["2026-07-28"]})
        }
        "tools/list" => {
            data.list_calls.fetch_add(1, Ordering::SeqCst);
            if data.hold_next_list.swap(false, Ordering::SeqCst) {
                data.list_started.notify_one();
                data.list_release.notified().await;
            }
            if data.fail_next_list.swap(false, Ordering::SeqCst) {
                return (
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    "fixture metadata failure",
                )
                    .into_response();
            }
            if data.external_schemas.load(Ordering::SeqCst) {
                return Json(json!({"jsonrpc":"2.0","id":request["id"],"result":{"tools":[{"name":"echo","inputSchema":{"$ref":"https://schemas.example/echo"}}]}})).into_response();
            }
            json!({"tools":[{"name":"echo","inputSchema":{"type":"object","properties":{"message":{"type":if data.changed.load(Ordering::SeqCst) {"integer"} else {"string"}}},"required":["message"],"additionalProperties":false}}]})
        }
        "tools/call" => {
            data.calls.lock().unwrap().push(request.clone());
            data.authorization.lock().unwrap().push(
                headers
                    .get("authorization")
                    .map_or("", |value| value.to_str().unwrap())
                    .into(),
            );
            data.started.notify_one();
            if request["params"]["arguments"]["message"] == "hold-invalidating" {
                let stream = futures_util::stream::unfold(
                    (0, data.clone(), request.clone()),
                    |(phase, data, request)| async move {
                        let frame = match phase {
                            0 => {
                                data.invalidation.notified().await;
                                json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed"})
                            }
                            1 => {
                                data.release.notified().await;
                                json!({"jsonrpc":"2.0","id":request["id"],"result":{"content":[],"structuredContent":request["params"]["arguments"]}})
                            }
                            _ => return None,
                        };
                        Some((
                            Ok::<_, std::io::Error>(format!("data: {frame}\n\n")),
                            (phase + 1, data, request),
                        ))
                    },
                );
                return axum::response::Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(axum::body::Body::from_stream(stream))
                    .unwrap();
            }
            if request["params"]["arguments"]["message"]
                .as_str()
                .is_some_and(|message| message.starts_with("interactive-parallel-"))
            {
                let value = &request["params"]["arguments"]["message"];
                if request["params"].get("inputResponses").is_none() {
                    let result = json!({
                        "resultType":"input_required", "requestState":format!("state:{value}"),
                        "inputRequests":{"confirm":{"method":"elicitation/create","params":{
                            "message":"Continue?", "requestedSchema":{"type":"object",
                                "properties":{"yes":{"type":"boolean"}}, "required":["yes"]}
                        }}}
                    });
                    return Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
                        .into_response();
                }
                assert_eq!(request["params"]["requestState"], format!("state:{value}"));
                assert_eq!(
                    request["params"]["inputResponses"]["confirm"]["content"]["yes"],
                    true
                );
                data.release.notified().await;
            }
            if request["params"]["arguments"]["message"] == "invalid-stream" {
                return axum::response::Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(axum::body::Body::from("data: not-json\n\n"))
                    .unwrap();
            }
            if request["params"]["arguments"]["message"] == "hold" {
                data.started.notify_one();
                data.release.notified().await;
            }
            if request["params"]["arguments"]["message"] == "interactive"
                && request["params"].get("inputResponses").is_none()
            {
                json!({"resultType":"input_required","requestState":"queue opaque state"})
            } else {
                if request["params"]["arguments"]["message"] == "interactive" {
                    assert_eq!(request["params"]["requestState"], "queue opaque state");
                }
                json!({"content":[],"structuredContent":request["params"]["arguments"]})
            }
        }
        other => panic!("unexpected method {other}"),
    };
    Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result})).into_response()
}
async fn server() -> Server {
    let data = Arc::new(Data::default());
    let router = axum::Router::new()
        .route("/mcp", axum::routing::post(handler))
        .with_state(data.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = ServerConfig::Http {
        url: format!("http://{}/mcp", listener.local_addr().unwrap()),
        protocol: ProtocolMode::Modern,
        credential: None,
    };
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    Server { config, data, task }
}
fn args(binding: &QueuedBinding, message: &str) -> mcpx_native::QueuedArgs {
    binding
        .args()
        .unwrap()
        .set("message", message)
        .unwrap()
        .finish()
        .unwrap()
}
fn message(outcome: CallOutcome) -> String {
    let CallOutcome::Completed(response) = outcome else {
        panic!()
    };
    response.raw["structuredContent"]["message"]
        .as_str()
        .unwrap()
        .into()
}
fn error<T>(result: mcpx_core::Result<T>) -> String {
    match result {
        Err(error) => error.code,
        Ok(_) => panic!("expected failure"),
    }
}

#[tokio::test]
async fn concurrent_callers_share_typed_bindings_without_moving_core_state_across_threads() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<QueuedRemote>();
    send_sync::<QueuedBinding>();
    send_sync::<mcpx_native::QueuedArgs>();
    let server = server().await;
    let remote = QueuedRemote::connect(server.config.clone(), QueueOptions::default())
        .await
        .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let mut callers = Vec::new();
    for index in 0..12 {
        let binding = binding.clone();
        callers.push(tokio::spawn(async move {
            let args = args(&binding, &format!("caller-{index}"));
            let operation = binding.submit(&args).unwrap();
            (operation.id(), message(operation.await.unwrap()))
        }));
    }
    let mut ids = std::collections::BTreeSet::new();
    for caller in callers {
        let (id, value) = caller.await.unwrap();
        assert!(ids.insert(id));
        assert!(value.starts_with("caller-"));
    }
    assert_eq!(server.data.calls.lock().unwrap().len(), 12);
    let thread_binding = binding.clone();
    let operation = std::thread::spawn(move || {
        thread_binding.submit(&args(&thread_binding, "plain OS thread"))
    })
    .join()
    .unwrap()
    .unwrap();
    assert_eq!(message(operation.await.unwrap()), "plain OS thread");
    let batch = remote.drain_events().unwrap().await.unwrap();
    assert_eq!(batch.dropped, 0);
    let events = batch.events;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "completed")
            .count(),
        13
    );
    let Execution::Outcome(outcome) = remote
        .execute("echo", "--message 'string call'")
        .unwrap()
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(message(outcome), "string call");
    remote.close().await.unwrap();
    assert_eq!(
        error(binding.submit(&args(&binding, "closed"))),
        "TransportClosed"
    );
}

#[tokio::test]
async fn bounded_admission_queued_cancellation_and_fifo_do_not_send_cancelled_calls() {
    let server = server().await;
    let remote = QueuedRemote::connect(
        server.config.clone(),
        QueueOptions {
            max_in_flight: 1,
            capacity: 3,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let first = binding.submit(&args(&binding, "hold")).unwrap();
    server.data.started.notified().await;
    let cancelled = binding.submit(&args(&binding, "must not send")).unwrap();
    let third = binding.submit(&args(&binding, "third")).unwrap();
    assert_eq!(
        error(binding.submit(&args(&binding, "overflow"))),
        "QueueFull"
    );
    assert!(first.id() < cancelled.id() && cancelled.id() < third.id());
    cancelled.cancel();
    assert_eq!(
        error(
            tokio::time::timeout(Duration::from_secs(1), cancelled)
                .await
                .unwrap()
        ),
        "Cancelled"
    );
    let fourth = binding.submit(&args(&binding, "fourth")).unwrap();
    server.data.release.notify_one();
    assert_eq!(message(first.await.unwrap()), "hold");
    assert_eq!(message(third.await.unwrap()), "third");
    assert_eq!(message(fourth.await.unwrap()), "fourth");
    {
        let calls = server.data.calls.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .map(|call| call["params"]["arguments"]["message"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["hold", "third", "fourth"]
        );
    }
    remote.close().await.unwrap();
}

#[tokio::test]
async fn short_queued_deadline_expires_while_the_active_call_is_still_running() {
    let server = server().await;
    let remote = QueuedRemote::connect(server.config.clone(), serial_options())
        .await
        .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let first = binding.submit(&args(&binding, "hold")).unwrap();
    server.data.started.notified().await;
    let expired = binding
        .submit_with_timeout(&args(&binding, "expired"), Duration::from_millis(30))
        .unwrap();
    assert_eq!(
        error(
            tokio::time::timeout(Duration::from_secs(1), expired)
                .await
                .unwrap()
        ),
        "Timeout"
    );
    assert_eq!(server.data.calls.lock().unwrap().len(), 1);
    server.data.release.notify_one();
    first.await.unwrap();
    assert_eq!(
        message(
            binding
                .call(&args(&binding, "after timeout"))
                .await
                .unwrap()
        ),
        "after timeout"
    );
    remote.close().await.unwrap();
}

#[tokio::test]
async fn active_cancel_and_dropped_queued_futures_leave_the_next_http_call_usable() {
    let server = server().await;
    let remote = QueuedRemote::connect(
        server.config.clone(),
        QueueOptions {
            max_in_flight: 1,
            capacity: 3,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let active = binding.submit(&args(&binding, "hold")).unwrap();
    server.data.started.notified().await;
    let dropped = binding.submit(&args(&binding, "dropped")).unwrap();
    let next = binding.submit(&args(&binding, "next")).unwrap();
    drop(dropped);
    let cancellation = active.cancellation_handle();
    assert_eq!(cancellation.id(), active.id());
    let other_thread = cancellation.clone();
    std::thread::spawn(move || other_thread.cancel())
        .join()
        .unwrap();
    assert_eq!(error(active.await), "Cancelled");
    cancellation.cancel();
    assert_eq!(message(next.await.unwrap()), "next");
    assert_eq!(server.data.calls.lock().unwrap().len(), 2);
    server.data.release.notify_one();
    remote.close().await.unwrap();
}

#[tokio::test]
async fn shutdown_cancels_active_and_pending_operations_and_is_idempotent() {
    let server = server().await;
    let remote = QueuedRemote::connect(server.config.clone(), serial_options())
        .await
        .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let active = binding.submit(&args(&binding, "hold")).unwrap();
    server.data.started.notified().await;
    let pending = binding.submit(&args(&binding, "never"));
    remote.close().await.unwrap();
    assert_eq!(error(active.await), "TransportClosed");
    assert_eq!(error(pending.unwrap().await), "TransportClosed");
    remote.close().await.unwrap();
    assert_eq!(server.data.calls.lock().unwrap().len(), 1);
    server.data.release.notify_one();
}

#[tokio::test]
async fn typed_queue_inputs_are_local_single_pass_and_keep_binding_leases_across_threads() {
    struct LocalInput {
        visits: Rc<Cell<usize>>,
        message: &'static str,
    }
    impl serde::Serialize for LocalInput {
        fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
            self.visits.set(self.visits.get() + 1);
            use serde::ser::SerializeStruct;
            let mut value = s.serialize_struct("LocalInput", 1)?;
            value.serialize_field("message", self.message)?;
            value.end()
        }
    }
    let server = server().await;
    let remote = QueuedRemote::connect(server.config.clone(), QueueOptions::default())
        .await
        .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let other = remote.tool("echo").await.unwrap();
    let visits = Rc::new(Cell::new(0));
    let args = binding
        .args_from(LocalInput {
            visits: visits.clone(),
            message: "typed-lease",
        })
        .unwrap();
    assert_eq!(visits.get(), 1);
    assert_eq!(error(other.submit(&args)), "BindingMismatch");
    assert_eq!(
        error(binding.submit_typed(json!({"message":3}))),
        "ValidationFailed"
    );
    assert!(server.data.calls.lock().unwrap().is_empty());
    let operation = binding
        .submit_typed(LocalInput {
            visits: visits.clone(),
            message: "typed-sent",
        })
        .unwrap();
    assert_eq!(visits.get(), 2);
    // LocalInput is deliberately !Send. Serialization is finished before this
    // operation is moved to Tokio's Send task and the caller binding is dropped.
    drop(binding);
    drop(args);
    assert_eq!(
        message(tokio::spawn(operation).await.unwrap().unwrap()),
        "typed-sent"
    );
    assert_eq!(
        message(
            other
                .call_typed(json!({"message":"typed-async"}))
                .await
                .unwrap()
        ),
        "typed-async"
    );
    assert_eq!(server.data.calls.lock().unwrap().len(), 2);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn argument_ownership_schema_changes_and_retained_job_leases_are_checked() {
    let server = server().await;
    let remote = QueuedRemote::connect(server.config.clone(), serial_options())
        .await
        .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let other = remote.tool("echo").await.unwrap();
    assert_eq!(
        error(binding.submit(&args(&other, "foreign"))),
        "BindingMismatch"
    );
    assert_eq!(
        error(binding.submit_json(json!({"message":5}))),
        "ValidationFailed"
    );
    let active = binding.submit(&args(&binding, "hold")).unwrap();
    server.data.started.notified().await;
    let queued = other.submit(&args(&other, "leased")).unwrap();
    drop(other);
    server.data.release.notify_one();
    active.await.unwrap();
    assert_eq!(message(queued.await.unwrap()), "leased");
    server.data.changed.store(true, Ordering::SeqCst);
    remote.refresh().unwrap().await.unwrap();
    assert_eq!(
        error(binding.submit(&args(&binding, "stale")).unwrap().await),
        "BindingStale"
    );
    assert_eq!(server.data.calls.lock().unwrap().len(), 2);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn queued_cache_controls_preserve_live_typed_and_json_bindings() {
    let server = server().await;
    let remote = QueuedRemote::spawn_with(server.config.clone(), QueueOptions::default(), || {
        Client::new()?.with_validator_cache_capacity(1)
    })
    .await
    .unwrap();
    let initial = remote.validator_cache_stats().unwrap().await.unwrap();
    assert_eq!((initial.capacity, initial.entries), (1, 0));
    let binding = remote.tool("echo").await.unwrap();
    let cold = remote.validator_cache_stats().unwrap().await.unwrap();
    assert_eq!((cold.entries, cold.compilations), (1, 1));
    let typed = args(&binding, "typed-pinned");
    remote.clear_validator_cache().unwrap().await.unwrap();
    assert_eq!(
        remote
            .validator_cache_stats()
            .unwrap()
            .await
            .unwrap()
            .entries,
        0
    );
    assert_eq!(message(binding.call(&typed).await.unwrap()), "typed-pinned");
    assert_eq!(
        message(
            binding
                .submit_json(json!({"message":"json-pinned"}))
                .unwrap()
                .await
                .unwrap()
        ),
        "json-pinned"
    );
    assert_eq!(
        error(binding.submit_json(json!({"message":3}))),
        "ValidationFailed"
    );
    assert_eq!(
        error(remote.set_validator_cache_capacity(4097)),
        "ConfigurationError"
    );
    remote
        .set_validator_cache_capacity(0)
        .unwrap()
        .await
        .unwrap();
    let after = remote.validator_cache_stats().unwrap().await.unwrap();
    assert_eq!(
        (after.capacity, after.entries, after.compilations),
        (0, 0, cold.compilations)
    );
    assert_eq!(server.data.calls.lock().unwrap().len(), 2);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn queued_bindings_carry_their_schema_resources_for_typed_and_json_local_validation() {
    let server = server().await;
    server.data.external_schemas.store(true, Ordering::SeqCst);
    let registry = mcpx_core::SchemaRegistry::new([("https://schemas.example/echo".into(), json!({"type":"object","properties":{"message":{"type":"string","minLength":2}},"required":["message"],"additionalProperties":false}))]).unwrap();
    let remote =
        QueuedRemote::spawn_with(server.config.clone(), QueueOptions::default(), move || {
            Ok(Client::new()?.with_schema_registry(registry))
        })
        .await
        .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    assert_eq!(
        error(binding.args().unwrap().set("message", 3).unwrap().finish()),
        "ValidationFailed"
    );
    assert_eq!(
        error(binding.submit_json(json!({"message":"x"}))),
        "ValidationFailed"
    );
    assert!(server.data.calls.lock().unwrap().is_empty());
    let worker = binding.clone();
    let called = tokio::spawn(async move {
        #[derive(serde::Serialize)]
        struct Input {
            message: &'static str,
        }
        let args = worker
            .args_from(Input {
                message: "typed-resource",
            })
            .unwrap();
        worker.call(&args).await.unwrap()
    });
    assert_eq!(message(called.await.unwrap()), "typed-resource");
    assert_eq!(
        message(
            binding
                .submit_json(json!({"message":"json-resource"}))
                .unwrap()
                .await
                .unwrap()
        ),
        "json-resource"
    );
    assert_eq!(server.data.calls.lock().unwrap().len(), 2);
    remote
        .set_schema_registry(mcpx_core::SchemaRegistry::default())
        .unwrap()
        .await
        .unwrap();
    assert_eq!(
        error(
            binding
                .submit(&args(&binding, "stale-resource"))
                .unwrap()
                .await
        ),
        "BindingStale"
    );
    assert_eq!(
        error(remote.tool("echo").await),
        "SchemaReferenceUnavailable"
    );
    remote.close().await.unwrap();
}

struct LocalProvider {
    count: Rc<Cell<usize>>,
    identity: Arc<AtomicUsize>,
}
impl CredentialProvider for LocalProvider {
    fn resolve<'a>(&'a mut self, _: CredentialRequest<'a>) -> CredentialFuture<'a> {
        Box::pin(async move {
            self.count.set(self.count.get() + 1);
            Credential::bearer(
                "test-token".into(),
                format!("scope-{}", self.identity.load(Ordering::SeqCst)),
            )
        })
    }
}
#[tokio::test]
async fn factory_can_create_non_send_providers_and_identity_changes_stop_old_calls() {
    let server = server().await;
    let identity = Arc::new(AtomicUsize::new(0));
    let shared = identity.clone();
    let remote = QueuedRemote::spawn_with(server.config.clone(), serial_options(), move || {
        Ok(Client::new()?.with_credential_provider(LocalProvider {
            count: Rc::new(Cell::new(0)),
            identity: shared,
        }))
    })
    .await
    .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    identity.store(1, Ordering::SeqCst);
    assert_eq!(
        error(
            binding
                .submit(&args(&binding, "old identity"))
                .unwrap()
                .await
        ),
        "BindingStale"
    );
    assert!(server.data.calls.lock().unwrap().is_empty());
    let binding = remote.tool("echo").await.unwrap();
    assert_eq!(
        message(binding.call(&args(&binding, "new identity")).await.unwrap()),
        "new identity"
    );
    remote.close().await.unwrap();
}

#[tokio::test]
async fn queue_continuations_are_send_and_remain_single_use() {
    let server = server().await;
    let remote = QueuedRemote::connect(server.config.clone(), serial_options())
        .await
        .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let CallOutcome::NeedsInput(handle) =
        binding.call(&args(&binding, "interactive")).await.unwrap()
    else {
        panic!()
    };
    let other_task = remote.clone();
    let handle_copy = handle.clone();
    let result =
        tokio::spawn(async move { other_task.resume(&handle_copy, json!({})).unwrap().await })
            .await
            .unwrap()
            .unwrap();
    assert_eq!(message(result), "interactive");
    assert_eq!(
        error(remote.resume(&handle, json!({})).unwrap().await),
        "InvalidContinuation"
    );
    remote.close().await.unwrap();
}

#[tokio::test]
async fn input_byte_budget_applies_independently_of_request_count() {
    let server = server().await;
    let remote = QueuedRemote::connect(
        server.config.clone(),
        QueueOptions {
            max_in_flight: 1,
            max_bytes: 1024,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let active = binding.submit(&args(&binding, "hold")).unwrap();
    server.data.started.notified().await;
    assert_eq!(
        error(binding.submit(&args(&binding, &"x".repeat(900)))),
        "QueueFull"
    );
    assert_eq!(
        error(remote.execute_argv("echo", &vec![String::new(); 4097])),
        "LimitExceeded"
    );
    assert_eq!(
        error(binding.submit_with_timeout(&args(&binding, "bad deadline"), Duration::MAX)),
        "ConfigurationError"
    );
    server.data.release.notify_one();
    active.await.unwrap();
    assert_eq!(
        message(
            binding
                .call(&args(&binding, &"x".repeat(700)))
                .await
                .unwrap()
        )
        .len(),
        700
    );
    remote.close().await.unwrap();
}

#[tokio::test]
async fn stdio_owner_thread_reuses_the_child_after_active_cancellation() {
    let fixture = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/stdio_server.py");
    let marker = std::env::temp_dir().join(format!("mcpx-queue-eof-{}", std::process::id()));
    let server = ServerConfig::Stdio {
        program: "python3".into(),
        args: vec![
            fixture.to_string_lossy().into(),
            "--async-calls".into(),
            "--selective-delay".into(),
            "--delay".into(),
            "10".into(),
            "--eof-marker".into(),
            marker.to_string_lossy().into(),
        ],
        env: BTreeMap::new(),
        working_directory: None,
        protocol: ProtocolMode::Modern,
    };
    let (progress, started) = tokio::sync::oneshot::channel();
    let remote = QueuedRemote::spawn_with(server, serial_options(), move || {
        let mut progress = Some(progress);
        Ok(Client::new()?.with_event_handler(move |event| {
            let mcpx_native::EventStreamItem::Event(event) = event else {
                panic!("unexpected core loss in fixture")
            };
            if event.kind == "progress"
                && let Some(progress) = progress.take()
            {
                let _ = progress.send(());
            }
        }))
    })
    .await
    .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let active = binding.submit(&args(&binding, "slow")).unwrap();
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .unwrap()
        .unwrap();
    active.cancel();
    assert_eq!(error(active.await), "Cancelled");
    assert_eq!(
        message(
            binding
                .submit(&args(&binding, "reused"))
                .unwrap()
                .await
                .unwrap()
        ),
        "reused"
    );
    remote.close().await.unwrap();
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "closed");
    std::fs::remove_file(marker).unwrap();
}

#[tokio::test]
async fn active_deadline_emits_timeout_and_http_remains_usable() {
    let server = server().await;
    let remote = QueuedRemote::connect(server.config.clone(), serial_options())
        .await
        .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let active = binding
        .submit_with_timeout(&args(&binding, "hold"), Duration::from_millis(50))
        .unwrap();
    server.data.started.notified().await;
    assert_eq!(error(active.await), "Timeout");
    let batch = remote.drain_events().unwrap().await.unwrap();
    assert_eq!(batch.dropped, 0);
    let events = batch.events;
    assert!(
        events
            .iter()
            .any(|event| event.kind == "failed" && event.payload["code"] == "Timeout")
    );
    assert_eq!(
        message(binding.call(&args(&binding, "next")).await.unwrap()),
        "next"
    );
    server.data.release.notify_one();
    remote.close().await.unwrap();
}

struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);
impl Drop for DropSignal {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}
struct PendingProvider {
    started: Option<tokio::sync::oneshot::Sender<()>>,
    _dropped: DropSignal,
}
impl CredentialProvider for PendingProvider {
    fn resolve<'a>(&'a mut self, _: CredentialRequest<'a>) -> CredentialFuture<'a> {
        Box::pin(async move {
            if let Some(started) = self.started.take() {
                let _ = started.send(());
            }
            std::future::pending().await
        })
    }
}
#[tokio::test]
async fn dropping_startup_future_cancels_credential_resolution_and_drops_the_owner() {
    let server = server().await;
    let (started, waiting) = tokio::sync::oneshot::channel();
    let (dropped, finished) = tokio::sync::oneshot::channel();
    let config = server.config.clone();
    let setup = tokio::spawn(async move {
        QueuedRemote::spawn_with(config, serial_options(), move || {
            Ok(Client::new()?.with_credential_provider(PendingProvider {
                started: Some(started),
                _dropped: DropSignal(Some(dropped)),
            }))
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap()
        .unwrap();
    setup.abort();
    assert!(matches!(setup.await, Err(error) if error.is_cancelled()));
    tokio::time::timeout(Duration::from_secs(2), finished)
        .await
        .unwrap()
        .unwrap();
    assert!(server.data.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn dropping_all_handles_shuts_down_an_active_owner_thread() {
    let server = server().await;
    let (dropped, finished) = tokio::sync::oneshot::channel();
    let remote = QueuedRemote::spawn_with(server.config.clone(), serial_options(), move || {
        let marker = DropSignal(Some(dropped));
        Ok(Client::new()?.with_event_handler(move |_| {
            let _ = &marker;
        }))
    })
    .await
    .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let active = binding.submit(&args(&binding, "hold")).unwrap();
    server.data.started.notified().await;
    drop(binding);
    drop(remote);
    assert_eq!(error(active.await), "TransportClosed");
    tokio::time::timeout(Duration::from_secs(2), finished)
        .await
        .unwrap()
        .unwrap();
    server.data.release.notify_one();
}

struct CountProvider(Arc<AtomicUsize>);
impl CredentialProvider for CountProvider {
    fn resolve<'a>(&'a mut self, _: CredentialRequest<'a>) -> CredentialFuture<'a> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Credential::bearer("frozen".into(), "stable scope".into()) })
    }
}
fn command_outcome(execution: Execution) -> CallOutcome {
    match execution {
        Execution::Outcome(outcome) => outcome,
        Execution::Help(_) => panic!("expected tool result"),
    }
}

#[tokio::test]
async fn typed_string_and_argv_calls_share_sixteen_slots_and_one_preflight_per_operation() {
    let server = server().await;
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let counter = provider_calls.clone();
    let events = Arc::new(Mutex::new(Vec::<mcpx_core::Event>::new()));
    let sink = events.clone();
    let remote =
        QueuedRemote::spawn_with(server.config.clone(), QueueOptions::default(), move || {
            Ok(Client::new()?
                .with_credential_provider(CountProvider(counter))
                .with_event_handler(move |event| {
                    let mcpx_native::EventStreamItem::Event(event) = event else {
                        panic!("unexpected core loss in fixture")
                    };
                    sink.lock().unwrap().push(event.clone());
                }))
        })
        .await
        .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let mut calls: Vec<
        std::pin::Pin<Box<dyn std::future::Future<Output = mcpx_core::Result<CallOutcome>> + Send>>,
    > = Vec::new();
    let mut ids = std::collections::BTreeSet::new();
    for index in 0..16 {
        if index % 3 == 0 {
            let call = binding.submit(&args(&binding, "hold")).unwrap();
            ids.insert(call.id());
            calls.push(Box::pin(call));
        } else {
            let call = if index % 3 == 1 {
                remote.execute("echo", "--message 'hold'")
            } else {
                remote.execute_argv("echo", &["--message".into(), "hold".into()])
            }
            .unwrap();
            ids.insert(call.id());
            calls.push(Box::pin(async move { call.await.map(command_outcome) }));
        }
    }
    let mut last = remote.execute("echo", "--message seventeenth").unwrap();
    ids.insert(last.id());
    wait_calls(&server.data, 16).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(40), &mut last)
            .await
            .is_err()
    );
    assert_eq!(server.data.calls.lock().unwrap().len(), 16);
    server.data.release.notify_waiters();
    for call in calls {
        assert_eq!(message(call.await.unwrap()), "hold");
    }
    assert_eq!(message(command_outcome(last.await.unwrap())), "seventeenth");
    assert_eq!(provider_calls.load(Ordering::SeqCst), 19); // connect, bind, 17 business admissions
    remote.close().await.unwrap();
    let records = events.lock().unwrap();
    let terminals: Vec<_> = records
        .iter()
        .filter(|event| event.kind == "completed")
        .collect();
    assert_eq!(terminals.len(), 17);
    assert_eq!(
        terminals
            .iter()
            .filter_map(|event| event.queue_operation_id)
            .collect::<std::collections::BTreeSet<_>>(),
        ids
    );
}

#[tokio::test]
async fn help_and_invalid_commands_finish_while_another_call_is_running_without_sending() {
    let server = server().await;
    let remote = QueuedRemote::connect(server.config.clone(), QueueOptions::default())
        .await
        .unwrap();
    let held = remote.execute("echo", "--message hold").unwrap();
    wait_calls(&server.data, 1).await;
    let help = tokio::time::timeout(
        Duration::from_secs(1),
        remote.execute("echo", "--help").unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(help, Execution::Help(text) if text.contains("--message")));
    assert_eq!(
        error(
            tokio::time::timeout(
                Duration::from_secs(1),
                remote.execute("echo", "--unknown input").unwrap()
            )
            .await
            .unwrap()
        ),
        "CommandParseError"
    );
    assert_eq!(server.data.calls.lock().unwrap().len(), 1);
    held.cancel();
    assert_eq!(error(held.await), "Cancelled");
    server.data.release.notify_waiters();
    remote.close().await.unwrap();
}

#[tokio::test]
async fn sixteen_resumes_keep_independent_state_allow_correction_and_do_not_block_named_calls() {
    let server = server().await;
    let remote = QueuedRemote::connect(server.config.clone(), QueueOptions::default())
        .await
        .unwrap();
    remote.set_elicitation(true, false).unwrap().await.unwrap();
    let mut initial = Vec::new();
    for index in 0..16 {
        initial.push(
            remote
                .execute("echo", &format!("--message interactive-parallel-{index}"))
                .unwrap(),
        );
    }
    let mut handles = Vec::new();
    for call in initial {
        let CallOutcome::NeedsInput(handle) = command_outcome(call.await.unwrap()) else {
            panic!()
        };
        handles.push(handle);
    }
    assert_eq!(
        error(
            remote
                .resume(
                    &handles[0],
                    json!({"confirm":{"action":"accept","content":{"yes":"bad"}}})
                )
                .unwrap()
                .await
        ),
        "ValidationFailed"
    );
    assert_eq!(server.data.calls.lock().unwrap().len(), 16);
    let mut resumes: Vec<_> = handles
        .iter()
        .map(|handle| {
            remote
                .resume(
                    handle,
                    json!({"confirm":{"action":"accept","content":{"yes":true}}}),
                )
                .unwrap()
        })
        .collect();
    wait_calls(&server.data, 32).await;
    let duplicate = remote
        .resume(&handles[0], json!({"confirm":{"action":"cancel"}}))
        .unwrap();
    let independent = remote.execute("echo", "--message independent").unwrap();
    resumes[0].cancel();
    assert_eq!(error(resumes.remove(0).await), "Cancelled");
    assert_eq!(
        error(
            tokio::time::timeout(Duration::from_secs(1), duplicate)
                .await
                .unwrap()
        ),
        "InvalidContinuation"
    );
    assert_eq!(
        message(command_outcome(
            tokio::time::timeout(Duration::from_secs(1), independent)
                .await
                .unwrap()
                .unwrap()
        )),
        "independent"
    );
    server.data.release.notify_waiters();
    for (index, resumed) in resumes.into_iter().enumerate() {
        assert_eq!(
            message(resumed.await.unwrap()),
            format!("interactive-parallel-{}", index + 1)
        );
    }
    {
        let records = server.data.calls.lock().unwrap();
        assert_eq!(records.len(), 33);
        for initial in records.iter().take(16) {
            let resumed = records
                .iter()
                .find(|request| {
                    request["params"]["arguments"] == initial["params"]["arguments"]
                        && request["params"].get("inputResponses").is_some()
                })
                .unwrap();
            assert_ne!(initial["id"], resumed["id"]);
            assert_eq!(
                resumed["params"]["requestState"],
                format!("state:{}", initial["params"]["arguments"]["message"])
            );
        }
    }
    remote.close().await.unwrap();
}

#[tokio::test]
async fn catalog_invalidation_during_command_preflight_drains_old_io_then_refreshes_once() {
    let server = server().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let (started, waiting) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let (notified, notification) = tokio::sync::oneshot::channel();
    let remote =
        QueuedRemote::spawn_with(server.config.clone(), QueueOptions::default(), move || {
            let mut notified = Some(notified);
            Ok(Client::new()?
                .with_credential_provider(FourthProvider {
                    calls: counter,
                    started: Some(started),
                    release: Some(gate),
                })
                .with_event_handler(move |event| {
                    let mcpx_native::EventStreamItem::Event(event) = event else {
                        panic!("unexpected core loss in fixture")
                    };
                    if event.payload["method"] == "notifications/tools/list_changed"
                        && let Some(sender) = notified.take()
                    {
                        let _ = sender.send(());
                    }
                }))
        })
        .await
        .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let first = binding
        .submit(&args(&binding, "hold-invalidating"))
        .unwrap();
    wait_calls(&server.data, 1).await;
    let mut next = remote.execute("echo", "--message 42").unwrap();
    tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap();
    server.data.changed.store(true, Ordering::SeqCst);
    server.data.invalidation.notify_one();
    tokio::time::timeout(Duration::from_secs(1), notification)
        .await
        .unwrap()
        .unwrap();
    release.send(()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(40), &mut next)
            .await
            .is_err()
    );
    assert_eq!(server.data.calls.lock().unwrap().len(), 1);
    server.data.release.notify_one();
    assert_eq!(message(first.await.unwrap()), "hold-invalidating");
    let CallOutcome::Completed(result) = command_outcome(next.await.unwrap()) else {
        panic!()
    };
    assert_eq!(result.raw["structuredContent"]["message"], 42);
    assert_eq!(server.data.list_calls.load(Ordering::SeqCst), 2);
    assert_eq!(server.data.discoveries.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn serial_command_entry_points_resolve_the_provider_once_even_when_returning_help() {
    let server = server().await;
    let ServerConfig::Http { url, .. } = &server.config else {
        panic!()
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let mut remote = Client::new()
        .unwrap()
        .with_credential_provider(CountProvider(calls.clone()))
        .connect(url, mcpx_core::Dialect::Modern)
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    remote.execute("echo", "--message text").await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    remote
        .execute_argv("echo", &["--message".into(), "argv".into()])
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(matches!(
        remote.execute("echo", "--help").await.unwrap(),
        Execution::Help(_)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert_eq!(server.data.calls.lock().unwrap().len(), 2);
    remote.close().await.unwrap();
}

struct ScopedProvider {
    calls: Arc<AtomicUsize>,
    changed: Arc<AtomicBool>,
}
impl CredentialProvider for ScopedProvider {
    fn resolve<'a>(&'a mut self, _: CredentialRequest<'a>) -> CredentialFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let changed = self.changed.load(Ordering::SeqCst);
        Box::pin(async move {
            Credential::bearer(
                if changed { "new-token" } else { "old-token" }.into(),
                if changed { "writer" } else { "reader" }.into(),
            )
        })
    }
}

#[tokio::test]
async fn named_commands_wait_for_old_scope_then_rebind_but_old_handles_do_not_cross_identity() {
    let server = server().await;
    let counter = Arc::new(AtomicUsize::new(0));
    let changed = Arc::new(AtomicBool::new(false));
    let provider_calls = counter.clone();
    let identity = changed.clone();
    let remote =
        QueuedRemote::spawn_with(server.config.clone(), QueueOptions::default(), move || {
            Ok(Client::new()?.with_credential_provider(ScopedProvider {
                calls: provider_calls,
                changed: identity,
            }))
        })
        .await
        .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let CallOutcome::NeedsInput(handle) = command_outcome(
        remote
            .execute("echo", "--message interactive")
            .unwrap()
            .await
            .unwrap(),
    ) else {
        panic!()
    };
    let first = binding.submit(&args(&binding, "hold")).unwrap();
    wait_calls(&server.data, 2).await;
    changed.store(true, Ordering::SeqCst);
    let mut named = remote.execute("echo", "--message new").unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(40), &mut named)
            .await
            .is_err()
    );
    assert_eq!(server.data.calls.lock().unwrap().len(), 2);
    server.data.release.notify_one();
    assert_eq!(message(first.await.unwrap()), "hold");
    assert_eq!(message(command_outcome(named.await.unwrap())), "new");
    assert_eq!(
        error(binding.call(&args(&binding, "old-no-wire")).await),
        "BindingStale"
    );
    assert_eq!(
        error(remote.resume(&handle, json!({})).unwrap().await),
        "InvalidContinuation"
    );
    assert!(matches!(
        remote.execute("echo", "--help").unwrap().await.unwrap(),
        Execution::Help(_)
    ));
    assert_eq!(counter.load(Ordering::SeqCst), 8);
    assert_eq!(
        *server.data.authorization.lock().unwrap(),
        ["Bearer old-token", "Bearer old-token", "Bearer new-token"]
    );
    assert_eq!(server.data.calls.lock().unwrap().len(), 3);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn cancelling_a_resume_waiting_for_exclusivity_keeps_its_unissued_handle_usable() {
    let server = server().await;
    let counter = Arc::new(AtomicUsize::new(0));
    let provider_calls = counter.clone();
    let (started, waiting) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let (notified, notification) = tokio::sync::oneshot::channel();
    let remote =
        QueuedRemote::spawn_with(server.config.clone(), QueueOptions::default(), move || {
            let mut notified = Some(notified);
            Ok(Client::new()?
                .with_credential_provider(FourthProvider {
                    calls: provider_calls,
                    started: Some(started),
                    release: Some(gate),
                })
                .with_event_handler(move |event| {
                    let mcpx_native::EventStreamItem::Event(event) = event else {
                        panic!("unexpected core loss in fixture")
                    };
                    if event.payload["method"] == "notifications/tools/list_changed"
                        && let Some(sender) = notified.take()
                    {
                        let _ = sender.send(());
                    }
                }))
        })
        .await
        .unwrap();
    let CallOutcome::NeedsInput(handle) = command_outcome(
        remote
            .execute("echo", "--message interactive")
            .unwrap()
            .await
            .unwrap(),
    ) else {
        panic!()
    };
    let held = remote
        .execute("echo", "--message hold-invalidating")
        .unwrap();
    wait_calls(&server.data, 2).await;
    let resumed = remote.resume(&handle, json!({})).unwrap();
    tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap();
    server.data.invalidation.notify_one();
    tokio::time::timeout(Duration::from_secs(1), notification)
        .await
        .unwrap()
        .unwrap();
    release.send(()).unwrap();
    resumed.cancel();
    assert_eq!(
        error(
            tokio::time::timeout(Duration::from_secs(1), resumed)
                .await
                .unwrap()
        ),
        "Cancelled"
    );
    assert_eq!(server.data.calls.lock().unwrap().len(), 2);
    server.data.release.notify_one();
    assert_eq!(
        message(command_outcome(held.await.unwrap())),
        "hold-invalidating"
    );
    assert_eq!(
        message(remote.resume(&handle, json!({})).unwrap().await.unwrap()),
        "interactive"
    );
    assert_eq!(counter.load(Ordering::SeqCst), 5);
    assert_eq!(server.data.list_calls.load(Ordering::SeqCst), 2);
    remote.close().await.unwrap();
}

#[tokio::test]
async fn named_and_resume_calls_each_keep_the_native_driver_deadline() {
    let server = server().await;
    let events = Arc::new(Mutex::new(Vec::<mcpx_core::Event>::new()));
    let sink = events.clone();
    let remote =
        QueuedRemote::spawn_with(server.config.clone(), QueueOptions::default(), move || {
            Ok(Client::new()?
                .with_timeout(Duration::from_secs(1))
                .with_event_handler(move |event| {
                    let mcpx_native::EventStreamItem::Event(event) = event else {
                        panic!("unexpected core loss in fixture")
                    };
                    sink.lock().unwrap().push(event.clone());
                }))
        })
        .await
        .unwrap();
    remote.set_elicitation(true, false).unwrap().await.unwrap();
    let CallOutcome::NeedsInput(handle) = command_outcome(
        remote
            .execute("echo", "--message interactive-parallel-deadline")
            .unwrap()
            .await
            .unwrap(),
    ) else {
        panic!()
    };
    let resumed = remote
        .resume(
            &handle,
            json!({"confirm":{"action":"accept","content":{"yes":true}}}),
        )
        .unwrap();
    let named = remote.execute("echo", "--message hold").unwrap();
    let ids = [resumed.id(), named.id()];
    wait_calls(&server.data, 3).await;
    assert_eq!(
        message(command_outcome(
            remote
                .execute("echo", "--message independent")
                .unwrap()
                .await
                .unwrap()
        )),
        "independent"
    );
    assert_eq!(
        error(
            tokio::time::timeout(Duration::from_secs(2), resumed)
                .await
                .unwrap()
        ),
        "Timeout"
    );
    assert_eq!(
        error(
            tokio::time::timeout(Duration::from_secs(2), named)
                .await
                .unwrap()
        ),
        "Timeout"
    );
    server.data.release.notify_waiters();
    assert_eq!(
        message(command_outcome(
            remote
                .execute("echo", "--message reused")
                .unwrap()
                .await
                .unwrap()
        )),
        "reused"
    );
    for id in ids {
        let records = events.lock().unwrap();
        let terminal: Vec<_> = records
            .iter()
            .filter(|event| {
                event.queue_operation_id == Some(id)
                    && event.is_terminal()
                    && event.kind != "catalog_updated"
            })
            .collect();
        assert_eq!(terminal.len(), 1);
        assert_eq!(terminal[0].payload["code"], "Timeout");
    }
    remote.close().await.unwrap();
}

#[tokio::test]
async fn lifecycle_subscription_covers_pending_controls_active_calls_and_dropped_futures() {
    use mcpx_core::OperationEventKind as Kind;
    let server = server().await;
    let (remote, mut stream) = QueuedRemote::connect_with_events(
        server.config.clone(),
        serial_options(),
        mcpx_core::EventLimits::default(),
    )
    .await
    .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let active = binding.submit(&args(&binding, "hold")).unwrap();
    let active_id = active.id();
    wait_calls(&server.data, 1).await;
    let pending = remote.complete_line("echo --m", 8).unwrap();
    let pending_id = pending.id();
    pending.cancel();
    let abandoned = remote.validator_cache_stats().unwrap();
    let abandoned_id = abandoned.id();
    drop(abandoned);
    assert_eq!(error(pending.await), "Cancelled");
    active.cancel();
    assert_eq!(error(active.await), "Cancelled");
    let local = remote.command_spec("echo").unwrap();
    let local_id = local.id();
    local.await.unwrap();
    remote.close().await.unwrap();
    let mut records = vec![];
    while let Some(batch) = stream.next_batch().await {
        assert_eq!(batch.dropped, 0);
        records.extend(batch.events);
    }
    assert!(
        records
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence)
    );
    for id in [active_id, pending_id, abandoned_id, local_id] {
        assert_eq!(
            records
                .iter()
                .filter(|event| event.queue_operation_id == id
                    && matches!(
                        event.kind,
                        Kind::Completed | Kind::NeedsInput | Kind::Failed | Kind::Cancelled
                    ))
                .count(),
            1
        );
    }
    let active = records
        .iter()
        .find(|event| event.queue_operation_id == active_id && event.kind == Kind::Cancelled)
        .unwrap();
    assert!(active.core_operation_id.is_some());
    assert_eq!(active.payload["execution_certainty"], "unknown");
    for id in [pending_id, abandoned_id] {
        let event = records
            .iter()
            .find(|event| event.queue_operation_id == id && event.kind == Kind::Cancelled)
            .unwrap();
        assert!(event.core_operation_id.is_none());
        assert_eq!(event.payload["execution_certainty"], "not_sent");
        assert!(
            !records
                .iter()
                .any(|event| event.queue_operation_id == id && event.kind == Kind::Started)
        );
    }
    let event = records
        .iter()
        .find(|event| event.queue_operation_id == local_id && event.kind == Kind::Completed)
        .unwrap();
    assert_eq!(event.payload["execution_certainty"], "not_sent");
    assert_eq!(server.data.calls.lock().unwrap().len(), 1);
    server.data.release.notify_one();
}

#[tokio::test]
async fn lifecycle_terminal_backpressure_precedes_admission_and_receiver_drop_unsubscribes() {
    let server = server().await;
    let (remote, mut stream) = QueuedRemote::connect_with_events(
        server.config.clone(),
        serial_options(),
        mcpx_core::EventLimits {
            capacity: 4,
            max_bytes: 49152,
        },
    )
    .await
    .unwrap();
    for _ in 0..4 {
        remote.validator_cache_stats().unwrap().await.unwrap();
    }
    assert_eq!(error(remote.validator_cache_stats()), "ConsumerLagged");
    let batch = stream.try_next_batch().unwrap();
    assert_eq!(
        batch
            .events
            .iter()
            .filter(|event| event.kind == mcpx_core::OperationEventKind::Completed)
            .count(),
        4
    );
    assert!(batch.dropped > 0);
    let next = remote.validator_cache_stats().unwrap();
    assert_eq!(next.id(), 5);
    next.await.unwrap();
    drop(stream);
    for _ in 0..8 {
        remote.validator_cache_stats().unwrap().await.unwrap();
    }
    remote.close().await.unwrap();
    assert!(server.data.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn concurrent_lifecycle_admission_order_matches_owner_processing_order() {
    use mcpx_core::OperationEventKind as Kind;
    let server = server().await;
    let (remote, mut stream) = QueuedRemote::connect_with_events(
        server.config.clone(),
        QueueOptions::default(),
        mcpx_core::EventLimits::default(),
    )
    .await
    .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(16));
    let callers: Vec<_> = (0..16)
        .map(|_| {
            let remote = remote.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                remote.validator_cache_stats().unwrap()
            })
        })
        .collect();
    for caller in callers {
        caller.join().unwrap().await.unwrap();
    }
    remote.close().await.unwrap();
    let mut records = vec![];
    while let Some(batch) = stream.next_batch().await {
        assert_eq!(batch.dropped, 0);
        records.extend(batch.events);
    }
    let ids = |kind| {
        records
            .iter()
            .filter(|event| event.kind == kind)
            .map(|event| event.queue_operation_id)
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(Kind::Queued), ids(Kind::Started));
    assert_eq!(ids(Kind::Completed).len(), 16);
    assert_eq!(
        ids(Kind::Queued)
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        16
    );
    assert!(server.data.calls.lock().unwrap().is_empty());
}
