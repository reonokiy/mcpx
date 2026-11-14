use mcpx_core::{CallOutcome, Event, Result};
use mcpx_native::{Client, ProtocolMode, QueueOptions, QueuedRemote, ServerConfig};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

static NEXT: AtomicU64 = AtomicU64::new(1);
fn temp(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "mcpx-pipe-{}-{}-{label}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn config(protocol: ProtocolMode, args: Vec<String>) -> ServerConfig {
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/stdio_server.py");
    ServerConfig::Stdio {
        program: "python3".into(),
        args: std::iter::once(fixture.to_string_lossy().into_owned())
            .chain(args)
            .collect(),
        env: BTreeMap::new(),
        working_directory: None,
        protocol,
    }
}
async fn file(path: &PathBuf) -> String {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(text) = tokio::fs::read_to_string(path).await {
                return text;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
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
fn error<T>(result: Result<T>) -> String {
    match result {
        Err(error) => error.code,
        Ok(_) => panic!("expected failure"),
    }
}

#[tokio::test]
async fn sixteen_calls_share_one_child_and_all_arrive_before_any_result_in_each_protocol_mode() {
    for (protocol, legacy) in [
        (ProtocolMode::Modern, false),
        (ProtocolMode::Legacy, true),
        (ProtocolMode::Auto, false),
        (ProtocolMode::Auto, true),
    ] {
        let log = temp("requests");
        let starts = temp("starts");
        let eof = temp("eof");
        let mut argv = vec![
            "--async-calls".into(),
            "--barrier".into(),
            "16".into(),
            "--verbose".into(),
            "--requests-log".into(),
            log.to_string_lossy().into(),
            "--start-count".into(),
            starts.to_string_lossy().into(),
            "--eof-marker".into(),
            eof.to_string_lossy().into(),
        ];
        if legacy {
            argv.push("--legacy".into());
        }
        let remote = QueuedRemote::connect(config(protocol, argv), QueueOptions::default())
            .await
            .unwrap();
        let binding = remote.tool("echo").await.unwrap();
        let mut calls = Vec::new();
        for index in 0..16 {
            calls.push(
                binding
                    .submit_json(json!({"message":format!("batch-{index}")}))
                    .unwrap(),
            );
        }
        tokio::time::timeout(Duration::from_secs(4), async {
            for (index, call) in calls.into_iter().enumerate() {
                assert_eq!(message(call.await.unwrap()), format!("batch-{index}"));
            }
        })
        .await
        .unwrap();
        remote.close().await.unwrap();
        assert_eq!(file(&starts).await, "1");
        assert_eq!(file(&eof).await, "closed");
        let records: Vec<Value> = file(&log)
            .await
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let calls: Vec<_> = records
            .iter()
            .filter(|record| record["method"] == "tools/call")
            .collect();
        assert_eq!(calls.len(), 16);
        assert_eq!(
            calls
                .iter()
                .map(|record| record["pid"].as_u64().unwrap())
                .collect::<BTreeSet<_>>()
                .len(),
            1
        );
        for path in [log, starts, eof] {
            std::fs::remove_file(path).unwrap();
        }
    }
}

#[tokio::test]
async fn cancellation_during_a_partial_utf8_frame_survives_late_data_and_a_catalog_refresh() {
    for protocol in [ProtocolMode::Modern, ProtocolMode::Legacy] {
        let partial = temp("partial");
        let log = temp("requests");
        let eof = temp("eof");
        let remote = QueuedRemote::spawn_with(
            config(
                protocol,
                vec![
                    "--async-calls".into(),
                    "--partial-marker".into(),
                    partial.to_string_lossy().into(),
                    "--requests-log".into(),
                    log.to_string_lossy().into(),
                    "--eof-marker".into(),
                    eof.to_string_lossy().into(),
                ],
            ),
            QueueOptions::default(),
            || Ok(Client::new()?.with_catalog_ttl(Duration::ZERO)),
        )
        .await
        .unwrap();
        let binding = remote.tool("echo").await.unwrap();
        let cancelled = binding
            .submit_json(json!({"message":"partial-🦀"}))
            .unwrap();
        let queue_id = cancelled.id();
        let request_id = file(&partial).await.parse::<u64>().unwrap();
        cancelled.cancel();
        assert_eq!(error(cancelled.await), "Cancelled");
        let next = binding.submit_json(json!({"message":"reused-🦀"})).unwrap();
        assert_eq!(
            message(
                tokio::time::timeout(Duration::from_secs(2), next)
                    .await
                    .unwrap()
                    .unwrap()
            ),
            "reused-🦀"
        );
        let batch = remote.drain_events().unwrap().await.unwrap();
        assert_eq!(batch.dropped, 0);
        let events = batch.events;
        let terminal: Vec<_> = events
            .iter()
            .filter(|event| {
                event.is_terminal()
                    && event.kind != "catalog_updated"
                    && event.queue_operation_id == Some(queue_id)
            })
            .collect();
        assert_eq!(terminal.len(), 1);
        assert_eq!(terminal[0].kind, "cancelled");
        // The TTL refresh is its own core operation, under the same host job.
        assert!(events.iter().any(|event| event.kind == "catalog_updated"
            && event.queue_operation_id == Some(queue_id)
            && event.operation_id != terminal[0].operation_id));
        remote.close().await.unwrap();
        assert_eq!(file(&eof).await, "closed");
        let records: Vec<Value> = file(&log)
            .await
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert!(
            records
                .iter()
                .any(|record| record["method"] == "notifications/cancelled"
                    && record["requestId"] == request_id)
        );
        assert!(
            records
                .iter()
                .filter(|record| record["method"] == "tools/list")
                .count()
                >= 3
        );
        for path in [partial, log, eof] {
            std::fs::remove_file(path).unwrap();
        }
    }
}

#[tokio::test]
async fn a_malformed_shared_frame_fails_all_sixteen_requests_once_and_stops_the_child() {
    for protocol in [ProtocolMode::Modern, ProtocolMode::Legacy] {
        let eof = temp("eof");
        let events = Arc::new(Mutex::new(Vec::<Event>::new()));
        let sink = events.clone();
        let remote = QueuedRemote::spawn_with(
            config(
                protocol,
                vec![
                    "--async-calls".into(),
                    "--invalid".into(),
                    "--barrier".into(),
                    "16".into(),
                    "--eof-marker".into(),
                    eof.to_string_lossy().into(),
                ],
            ),
            QueueOptions::default(),
            move || {
                Ok(Client::new()?.with_event_handler(move |event| {
                    let mcpx_native::EventStreamItem::Event(event) = event else {
                        panic!("unexpected core loss in fixture")
                    };
                    sink.lock().unwrap().push(event.clone());
                }))
            },
        )
        .await
        .unwrap();
        let binding = remote.tool("echo").await.unwrap();
        let mut calls = Vec::new();
        for index in 0..16 {
            calls.push(
                binding
                    .submit_json(json!({"message":format!("batch-{index}")}))
                    .unwrap(),
            );
        }
        for call in calls {
            assert_eq!(error(call.await), "ProtocolError");
        }
        let after = match binding.submit_json(json!({"message":"closed"})) {
            Ok(call) => call.await,
            Err(error) => Err(error),
        };
        assert_eq!(error(after), "TransportClosed");
        remote.close().await.unwrap();
        assert_eq!(file(&eof).await, "closed");
        let events = events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.is_terminal() && event.queue_operation_id.is_some())
                .count(),
            16
        );
        std::fs::remove_file(eof).unwrap();
    }
}

#[tokio::test]
async fn cancelling_a_blocked_write_retains_its_byte_budget_until_transport_cleanup() {
    let marker = temp("blocked");
    let remote = QueuedRemote::connect(
        config(
            ProtocolMode::Modern,
            vec![
                "--block-input-marker".into(),
                marker.to_string_lossy().into(),
            ],
        ),
        QueueOptions {
            max_bytes: 900_000,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let call = binding
        .submit_json(json!({"message":"x".repeat(600_000)}))
        .unwrap();
    file(&marker).await;
    call.cancel();
    assert_eq!(error(call.await), "Cancelled");
    assert_eq!(
        error(binding.submit_json(json!({"message":"y".repeat(600_000)}))),
        "QueueFull"
    );
    tokio::time::timeout(Duration::from_secs(2), remote.close())
        .await
        .unwrap()
        .unwrap();
    std::fs::remove_file(marker).unwrap();
}

fn execution(value: mcpx_native::Execution) -> CallOutcome {
    match value {
        mcpx_native::Execution::Outcome(value) => value,
        _ => panic!("expected tool result"),
    }
}

#[tokio::test]
async fn typed_string_and_argv_calls_reach_the_same_stdio_barrier_in_all_protocol_modes() {
    for (protocol, legacy) in [
        (ProtocolMode::Modern, false),
        (ProtocolMode::Legacy, true),
        (ProtocolMode::Auto, false),
        (ProtocolMode::Auto, true),
    ] {
        let log = temp("commands");
        let eof = temp("command-eof");
        let mut options = vec![
            "--async-calls".into(),
            "--barrier".into(),
            "16".into(),
            "--requests-log".into(),
            log.to_string_lossy().into(),
            "--eof-marker".into(),
            eof.to_string_lossy().into(),
        ];
        if legacy {
            options.push("--legacy".into());
        }
        let remote = QueuedRemote::connect(config(protocol, options), QueueOptions::default())
            .await
            .unwrap();
        let binding = remote.tool("echo").await.unwrap();
        let mut calls: Vec<
            std::pin::Pin<Box<dyn std::future::Future<Output = Result<CallOutcome>> + Send>>,
        > = Vec::new();
        for index in 0..16 {
            let value = format!("batch-🦀 {index} $HOME $(literal)");
            if index % 3 == 0 {
                calls.push(Box::pin(
                    binding.submit_json(json!({"message":value})).unwrap(),
                ));
            } else {
                let call = if index % 3 == 1 {
                    remote.execute("echo", &format!("--message '{value}'"))
                } else {
                    remote.execute_argv("echo", &["--message".into(), value])
                }
                .unwrap();
                calls.push(Box::pin(async move { call.await.map(execution) }));
            }
        }
        tokio::time::timeout(Duration::from_secs(4), async {
            for (index, call) in calls.into_iter().enumerate() {
                assert_eq!(
                    message(call.await.unwrap()),
                    format!("batch-🦀 {index} $HOME $(literal)")
                );
            }
        })
        .await
        .unwrap();
        remote.close().await.unwrap();
        let records: Vec<Value> = file(&log)
            .await
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            records
                .iter()
                .filter(|record| record["method"] == "tools/call")
                .count(),
            16
        );
        assert_eq!(
            records
                .iter()
                .map(|record| record["pid"].as_u64().unwrap())
                .collect::<BTreeSet<_>>()
                .len(),
            1
        );
        assert_eq!(file(&eof).await, "closed");
        for path in [log, eof] {
            std::fs::remove_file(path).unwrap();
        }
    }
}

#[tokio::test]
async fn sixteen_stdio_form_resumes_share_the_child_and_cancellation_does_not_stop_the_others() {
    for protocol in [ProtocolMode::Modern, ProtocolMode::Auto] {
        let log = temp("resume");
        let eof = temp("resume-eof");
        let events = Arc::new(Mutex::new(Vec::<Event>::new()));
        let sink = events.clone();
        let remote = QueuedRemote::spawn_with(
            config(
                protocol,
                vec![
                    "--async-calls".into(),
                    "--interactive".into(),
                    "--resume-barrier".into(),
                    "16".into(),
                    "--delay".into(),
                    "10".into(),
                    "--selective-delay".into(),
                    "--requests-log".into(),
                    log.to_string_lossy().into(),
                    "--eof-marker".into(),
                    eof.to_string_lossy().into(),
                ],
            ),
            QueueOptions::default(),
            move || {
                Ok(Client::new()?.with_event_handler(move |event| {
                    let mcpx_native::EventStreamItem::Event(event) = event else {
                        panic!("unexpected core loss in fixture")
                    };
                    sink.lock().unwrap().push(event.clone());
                }))
            },
        )
        .await
        .unwrap();
        remote.set_elicitation(true, false).unwrap().await.unwrap();
        let mut initial = Vec::new();
        for index in 0..16 {
            let value = if index == 0 {
                "interactive-slow".into()
            } else {
                format!("interactive-{index}-🦀")
            };
            initial.push(
                remote
                    .execute_argv("echo", &["--message".into(), value])
                    .unwrap(),
            );
        }
        let mut handles = Vec::new();
        for call in initial {
            let CallOutcome::NeedsInput(handle) = execution(call.await.unwrap()) else {
                panic!()
            };
            handles.push(handle);
        }
        assert_eq!(
            error(
                remote
                    .resume(
                        &handles[0],
                        json!({"confirm":{"action":"accept","content":{"yes":"wrong"}}})
                    )
                    .unwrap()
                    .await
            ),
            "ValidationFailed"
        );
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
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let records = tokio::fs::read_to_string(&log).await.unwrap();
                if records
                    .lines()
                    .filter(|line| {
                        serde_json::from_str::<Value>(line).unwrap()["method"] == "tools/call"
                    })
                    .count()
                    == 32
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let cancelled = resumes.remove(0);
        let cancelled_id = cancelled.id();
        cancelled.cancel();
        assert_eq!(error(cancelled.await), "Cancelled");
        tokio::time::timeout(Duration::from_secs(3), async {
            for (index, call) in resumes.into_iter().enumerate() {
                assert_eq!(
                    message(call.await.unwrap()),
                    format!("interactive-{}-🦀", index + 1)
                );
            }
        })
        .await
        .unwrap();
        assert_eq!(
            error(
                remote
                    .resume(&handles[0], json!({"confirm":{"action":"cancel"}}))
                    .unwrap()
                    .await
            ),
            "InvalidContinuation"
        );
        assert_eq!(
            message(execution(
                remote
                    .execute("echo", "--message 'child reused'")
                    .unwrap()
                    .await
                    .unwrap()
            )),
            "child reused"
        );
        remote.close().await.unwrap();
        {
            let events = events.lock().unwrap();
            let terminal: Vec<_> = events
                .iter()
                .filter(|event| {
                    event.queue_operation_id == Some(cancelled_id) && event.is_terminal()
                })
                .collect();
            assert_eq!(terminal.len(), 1);
            assert_eq!(terminal[0].kind, "cancelled");
        }
        assert_eq!(file(&eof).await, "closed");
        let records: Vec<Value> = file(&log)
            .await
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            records
                .iter()
                .filter(|record| record["method"] == "tools/call")
                .count(),
            33
        );
        assert_eq!(
            records
                .iter()
                .map(|record| record["pid"].as_u64().unwrap())
                .collect::<BTreeSet<_>>()
                .len(),
            1
        );
        for path in [log, eof] {
            std::fs::remove_file(path).unwrap();
        }
    }
}

#[tokio::test]
async fn automatic_stdio_recovery_never_replays_sixteen_unknown_calls_and_rebinds_new_schema() {
    for (mode, legacy) in [
        (ProtocolMode::Modern, false),
        (ProtocolMode::Legacy, true),
        (ProtocolMode::Auto, false),
    ] {
        let starts = temp("reconnect-starts");
        let log = temp("reconnect-log");
        let mut argv = vec![
            "--async-calls".into(),
            "--selective-delay".into(),
            "--delay".into(),
            "60".into(),
            "--exit-after-calls".into(),
            "16".into(),
            "--integer-after-restart".into(),
            "--start-count".into(),
            starts.to_string_lossy().into(),
            "--requests-log".into(),
            log.to_string_lossy().into(),
        ];
        if legacy {
            argv.push("--legacy".into());
        }
        let (remote, mut events) = QueuedRemote::spawn_with_events(
            config(mode, argv),
            QueueOptions::default(),
            mcpx_core::EventLimits::default(),
            || {
                Client::new()?.with_stdio_reconnect(mcpx_native::StdioReconnectPolicy {
                    max_attempts: 2,
                    backoff: Duration::from_millis(5),
                    timeout: Duration::from_secs(5),
                })
            },
        )
        .await
        .unwrap();
        let binding = remote.tool("echo").await.unwrap();
        let calls: Vec<_> = (0..16)
            .map(|_| binding.submit_json(json!({"message":"slow"})).unwrap())
            .collect();
        let ids: Vec<_> = calls.iter().map(|call| call.id()).collect();
        let next = remote
            .execute_argv("echo", &["--message".into(), "7".into()])
            .unwrap();
        for call in calls {
            let error = tokio::time::timeout(Duration::from_secs(5), call)
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, "TransportClosed");
            assert_eq!(
                error.execution_certainty,
                mcpx_core::ExecutionCertainty::Unknown
            );
        }
        let outcome = tokio::time::timeout(Duration::from_secs(5), next)
            .await
            .unwrap()
            .unwrap();
        let mcpx_native::Execution::Outcome(CallOutcome::Completed(response)) = outcome else {
            panic!()
        };
        assert_eq!(response.raw["structuredContent"]["message"], 7);
        let stale = binding
            .submit_json(json!({"message":"must-not-replay"}))
            .unwrap()
            .await
            .unwrap_err();
        assert_eq!(stale.code, "BindingStale");
        assert_eq!(
            stale.execution_certainty,
            mcpx_core::ExecutionCertainty::NotSent
        );
        let new = remote.tool("echo").await.unwrap();
        assert!(new.submit_json(json!({"message":8})).unwrap().await.is_ok());
        remote.close().await.unwrap();
        let records: Vec<Value> = std::fs::read_to_string(&log)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            records
                .iter()
                .filter(|record| record["method"] == "tools/call")
                .count(),
            18
        );
        assert_eq!(
            records
                .iter()
                .filter(|record| record["method"] == "tools/call")
                .map(|record| record["pid"].as_u64().unwrap())
                .collect::<BTreeSet<_>>()
                .len(),
            2
        );
        let rpc: Vec<_> = records
            .iter()
            .filter_map(|record| record["id"].as_u64())
            .collect();
        assert_eq!(
            rpc.len(),
            rpc.iter().copied().collect::<BTreeSet<_>>().len()
        );
        assert_eq!(std::fs::read_to_string(&starts).unwrap(), "2");
        let mut lifecycle = vec![];
        while let Some(batch) = events.next_batch().await {
            assert_eq!(batch.dropped, 0);
            lifecycle.extend(batch.events);
        }
        for id in ids {
            assert_eq!(
                lifecycle
                    .iter()
                    .filter(|event| event.queue_operation_id == id
                        && event.kind == mcpx_core::OperationEventKind::Failed)
                    .count(),
                1
            );
        }
        std::fs::remove_file(starts).unwrap();
        std::fs::remove_file(log).unwrap();
    }
}

#[tokio::test]
async fn stdio_recovery_rounds_are_bounded_and_fatal_protocol_errors_do_not_restart() {
    for invalid in [false, true] {
        let starts = temp("recovery-failed-starts");
        let mut argv = vec![
            "--async-calls".into(),
            "--start-count".into(),
            starts.to_string_lossy().into(),
            "--fail-restarts".into(),
            "8".into(),
        ];
        if invalid {
            argv.push("--invalid".into());
        } else {
            argv.extend(["--exit-after-calls".into(), "1".into()]);
        }
        let remote = QueuedRemote::spawn_with(
            config(ProtocolMode::Modern, argv),
            QueueOptions::default(),
            || {
                Client::new()?.with_stdio_reconnect(mcpx_native::StdioReconnectPolicy {
                    max_attempts: 2,
                    backoff: Duration::from_millis(5),
                    timeout: Duration::from_secs(2),
                })
            },
        )
        .await
        .unwrap();
        let binding = remote.tool("echo").await.unwrap();
        let error = binding
            .submit_json(json!({"message":"once"}))
            .unwrap()
            .await
            .unwrap_err();
        assert_eq!(
            error.code,
            if invalid {
                "ProtocolError"
            } else {
                "TransportClosed"
            }
        );
        let stopped = tokio::time::timeout(Duration::from_secs(4), async {
            match remote.validator_cache_stats() {
                Ok(operation) => operation.await,
                Err(error) => Err(error),
            }
        })
        .await
        .unwrap();
        assert_eq!(stopped.unwrap_err().code, "TransportClosed");
        remote.close().await.unwrap();
        assert_eq!(
            std::fs::read_to_string(&starts).unwrap(),
            if invalid { "1" } else { "3" }
        );
        std::fs::remove_file(starts).unwrap();
    }
}

#[tokio::test]
async fn recovery_deadline_keeps_pending_cancellation_and_expiry_live_without_business_dispatch() {
    let starts = temp("recovery-deadline-starts");
    let log = temp("recovery-deadline-log");
    let (remote, mut events) = QueuedRemote::spawn_with_events(
        config(
            ProtocolMode::Modern,
            vec![
                "--exit-after-calls".into(),
                "1".into(),
                "--restart-discovery-delay".into(),
                "10".into(),
                "--start-count".into(),
                starts.to_string_lossy().into(),
                "--requests-log".into(),
                log.to_string_lossy().into(),
            ],
        ),
        QueueOptions::default(),
        mcpx_core::EventLimits::default(),
        || {
            Client::new()?.with_stdio_reconnect(mcpx_native::StdioReconnectPolicy {
                max_attempts: 8,
                backoff: Duration::from_secs(1),
                timeout: Duration::from_millis(500),
            })
        },
    )
    .await
    .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    assert_eq!(
        binding
            .submit_json(json!({"message":"once"}))
            .unwrap()
            .await
            .unwrap_err()
            .code,
        "TransportClosed"
    );
    let expired_args = binding.args_from(json!({"message":"never"})).unwrap();
    let expired = binding
        .submit_with_timeout(&expired_args, Duration::from_millis(25))
        .unwrap();
    let expired_id = expired.id();
    let cancelled = remote.command_spec("echo").unwrap();
    let cancel_id = cancelled.id();
    cancelled.cancel();
    assert_eq!(cancelled.await.unwrap_err().code, "Cancelled");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), expired)
            .await
            .unwrap()
            .unwrap_err()
            .code,
        "Timeout"
    );
    let before = std::time::Instant::now();
    remote.close().await.unwrap();
    assert!(before.elapsed() < Duration::from_secs(2));
    let mut records = vec![];
    while let Some(batch) = events.next_batch().await {
        records.extend(batch.events);
    }
    for id in [expired_id, cancel_id] {
        let event = records
            .iter()
            .find(|event| {
                event.queue_operation_id == id
                    && matches!(
                        event.kind,
                        mcpx_core::OperationEventKind::Failed
                            | mcpx_core::OperationEventKind::Cancelled
                    )
            })
            .unwrap();
        assert_eq!(event.payload["execution_certainty"], "not_sent");
    }
    let requests: Vec<Value> = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["method"] == "tools/call")
            .count(),
        1
    );
    assert!(
        std::fs::read_to_string(&starts)
            .unwrap()
            .parse::<u32>()
            .unwrap()
            <= 2
    );
    std::fs::remove_file(starts).unwrap();
    std::fs::remove_file(log).unwrap();
}

#[tokio::test]
async fn explicit_queued_reconnect_replaces_shared_io_reprobes_auto_and_invalidates_input() {
    let starts = temp("explicit-queue-reconnect");
    let log = temp("explicit-queue-reconnect-log");
    let (remote, mut events) = QueuedRemote::connect_with_events(
        config(
            ProtocolMode::Auto,
            vec![
                "--async-calls".into(),
                "--interactive".into(),
                "--legacy-after-restart".into(),
                "--start-count".into(),
                starts.to_string_lossy().into(),
                "--requests-log".into(),
                log.to_string_lossy().into(),
            ],
        ),
        QueueOptions::default(),
        mcpx_core::EventLimits::default(),
    )
    .await
    .unwrap();
    remote.set_elicitation(true, false).unwrap().await.unwrap();
    let binding = remote.tool("echo").await.unwrap();
    let CallOutcome::NeedsInput(handle) = binding
        .submit_json(json!({"message":"interactive-before"}))
        .unwrap()
        .await
        .unwrap()
    else {
        panic!()
    };
    let reconnect = remote.reconnect().unwrap();
    let id = reconnect.id();
    reconnect.await.unwrap();
    assert_eq!(
        binding
            .submit_json(json!({"message":"stale"}))
            .unwrap()
            .await
            .unwrap_err()
            .code,
        "BindingStale"
    );
    assert_eq!(
        remote
            .resume(
                &handle,
                json!({"confirm":{"action":"accept","content":{"yes":true}}})
            )
            .unwrap()
            .await
            .unwrap_err()
            .code,
        "InvalidContinuation"
    );
    let result = remote
        .execute_argv("echo", &["--message".into(), "new-writer".into()])
        .unwrap()
        .await
        .unwrap();
    let mcpx_native::Execution::Outcome(CallOutcome::Completed(response)) = result else {
        panic!()
    };
    assert_eq!(response.raw["structuredContent"]["message"], "new-writer");
    remote.close().await.unwrap();
    let mut records = vec![];
    while let Some(batch) = events.next_batch().await {
        records.extend(batch.events);
    }
    assert_eq!(
        records
            .iter()
            .filter(|event| event.queue_operation_id == id
                && event.kind == mcpx_core::OperationEventKind::Completed)
            .count(),
        1
    );
    let requests: Vec<Value> = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["method"] == "tools/call")
            .count(),
        2
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["method"] == "initialize")
            .count(),
        1
    );
    assert_eq!(std::fs::read_to_string(&starts).unwrap(), "2");
    std::fs::remove_file(starts).unwrap();
    std::fs::remove_file(log).unwrap();
}

#[tokio::test]
async fn repeated_successful_metadata_then_eof_cannot_restart_forever() {
    let starts = temp("flapping-starts");
    let remote = QueuedRemote::spawn_with(
        config(
            ProtocolMode::Modern,
            vec![
                "--exit-after-calls".into(),
                "1".into(),
                "--restart-exit-after-list".into(),
                "--start-count".into(),
                starts.to_string_lossy().into(),
            ],
        ),
        QueueOptions::default(),
        || {
            Client::new()?.with_stdio_reconnect(mcpx_native::StdioReconnectPolicy {
                max_attempts: 2,
                backoff: Duration::from_millis(10),
                timeout: Duration::from_secs(2),
            })
        },
    )
    .await
    .unwrap();
    let binding = remote.tool("echo").await.unwrap();
    assert!(
        binding
            .submit_json(json!({"message":"once"}))
            .unwrap()
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match remote.validator_cache_stats() {
                Err(error) => {
                    assert_eq!(error.code, "TransportClosed");
                    break;
                }
                Ok(operation) => {
                    if let Err(error) = operation.await {
                        assert_eq!(error.code, "TransportClosed");
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    remote.close().await.unwrap();
    assert_eq!(std::fs::read_to_string(&starts).unwrap(), "3");
    std::fs::remove_file(starts).unwrap();
}

#[tokio::test]
async fn a_real_tool_result_resets_the_consecutive_recovery_budget() {
    let starts = temp("successful-recovery-budget");
    let remote = QueuedRemote::spawn_with(
        config(
            ProtocolMode::Modern,
            vec![
                "--exit-message".into(),
                "crash".into(),
                "--start-count".into(),
                starts.to_string_lossy().into(),
            ],
        ),
        QueueOptions::default(),
        || {
            Client::new()?.with_stdio_reconnect(mcpx_native::StdioReconnectPolicy {
                max_attempts: 1,
                backoff: Duration::ZERO,
                timeout: Duration::from_secs(3),
            })
        },
    )
    .await
    .unwrap();
    for _ in 0..2 {
        let binding = remote.tool("echo").await.unwrap();
        assert_eq!(
            binding
                .submit_json(json!({"message":"crash"}))
                .unwrap()
                .await
                .unwrap_err()
                .code,
            "TransportClosed"
        );
        let new = remote
            .execute_argv("echo", &["--message".into(), "healthy".into()])
            .unwrap();
        let mcpx_native::Execution::Outcome(CallOutcome::Completed(response)) =
            tokio::time::timeout(Duration::from_secs(4), new)
                .await
                .unwrap()
                .unwrap()
        else {
            panic!()
        };
        assert_eq!(response.raw["structuredContent"]["message"], "healthy");
    }
    remote.close().await.unwrap();
    assert_eq!(std::fs::read_to_string(&starts).unwrap(), "3");
    std::fs::remove_file(starts).unwrap();
}
