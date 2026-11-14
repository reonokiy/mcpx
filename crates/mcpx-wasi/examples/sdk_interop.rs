//! Standalone guest exercised against the pinned official Rust MCP SDK.
#[cfg(all(target_os = "wasi", target_env = "p2"))]
fn main() -> mcpx_core::Result<()> {
    use mcpx_core::{
        CallOutcome, Credential, Dialect, Error, ExecutionCertainty, HostRuntime,
        NotificationPhase, OutputValidation,
    };
    use mcpx_wasi::{Execution, WasiDriver, WasiQueue, WasiQueueOptions};
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::{
        future::{Future, poll_fn},
        pin::Pin,
        task::{Context, Poll, Waker},
        time::Duration,
    };
    #[derive(Serialize)]
    struct Echo {
        message: String,
    }
    #[derive(Deserialize)]
    struct EchoResult {
        message: String,
    }
    fn message(value: CallOutcome) -> String {
        let CallOutcome::Completed(value) = value else {
            panic!("expected completed SDK result")
        };
        assert!(matches!(value.output_validation, OutputValidation::Valid));
        value.deserialize::<EchoResult>().unwrap().message
    }
    fn execution(value: Execution) -> CallOutcome {
        let Execution::Outcome(value) = value else {
            panic!("expected call")
        };
        value
    }
    let mut args = std::env::args().skip(1);
    let endpoint = args.next().unwrap();
    let protocol = args.next().unwrap();
    let scenario = args.next().unwrap();
    let dialect = if protocol.starts_with("auto") {
        Dialect::Auto
    } else if protocol == "legacy" {
        Dialect::Legacy
    } else {
        Dialect::Modern
    };
    let mut reactor = WasiQueue::new(
        WasiDriver::new(
            Credential::bearer("wasi-sdk-fixture".into(), "wasi-sdk".into())?,
            Duration::from_secs(8),
        )?,
        HostRuntime::with_dialect(endpoint, dialect)?,
        WasiQueueOptions::default(),
    )?
    .into_reactor();
    let remote = reactor.remote();
    if scenario == "tls-reject" || scenario == "modern-reject" {
        let error = reactor
            .run(async { remote.bind("echo")?.await })
            .err()
            .unwrap();
        assert_eq!(
            error.code,
            if scenario == "tls-reject" {
                "HttpError"
            } else {
                "UnsupportedProtocolVersion"
            }
        );
        assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
        reactor.close()?;
        println!("{}", json!({"result":scenario}));
        return Ok(());
    }
    if scenario == "manual" {
        let mut future = std::pin::pin!(async {
            let echo = remote.bind("echo")?.await?;
            assert_eq!(
                message(
                    remote
                        .submit_typed(
                            &echo,
                            Echo {
                                message: "wasi-manual".into()
                            }
                        )?
                        .await?
                ),
                "wasi-manual"
            );
            Ok::<_, Error>(())
        });
        let deadline = wasip2::clocks::monotonic_clock::now() + 8_000_000_000;
        loop {
            if let Poll::Ready(result) = future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            {
                result?;
                break;
            }
            assert!(
                wasip2::clocks::monotonic_clock::now() < deadline,
                "SDK external poll stalled: {:?}",
                remote.stats()?
            );
            reactor.poll(Duration::ZERO)?;
            wasip2::clocks::monotonic_clock::subscribe_duration(1_000_000).block();
        }
    } else {
        reactor.run(async {
            let echo = remote.bind("echo")?.await?;
            assert_eq!(
                remote
                    .submit_json(&echo, json!({"message":1}))
                    .unwrap_err()
                    .code,
                "ValidationFailed"
            );
            match scenario.as_str() {
                "reconnect" => {
                    assert_eq!(
                        message(
                            remote
                                .submit_json(&echo, json!({"message":"wasi-before-reconnect"}))?
                                .await?
                        ),
                        "wasi-before-reconnect"
                    );
                    remote.reconnect()?.await?;
                    assert!(!remote.is_suspended()?);
                    let error = remote
                        .submit_json(&echo, json!({"message":"never-old-reconnect"}))
                        .unwrap_err();
                    assert_eq!(error.code, "BindingStale");
                    assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
                    let fresh = remote.bind("echo")?.await?;
                    assert_eq!(
                        message(
                            remote
                                .submit_typed(
                                    &fresh,
                                    Echo {
                                        message: "wasi-after-reconnect".into()
                                    }
                                )?
                                .await?
                        ),
                        "wasi-after-reconnect"
                    );
                }
                "serial" | "cache-zero" => {
                    assert_eq!(
                        message(
                            remote
                                .submit_json(&echo, json!({"message":"wasi-json-terminal"}))?
                                .await?
                        ),
                        "wasi-json-terminal"
                    );
                    assert_eq!(
                        message(
                            remote
                                .submit_typed(
                                    &echo,
                                    Echo {
                                        message: "wasi-typed".into()
                                    }
                                )?
                                .await?
                        ),
                        "wasi-typed"
                    );
                    assert_eq!(
                        message(
                            remote
                                .submit_json(&echo, json!({"message":"wasi-json"}))?
                                .await?
                        ),
                        "wasi-json"
                    );
                    assert_eq!(
                        message(
                            remote
                                .submit_raw(&echo, r#"{"message":"wasi-raw"}"#)?
                                .await?
                        ),
                        "wasi-raw"
                    );
                    assert_eq!(
                        message(execution(
                            remote.execute("echo", "--message 'wasi string'")?.await?
                        )),
                        "wasi string"
                    );
                    assert_eq!(
                        message(execution(
                            remote
                                .execute_argv("echo", &["--message".into(), "wasi argv".into()])?
                                .await?
                        )),
                        "wasi argv"
                    );
                    remote.command_spec("echo")?.await?;
                    remote.complete_line("echo --m", 8)?.await?;
                }
                "parallel" => {
                    let mut calls: Vec<
                        Pin<Box<dyn Future<Output = mcpx_core::Result<CallOutcome>>>>,
                    > = Vec::new();
                    for index in 0..16 {
                        let text = format!("wasi-parallel-{index}");
                        if index % 3 == 0 {
                            calls.push(Box::pin(
                                remote.submit_typed(&echo, Echo { message: text })?,
                            ));
                        } else {
                            let call = if index % 3 == 1 {
                                remote.execute("echo", &format!("--message {text}"))?
                            } else {
                                remote.execute_argv("echo", &["--message".into(), text])?
                            };
                            calls.push(Box::pin(async move { Ok(execution(call.await?)) }));
                        }
                    }
                    let last = remote.submit_json(&echo, json!({"message":"wasi-last"}))?;
                    for (index, call) in calls.into_iter().enumerate() {
                        assert_eq!(message(call.await?), format!("wasi-parallel-{index}"));
                    }
                    assert_eq!(message(last.await?), "wasi-last");
                }
                "resume" => {
                    remote.set_elicitation(true, false)?.await?;
                    let mut initial = Vec::new();
                    for _ in 0..16 {
                        initial.push(remote.submit_json(&echo, json!({"message":"interactive"}))?);
                    }
                    let mut resumed = Vec::new();
                    for call in initial {
                        let CallOutcome::NeedsInput(handle) = call.await? else {
                            panic!("SDK interaction required")
                        };
                        resumed.push(remote.resume(
                            &handle,
                            json!({"confirm":{"action":"accept","content":{"yes":true}}}),
                        )?);
                    }
                    for call in resumed {
                        assert_eq!(message(call.await?), "interactive");
                    }
                }
                "timeout" => {
                    let interrupted =
                        remote.submit_json(&echo, json!({"message":"wasi-timeout"}))?;
                    remote.set_timeout(&interrupted, Duration::from_millis(350))?;
                    let peer = remote.submit_json(&echo, json!({"message":"wasi-independent"}))?;
                    assert_eq!(message(peer.await?), "wasi-independent");
                    let error = interrupted.await.unwrap_err();
                    assert_eq!(error.code, "Timeout");
                    assert_eq!(error.execution_certainty, ExecutionCertainty::Unknown);
                }
                "notifications" => {
                    let mut watcher = remote.subscribe_catalog()?;
                    remote.start_notifications()?;
                    poll_fn(|_| {
                        let status = remote.notification_status().unwrap().unwrap();
                        assert_ne!(
                            status.phase,
                            NotificationPhase::Failed,
                            "SDK notification failure: {status:?}"
                        );
                        if status.phase == NotificationPhase::Listening {
                            Poll::Ready(())
                        } else {
                            Poll::Pending
                        }
                    })
                    .await;
                    assert_eq!(
                        message(
                            remote
                                .submit_json(&echo, json!({"message":"wasi-notify"}))?
                                .await?
                        ),
                        "wasi-notify"
                    );
                    assert!(watcher.changed().await.unwrap().invalidated);
                    let fresh = remote.bind("echo")?.await?;
                    assert_eq!(
                        message(
                            remote
                                .submit_json(&fresh, json!({"message":"wasi-after-notify"}))?
                                .await?
                        ),
                        "wasi-after-notify"
                    );
                    remote.stop_notifications()?;
                }
                _ => panic!("unknown scenario"),
            }
            Ok(())
        })?;
    }
    let events = remote.drain_events()?;
    assert!(
        events.events.iter().any(|event| event.kind == "progress"),
        "SDK progress was lost"
    );
    assert_eq!(remote.stats()?.input_bytes, 0);
    reactor.close()?;
    println!("{}", json!({"result":scenario}));
    Ok(())
}
#[cfg(not(all(target_os = "wasi", target_env = "p2")))]
fn main() {
    eprintln!("build this example for wasm32-wasip2");
}
