//! Fixture-driven asynchronous workflows using weak WASI submission handles.
#[cfg(all(target_os = "wasi", target_env = "p2"))]
fn main() -> mcpx_core::Result<()> {
    use mcpx_core::{
        CallOutcome, Client, Credential, CredentialFuture, CredentialProvider, CredentialRequest,
        Dialect, Error, ExecutionCertainty, HostRuntime, NotificationPhase,
    };
    use mcpx_wasi::{
        Execution, WasiClientExt, WasiDriver, WasiQueue, WasiQueueOptions, WasiRemote, run_blocking,
    };
    use serde::{Serialize, ser::SerializeStruct};
    use serde_json::json;
    use std::{
        cell::{Cell, RefCell},
        future::poll_fn,
        io::Write,
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
        time::Duration,
    };
    #[derive(Default)]
    struct Shared {
        remote: RefCell<Option<WasiRemote>>,
        resolutions: Cell<usize>,
        drops: Cell<usize>,
        events: Cell<usize>,
        serializers: Cell<usize>,
    }
    struct Provider(Rc<Shared>);
    impl Drop for Provider {
        fn drop(&mut self) {
            self.0.drops.set(self.0.drops.get() + 1);
        }
    }
    impl CredentialProvider for Provider {
        fn resolve<'a>(&'a mut self, _: CredentialRequest<'a>) -> CredentialFuture<'a> {
            self.0.resolutions.set(self.0.resolutions.get() + 1);
            assert_eq!(
                self.0
                    .remote
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .refresh()
                    .unwrap_err()
                    .code,
                "Busy"
            );
            let mut yielded = false;
            Box::pin(poll_fn(move |cx| {
                if !yielded {
                    yielded = true;
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(Credential::bearer(
                    "notify-snapshot".into(),
                    "reactor".into(),
                ))
            }))
        }
    }
    struct Typed<'a>(&'a Shared, &'a str);
    impl Serialize for Typed<'_> {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            assert_eq!(
                self.0
                    .remote
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .stats()
                    .unwrap_err()
                    .code,
                "Busy"
            );
            self.0.serializers.set(self.0.serializers.get() + 1);
            let mut value = serializer.serialize_struct("Echo", 1)?;
            value.serialize_field("message", self.1)?;
            value.end()
        }
    }
    fn stage(name: &str) {
        println!("{}", json!({"stage":name}));
        std::io::stdout().flush().unwrap();
    }
    async fn handshake(name: &str) {
        stage(name);
        let input = wasip2::cli::stdin::get_stdin();
        let mut bytes = Vec::new();
        poll_fn(|_| {
            bytes.extend(input.read(16).unwrap());
            if bytes.ends_with(b"\n") {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        assert_eq!(bytes, b"go\n");
    }
    fn completed(value: CallOutcome) {
        assert!(matches!(value, CallOutcome::Completed(_)));
    }
    fn executed(value: Execution) {
        let Execution::Outcome(value) = value else {
            panic!("expected call")
        };
        completed(value);
    }
    #[derive(Default)]
    struct CountWake(AtomicUsize);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
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
    let shared = Rc::<Shared>::default();
    let observed = shared.clone();
    let driver = WasiDriver::new(
        Credential::bearer("notify-snapshot".into(), "reactor".into())?,
        Duration::from_secs(5),
    )?
    .with_event_handler(move |batch| {
        if batch
            .events
            .iter()
            .any(|event| event.kind == "catalog_stale")
        {
            assert_eq!(
                observed
                    .remote
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .refresh()
                    .unwrap_err()
                    .code,
                "Busy"
            );
            observed.events.set(observed.events.get() + 1);
        }
    });
    let mut queue = WasiQueue::new(
        driver,
        HostRuntime::with_dialect(endpoint.clone(), dialect)?,
        WasiQueueOptions::default(),
    )?;
    queue.set_credential_provider(Provider(shared.clone()), None)?;
    let mut transferred = if scenario == "transfer" {
        Some(queue.bind("echo")?)
    } else {
        None
    };
    let mut reactor = queue.into_reactor();
    let remote = reactor.remote();
    *shared.remote.borrow_mut() = Some(remote.clone());
    assert_eq!(shared.resolutions.get(), 0);
    let late = Rc::new(RefCell::new(None::<Waker>));
    if scenario == "idle" {
        let marker = Rc::new(Cell::new(0));
        let start = wasip2::clocks::monotonic_clock::now();
        let result = reactor.run(async {
            poll_fn(|cx| {
                *late.borrow_mut() = Some(cx.waker().clone());
                marker.set(marker.get() + 1);
                if marker.get() == 3 {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            assert_eq!(remote.stats()?.input_bytes, 0);
            Ok(marker.get())
        })?;
        assert_eq!(result, 3);
        assert!(
            wasip2::clocks::monotonic_clock::now() - start >= 10_000_000,
            "idle future busy-looped"
        );
        assert_eq!(shared.resolutions.get(), 0);
    } else if scenario == "direct-zero" {
        let mut direct = Client::new(
            WasiDriver::new(
                Credential::bearer("notify-snapshot".into(), "direct-zero".into())?,
                Duration::from_secs(5),
            )?,
            HostRuntime::with_dialect(endpoint, dialect)?,
        );
        run_blocking(direct.fresh_tool("echo"))?;
        let mut watcher = direct.subscribe_catalog()?;
        direct.start_notifications()?;
        let deadline = wasip2::clocks::monotonic_clock::now() + 5_000_000_000;
        loop {
            direct.poll_notifications(Duration::ZERO)?;
            if direct
                .notification_status()
                .is_some_and(|status| status.phase == NotificationPhase::Listening)
            {
                break;
            }
            assert!(
                wasip2::clocks::monotonic_clock::now() < deadline,
                "direct zero-wait listener stalled: {:?}",
                direct.notification_status()
            );
            wasip2::clocks::monotonic_clock::subscribe_duration(1_000_000).block();
        }
        stage("listening");
        loop {
            direct.poll_notifications(Duration::ZERO)?;
            if let Poll::Ready(Some(change)) = watcher.try_changed() {
                assert!(change.invalidated);
                break;
            }
            assert!(wasip2::clocks::monotonic_clock::now() < deadline);
            wasip2::clocks::monotonic_clock::subscribe_duration(1_000_000).block();
        }
        direct.stop_notifications()?;
        run_blocking(direct.close())?;
        assert_eq!(shared.resolutions.get(), 0);
    } else if scenario == "transfer" {
        let other = WasiQueue::new(
            WasiDriver::new(Credential::anonymous(), Duration::from_secs(5))?,
            HostRuntime::with_dialect(endpoint, dialect)?,
            WasiQueueOptions::default(),
        )?;
        let mut other = other.into_reactor();
        assert_eq!(
            other
                .remote()
                .set_timeout(transferred.as_ref().unwrap(), Duration::from_millis(200))
                .unwrap_err()
                .code,
            "InvalidHandle"
        );
        let binding = reactor.run(async { transferred.take().unwrap().await })?;
        completed(reactor.run(async {
            remote
                .submit_json(&binding, json!({"message":"transferred"}))?
                .await
        })?);
        other.close()?;
    } else if scenario == "cleanup" {
        reactor.run(async { remote.bind("echo")?.await })?;
        assert_eq!(reactor.close().unwrap_err().code, "SessionCleanupFailed");
        assert_eq!(shared.drops.get(), 1);
        assert_eq!(remote.bind("echo").err().unwrap().code, "TransportClosed");
        reactor.close()?;
        assert_eq!(shared.drops.get(), 1);
        println!(
            "{}",
            json!({"result":scenario,"resolutions":shared.resolutions.get()})
        );
        return Ok(());
    } else if scenario == "manual" {
        let count = Arc::<CountWake>::default();
        let waker = Waker::from(count.clone());
        let mut future = std::pin::pin!(async {
            let binding = remote.bind("echo")?.await?;
            completed(
                remote
                    .submit_typed(&binding, Typed(&shared, "manual"))?
                    .await?,
            );
            Ok::<_, Error>(())
        });
        let deadline = wasip2::clocks::monotonic_clock::now() + 5_000_000_000;
        loop {
            if let Poll::Ready(result) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
                result?;
                break;
            }
            assert!(
                wasip2::clocks::monotonic_clock::now() < deadline,
                "external poll stalled: {:?}, resolutions={}, wakes={}",
                remote.stats()?,
                shared.resolutions.get(),
                count.0.load(Ordering::Relaxed)
            );
            reactor.poll(Duration::ZERO)?;
            wasip2::clocks::monotonic_clock::subscribe_duration(1_000_000).block();
        }
        assert!(
            count.0.load(Ordering::Relaxed) >= 2,
            "external executor was not woken for completions"
        );
    } else if scenario == "drop" || scenario == "close" {
        let mut operation = reactor.run(async {
            let binding = remote.bind("echo")?.await?;
            let call = remote.submit_json(&binding, json!({"message":"held-owner"}))?;
            handshake("active").await;
            Ok(call)
        })?;
        let mut control = remote.schema_registry_stats()?;
        if scenario == "close" {
            reactor.close()?;
        } else {
            drop(reactor);
        }
        let error = operation.try_result().unwrap().unwrap_err();
        assert_eq!(error.code, "TransportClosed");
        assert_eq!(error.execution_certainty, ExecutionCertainty::Unknown);
        let error = control.try_result().unwrap().unwrap_err();
        assert_eq!(error.code, "TransportClosed");
        assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
        assert_eq!(
            shared.drops.get(),
            1,
            "retained weak handles kept provider alive"
        );
        assert_eq!(remote.bind("echo").err().unwrap().code, "TransportClosed");
        println!(
            "{}",
            json!({"result":scenario,"resolutions":shared.resolutions.get()})
        );
        return Ok(());
    } else {
        let binding = reactor.run(async { remote.bind("echo")?.await })?;
        if scenario == "workflow" {
            let marker = Rc::new(Cell::new(false));
            reactor.run(async {
                remote.set_validator_cache_capacity(2)?.await?;
                assert_eq!(remote.validator_cache_stats()?.await?.capacity, 2);
                assert_eq!(remote.schema_registry_stats()?.await?.resources, 0);
                let mut watcher = remote.subscribe_catalog()?;
                remote.refresh()?.await?;
                assert!(watcher.changed().await.unwrap().published);
                let binding = remote.bind("echo")?.await?;
                completed(
                    remote
                        .submit_typed(&binding, Typed(&shared, "typed"))?
                        .await?,
                );
                marker.set(true);
                completed(
                    remote
                        .submit_json(&binding, json!({"message":"json"}))?
                        .await?,
                );
                completed(remote.submit_raw(&binding, r#"{"message":"raw"}"#)?.await?);
                executed(remote.execute("echo", "--message string")?.await?);
                executed(
                    remote
                        .execute_argv("echo", &["--message".into(), "argv".into()])?
                        .await?,
                );
                remote.command_spec("echo")?.await?;
                remote.complete_line("echo --m", 8)?.await?;
                remote.clear_validator_cache()?.await?;
                assert_eq!(remote.validator_cache_stats()?.await?.entries, 0);
                Ok(())
            })?;
            assert!(marker.get());
            assert_eq!(shared.serializers.get(), 1);
        } else if scenario == "parallel" {
            reactor.run(async {
                let mut calls = Vec::new();
                let handles: Vec<_> = (0..16).map(|_| remote.clone()).collect();
                for (index, handle) in handles.iter().enumerate() {
                    calls.push(handle.submit_json(
                        &binding,
                        json!({"message":format!("held-parallel-{index}")}),
                    )?);
                }
                let seventeenth = remote.submit_json(&binding, json!({"message":"seventeenth"}))?;
                stage("barrier");
                for call in calls {
                    completed(call.await?);
                }
                completed(seventeenth.await?);
                Ok(())
            })?;
        } else if scenario == "cancel" || scenario == "timeout" {
            reactor.run(async {
                let peer = remote.submit_json(&binding, json!({"message":"held-peer"}))?;
                let interrupted =
                    remote.submit_json(&binding, json!({"message":"held-interrupted"}))?;
                if scenario == "timeout" {
                    remote.set_timeout(&interrupted, Duration::from_millis(250))?;
                }
                handshake("active").await;
                if scenario == "cancel" {
                    interrupted.cancel();
                }
                let error = interrupted.await.unwrap_err();
                assert_eq!(
                    error.code,
                    if scenario == "cancel" {
                        "Cancelled"
                    } else {
                        "Timeout"
                    }
                );
                assert_eq!(error.execution_certainty, ExecutionCertainty::Unknown);
                completed(
                    remote
                        .submit_json(&binding, json!({"message":"independent"}))?
                        .await?,
                );
                stage("release");
                completed(peer.await?);
                Ok(())
            })?;
        } else if scenario == "root-error" {
            let peer = remote.submit_json(&binding, json!({"message":"held-retained"}))?;
            let error = reactor
                .run::<()>(async {
                    let interrupted =
                        remote.submit_json(&binding, json!({"message":"held-root"}))?;
                    handshake("active").await;
                    assert!(!interrupted.is_finished());
                    *late.borrow_mut() = Some(poll_fn(|cx| Poll::Ready(cx.waker().clone())).await);
                    Err(Error::local("FixtureRootError", "root future failed"))
                })
                .unwrap_err();
            assert_eq!(error.code, "FixtureRootError");
            reactor.poll(Duration::ZERO)?;
            assert!(!peer.is_finished());
            stage("release");
            completed(reactor.run(peer)?);
        } else if scenario == "notifications" {
            reactor.run(async {
                let mut watcher = remote.subscribe_catalog()?;
                remote.start_notifications()?;
                poll_fn(|_| {
                    if remote
                        .notification_status()
                        .unwrap()
                        .is_some_and(|status| status.phase == NotificationPhase::Listening)
                    {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                })
                .await;
                stage("listening");
                assert!(watcher.changed().await.unwrap().invalidated);
                let binding = remote.bind("echo")?.await?;
                completed(
                    remote
                        .submit_json(&binding, json!({"message":"after-notification"}))?
                        .await?,
                );
                remote.stop_notifications()?;
                Ok(())
            })?;
            assert!(shared.events.get() > 0);
        } else if scenario == "resume" {
            reactor.run(async {
                remote.set_elicitation(true, false)?.await?;
                let CallOutcome::NeedsInput(handle) = remote
                    .submit_json(&binding, json!({"message":"interactive"}))?
                    .await?
                else {
                    panic!("expected interaction")
                };
                completed(
                    remote
                        .resume(
                            &handle,
                            json!({"confirm":{"action":"accept","content":{"yes":true}}}),
                        )?
                        .await?,
                );
                Ok(())
            })?;
        } else {
            panic!("unknown scenario")
        }
    }
    assert_eq!(remote.stats()?.input_bytes, 0);
    let events = remote.drain_operation_events()?;
    assert!(!events.events.is_empty() || scenario == "idle" || scenario == "direct-zero");
    reactor.close()?;
    assert_eq!(shared.drops.get(), 1);
    assert_eq!(remote.bind("echo").err().unwrap().code, "TransportClosed");
    drop(reactor);
    assert_eq!(remote.stats().unwrap_err().code, "TransportClosed");
    if let Some(waker) = late.borrow_mut().take() {
        waker.wake_by_ref();
    }
    assert_eq!(shared.drops.get(), 1);
    println!(
        "{}",
        json!({"result":scenario,"resolutions":shared.resolutions.get()})
    );
    Ok(())
}
#[cfg(not(all(target_os = "wasi", target_env = "p2")))]
fn main() {
    eprintln!("build this example for wasm32-wasip2");
}
