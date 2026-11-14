//! Local WASI provider/runner lifecycle fixture; the companion smoke coordinates
//! peer responses and session cleanup so pending futures cannot pass accidentally.
#[cfg(all(target_os = "wasi", target_env = "p2"))]
fn main() -> mcpx_core::Result<()> {
    use mcpx_core::{
        CallOutcome, Credential, CredentialFuture, CredentialProvider, CredentialRequest, Dialect,
        Error, ExecutionCertainty, HostRuntime, NotificationPhase,
    };
    use mcpx_wasi::{WasiDriver, WasiQueue, WasiQueueOptions};
    use serde_json::json;
    use std::{
        cell::{Cell, RefCell},
        future::Future,
        io::Write,
        pin::Pin,
        rc::Rc,
        task::{Context, Poll},
        time::Duration,
    };
    use wasip2::clocks::monotonic_clock;
    fn stage(name: &str) {
        println!("{}", json!({"stage":name}));
        std::io::stdout().flush().unwrap();
    }
    fn handshake(queue: &mut WasiQueue, name: &str) -> mcpx_core::Result<()> {
        stage(name);
        let input = wasip2::cli::stdin::get_stdin();
        let mut bytes = Vec::new();
        while !bytes.ends_with(b"\n") {
            queue.poll(Duration::from_millis(5))?;
            bytes.extend(input.read(16).unwrap());
        }
        assert_eq!(bytes, b"go\n");
        Ok(())
    }
    fn text(value: CallOutcome) -> String {
        let CallOutcome::Completed(value) = value else {
            panic!("expected completed");
        };
        value.raw["structuredContent"]["message"]
            .as_str()
            .unwrap()
            .into()
    }
    fn listening(queue: &mut WasiQueue) {
        let deadline = monotonic_clock::now() + 3_000_000_000;
        while queue
            .notification_status()
            .is_none_or(|s| s.phase != NotificationPhase::Listening)
        {
            queue.poll(Duration::from_millis(10)).unwrap();
            assert!(monotonic_clock::now() < deadline);
        }
    }
    struct Shared {
        calls: Cell<usize>,
        active: Cell<usize>,
        dropped: Cell<usize>,
        providers_dropped: Cell<usize>,
        delay: Cell<u64>,
        second: Cell<bool>,
        same: Cell<bool>,
        fail: Cell<bool>,
        oversize: Cell<bool>,
        watch: Cell<bool>,
        wake: RefCell<Option<std::task::Waker>>,
        endpoint: String,
    }
    struct Provider(Rc<Shared>);
    struct Guard(Rc<Shared>);
    impl Drop for Provider {
        fn drop(&mut self) {
            self.0
                .providers_dropped
                .set(self.0.providers_dropped.get() + 1);
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.active.set(self.0.active.get() - 1);
            self.0.dropped.set(self.0.dropped.get() + 1);
        }
    }
    struct Delay<'a>(u64, &'a RefCell<Option<std::task::Waker>>);
    impl Future for Delay<'_> {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if monotonic_clock::now() >= self.0 {
                Poll::Ready(())
            } else {
                *self.1.borrow_mut() = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
    impl CredentialProvider for Provider {
        fn resolve<'a>(&'a mut self, request: CredentialRequest<'a>) -> CredentialFuture<'a> {
            assert_eq!(request.endpoint, self.0.endpoint);
            assert_eq!(request.reference, Some("fixture:credential"));
            self.0.calls.set(self.0.calls.get() + 1);
            Box::pin(async move {
                self.0.active.set(self.0.active.get() + 1);
                assert_eq!(self.0.active.get(), 1);
                let _guard = Guard(self.0.clone());
                Delay(monotonic_clock::now() + self.0.delay.get(), &self.0.wake).await;
                if self.0.fail.get() {
                    return Err(Error::new("secret-provider-code", "notify-snapshot"));
                }
                Credential::bearer(
                    if self.0.oversize.get() {
                        "x".repeat(65537)
                    } else if self.0.second.get() {
                        "notify-second".into()
                    } else {
                        "notify-snapshot".into()
                    },
                    if self.0.second.get() && !self.0.same.get() {
                        "second".into()
                    } else {
                        "first".into()
                    },
                )
            })
        }
    }
    struct YieldOnce(bool);
    struct Probe;
    impl CredentialProvider for Probe {
        fn resolve<'a>(&'a mut self, _: CredentialRequest<'a>) -> CredentialFuture<'a> {
            panic!("unexpected replacement provider")
        }
    }
    impl Future for YieldOnce {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
    let mut args = std::env::args().skip(1);
    let endpoint = args.next().unwrap();
    let protocol = args.next().unwrap();
    let scenario = args.next().unwrap();
    let runtime = HostRuntime::with_dialect(
        endpoint.clone(),
        if protocol.starts_with("auto") {
            Dialect::Auto
        } else if protocol == "legacy" {
            Dialect::Legacy
        } else {
            Dialect::Modern
        },
    )?;
    let shared = Rc::new(Shared {
        calls: Cell::new(0),
        active: Cell::new(0),
        dropped: Cell::new(0),
        providers_dropped: Cell::new(0),
        delay: Cell::new(0),
        second: Cell::new(false),
        same: Cell::new(false),
        fail: Cell::new(false),
        oversize: Cell::new(false),
        watch: Cell::new(false),
        wake: RefCell::new(None),
        endpoint,
    });
    let observed = shared.clone();
    let driver = WasiDriver::new(Credential::anonymous(), Duration::from_secs(5))?
        .with_http_notifications(true)
        .with_event_handler(move |batch| {
            if observed.watch.get()
                && batch
                    .events
                    .iter()
                    .any(|event| event.kind == "catalog_stale")
            {
                observed.watch.set(false);
                stage("old-stream");
            }
        });
    let mut queue = WasiQueue::new(
        driver,
        runtime,
        WasiQueueOptions {
            capacity: if matches!(scenario.as_str(), "provider-cancel" | "provider-timeout") {
                2
            } else {
                64
            },
            ..Default::default()
        },
    )?;
    queue.set_credential_provider(Provider(shared.clone()), Some("fixture:credential".into()))?;
    let local = Rc::new(Cell::new(0));
    let captured = local.clone();
    queue.run(async move {
        YieldOnce(false).await;
        captured.set(7);
        Ok(())
    })?;
    assert_eq!(local.get(), 7);
    assert_eq!(shared.calls.get(), 0);
    let binding = queue.fresh_tool("echo")?;
    listening(&mut queue);
    assert_eq!(shared.calls.get(), 1);
    assert_eq!(
        queue
            .set_credential_provider(Probe, Some(String::new()))
            .unwrap_err()
            .code,
        "ConfigurationError"
    );
    if scenario == "provider-drop" {
        shared.delay.set(10_000_000_000);
        let mut operation = queue.submit_json(&binding, json!({"message":"never-drop"}))?;
        while shared.active.get() == 0 {
            queue.poll(Duration::ZERO)?;
        }
        drop(queue);
        assert_eq!(
            operation.try_result().unwrap().unwrap_err().code,
            "TransportClosed"
        );
        assert_eq!(shared.active.get(), 0);
        assert_eq!(shared.providers_dropped.get(), 1);
        if let Some(waker) = shared.wake.borrow_mut().take() {
            waker.wake();
        }
        println!(
            "{}",
            json!({"result":scenario,"providerCalls":shared.calls.get()})
        );
        return Ok(());
    }
    let result = (|| -> mcpx_core::Result<()> {
        match scenario.as_str() {
            "rotation" => {
                shared.second.set(true);
                shared.same.set(true);
                let operation = queue.submit_json(&binding, json!({"message":"same-context"}))?;
                assert_eq!(text(queue.run(operation)?), "same-context");
                assert_eq!(shared.calls.get(), 2);
                handshake(&mut queue, "frozen-stream")?;
                assert_eq!(shared.calls.get(), 2);
                queue.stop_notifications()?;
                queue.start_notifications()?;
                listening(&mut queue);
                assert_eq!(shared.calls.get(), 2);
            }
            "identity" => {
                let first = queue.submit_json(&binding, json!({"message":"held-first"}))?;
                let second = queue.submit_json(&binding, json!({"message":"held-second"}))?;
                handshake(&mut queue, "peers")?;
                shared.second.set(true);
                let next = queue.bind("echo")?;
                while shared.calls.get() < 4 {
                    queue.poll(Duration::ZERO)?;
                }
                assert_eq!(queue.stats().active, 2);
                assert!(!queue.stats().authenticating);
                shared.watch.set(true);
                stage("identity-waiting");
                let (a, b, next) =
                    queue.run(async move { Ok((first.await?, second.await?, next.await?)) })?;
                assert_eq!(text(a), "held-first");
                assert_eq!(text(b), "held-second");
                assert_eq!(shared.calls.get(), 4);
                assert_eq!(
                    queue
                        .submit_json(&binding, json!({"message":"never-old"}))
                        .unwrap_err()
                        .code,
                    "BindingStale"
                );
                let after = queue.submit_json(&next, json!({"message":"after-identity"}))?;
                assert_eq!(text(queue.run(after)?), "after-identity");
                listening(&mut queue);
                assert_eq!(shared.calls.get(), 5);
            }
            "binding" => {
                shared.second.set(true);
                let mut old = queue.submit_json(&binding, json!({"message":"never-binding"}))?;
                let error = queue.wait(&mut old).unwrap_err();
                assert_eq!(error.code, "BindingStale");
                assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
                queue.fresh_tool("echo")?;
                listening(&mut queue);
                assert_eq!(shared.calls.get(), 3);
            }
            "resume" => {
                let configure = queue.set_elicitation(true, false)?;
                queue.run(configure)?;
                let initial = queue.submit_json(&binding, json!({"message":"interactive"}))?;
                let CallOutcome::NeedsInput(handle) = queue.run(initial)? else {
                    panic!("expected input");
                };
                shared.second.set(true);
                let mut resumed = queue.resume(
                    &handle,
                    json!({"confirm":{"action":"accept","content":{"yes":true}}}),
                )?;
                assert_eq!(
                    queue.wait(&mut resumed).unwrap_err().code,
                    "InvalidContinuation"
                );
                assert_eq!(shared.calls.get(), 3);
            }
            "provider-error" | "provider-oversize" => {
                shared.fail.set(scenario == "provider-error");
                shared.oversize.set(scenario == "provider-oversize");
                let mut operation =
                    queue.submit_json(&binding, json!({"message":"never-failed-provider"}))?;
                let error = queue.wait(&mut operation).unwrap_err();
                assert_eq!(error.code, "CredentialUnavailable");
                assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
                assert!(!format!("{error:?}").contains("notify-snapshot"));
                shared.fail.set(false);
                shared.oversize.set(false);
                let after =
                    queue.submit_json(&binding, json!({"message":"after-provider-failure"}))?;
                assert_eq!(text(queue.run(after)?), "after-provider-failure");
                assert_eq!(shared.calls.get(), 3);
            }
            "provider-cancel" | "provider-timeout" => {
                let peer = queue.submit_json(&binding, json!({"message":"held-peer"}))?;
                handshake(&mut queue, "peer-active")?;
                shared.delay.set(10_000_000_000);
                let mut pending =
                    queue.submit_json(&binding, json!({"message":"never-pending"}))?;
                while shared.active.get() == 0 {
                    queue.poll(Duration::ZERO)?;
                }
                assert_eq!(
                    queue
                        .submit_json(&binding, json!({"message":"never-full"}))
                        .unwrap_err()
                        .code,
                    "QueueFull"
                );
                assert_eq!(
                    queue.set_credential_provider(Probe, None).unwrap_err().code,
                    "Busy"
                );
                stage("provider-pending");
                assert_eq!(text(queue.run(peer)?), "held-peer");
                assert_eq!(shared.active.get(), 1);
                assert_eq!(shared.calls.get(), 3);
                if scenario == "provider-cancel" {
                    pending.cancel();
                } else {
                    queue.set_timeout(&pending, Duration::from_millis(200))?;
                }
                let error = queue.wait(&mut pending).unwrap_err();
                assert_eq!(
                    error.code,
                    if scenario == "provider-cancel" {
                        "Cancelled"
                    } else {
                        "Timeout"
                    }
                );
                assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
                assert_eq!(shared.active.get(), 0);
                if let Some(waker) = shared.wake.borrow_mut().take() {
                    waker.wake();
                }
                shared.delay.set(0);
                let after =
                    queue.submit_json(&binding, json!({"message":"after-provider-wait"}))?;
                assert_eq!(text(queue.run(after)?), "after-provider-wait");
                assert_eq!(shared.calls.get(), 4);
            }
            "provider-close" | "provider-unpolled" => {
                shared.delay.set(10_000_000_000);
                let mut pending = queue.submit_json(&binding, json!({"message":"never-close"}))?;
                if scenario == "provider-unpolled" {
                    queue.poll(Duration::ZERO)?;
                    assert!(queue.stats().resolving);
                    assert_eq!(shared.calls.get(), 1);
                } else {
                    while shared.active.get() == 0 {
                        queue.poll(Duration::ZERO)?;
                    }
                }
                queue.close()?;
                assert_eq!(
                    queue.wait(&mut pending).unwrap_err().code,
                    "TransportClosed"
                );
                assert_eq!(
                    shared.calls.get(),
                    if scenario == "provider-unpolled" {
                        1
                    } else {
                        2
                    }
                );
                assert_eq!(shared.providers_dropped.get(), 1);
                assert_eq!(shared.active.get(), 0);
            }
            "auth-cancel" | "auth-timeout" | "auth-failure" => {
                shared.second.set(true);
                let mut next = queue.bind("echo")?;
                if scenario != "auth-failure" {
                    while !queue.stats().authenticating {
                        queue.poll(Duration::ZERO)?;
                    }
                    handshake(&mut queue, "auth-pending")?;
                    if scenario == "auth-cancel" {
                        next.cancel();
                    } else {
                        queue.set_timeout(&next, Duration::from_millis(300))?;
                    }
                }
                let error = queue.wait(&mut next).err().unwrap();
                assert_eq!(
                    error.code,
                    match scenario.as_str() {
                        "auth-cancel" => "Cancelled",
                        "auth-timeout" => "Timeout",
                        _ => "SessionCleanupFailed",
                    }
                );
                assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
                handshake(&mut queue, "auth-failed")?;
                let next = queue.fresh_tool("echo")?;
                let after = queue.submit_json(&next, json!({"message":"after-auth-retry"}))?;
                assert_eq!(text(queue.run(after)?), "after-auth-retry");
                assert_eq!(shared.calls.get(), 4);
                listening(&mut queue);
            }
            _ => panic!("unknown scenario"),
        }
        Ok(())
    })();
    let closed = queue.close();
    result?;
    closed?;
    assert_eq!(shared.active.get(), 0);
    assert_eq!(shared.providers_dropped.get(), 1);
    assert_eq!(queue.stats().input_bytes, 0);
    println!(
        "{}",
        json!({"result":scenario,"providerCalls":shared.calls.get(),"providerFuturesDropped":shared.dropped.get()})
    );
    Ok(())
}
#[cfg(not(all(target_os = "wasi", target_env = "p2")))]
fn main() {
    eprintln!("Build this example with --target wasm32-wasip2");
}
