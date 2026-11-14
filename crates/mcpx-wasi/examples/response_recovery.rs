//! Real WASIp2 high-level response recovery scenarios; driven by the smoke script.
#[cfg(all(target_os = "wasi", target_env = "p2"))]
fn main() -> mcpx_core::Result<()> {
    use mcpx_core::{
        CallOutcome, Client, Credential, CredentialFuture, CredentialProvider, CredentialRequest,
        Dialect, Error, ExecutionCertainty, HostRuntime, Result,
    };
    use mcpx_wasi::{
        Execution, ResponseRecoveryPolicy, WasiDriver, WasiQueue, WasiQueueOptions, run_blocking,
    };
    use serde_json::json;
    use std::{
        cell::Cell,
        future::poll_fn,
        io::Write,
        rc::Rc,
        task::Poll,
        time::{Duration, Instant},
    };
    #[derive(Default)]
    struct Shared {
        sdk: Cell<bool>,
        second: Cell<bool>,
        blocked: Cell<bool>,
        calls: Cell<usize>,
        drops: Cell<usize>,
    }
    struct Provider(Rc<Shared>);
    impl Drop for Provider {
        fn drop(&mut self) {
            self.0.drops.set(self.0.drops.get() + 1);
        }
    }
    impl CredentialProvider for Provider {
        fn resolve<'a>(&'a mut self, _: CredentialRequest<'a>) -> CredentialFuture<'a> {
            self.0.calls.set(self.0.calls.get() + 1);
            Box::pin(poll_fn(move |_| {
                if self.0.blocked.get() {
                    Poll::Pending
                } else {
                    Poll::Ready(Credential::bearer(
                        if self.0.sdk.get() {
                            "response-frozen"
                        } else if self.0.second.get() {
                            "response-second"
                        } else {
                            "response-first"
                        }
                        .into(),
                        "same".into(),
                    ))
                }
            }))
        }
    }
    fn stage(name: &str) {
        println!("{}", json!({"stage":name}));
        std::io::stdout().flush().unwrap();
    }
    fn handoff(queue: &mut WasiQueue, name: &str) -> Result<()> {
        stage(name);
        let input = wasip2::cli::stdin::get_stdin();
        let mut bytes = Vec::new();
        let until = Instant::now() + Duration::from_secs(8);
        while !bytes.ends_with(b"\n") {
            queue.poll(Duration::from_millis(5))?;
            bytes.extend(input.read(16).unwrap());
            assert!(Instant::now() < until, "handoff {name}");
        }
        assert_eq!(bytes, b"go\n");
        Ok(())
    }
    fn check(outcome: CallOutcome, message: &str) {
        let CallOutcome::Completed(response) = outcome else {
            panic!("expected result")
        };
        assert_eq!(response.raw["structuredContent"]["message"], message);
        assert!(response.operation_id.is_some());
    }
    fn error(error: Error, code: &str) {
        assert_eq!(error.code, code);
        assert_eq!(error.execution_certainty, ExecutionCertainty::Unknown);
        assert!(error.operation_id.is_some());
    }
    let args: Vec<_> = std::env::args().collect();
    let endpoint = &args[1];
    let protocol = &args[2];
    let mode = &args[3];
    let sdk = mode.starts_with("sdk-");
    let legacy = protocol.ends_with("legacy");
    let dialect = if protocol.starts_with("auto-") {
        Dialect::Auto
    } else if legacy {
        Dialect::Legacy
    } else {
        Dialect::Modern
    };
    let policy = ResponseRecoveryPolicy {
        max_attempts: 2,
        backoff: Duration::from_millis(10),
        timeout: Duration::from_millis(
            if matches!(
                mode.as_str(),
                "window" | "reply-window" | "opening" | "reading"
            ) {
                100
            } else {
                2000
            },
        ),
    };
    let driver = WasiDriver::new(
        Credential::bearer(
            if sdk {
                "response-frozen"
            } else {
                "response-first"
            }
            .into(),
            "same".into(),
        )?,
        Duration::from_millis(
            if matches!(mode.as_str(), "client-deadline" | "client-known") {
                180
            } else {
                5000
            },
        ),
    )?;
    if mode == "invalid" {
        assert_eq!(
            driver
                .with_response_recovery(Some(ResponseRecoveryPolicy {
                    max_attempts: 0,
                    ..policy
                }))
                .err()
                .unwrap()
                .code,
            "ConfigurationError"
        );
        println!("PASS WASI response recovery invalid");
        return Ok(());
    }
    let driver = driver
        .with_response_recovery((mode != "disabled").then_some(policy))?
        .with_http_notifications(mode == "notifications");
    let runtime = HostRuntime::with_dialect(endpoint.clone(), dialect)?;
    let expected = match mode.as_str() {
        "flap" => Some("ResponseRecoveryExhausted"),
        "window" | "reply-window" | "opening" | "reading" | "client-deadline" => Some("Timeout"),
        "401" => Some("AuthFailed"),
        "404" => Some("SessionExpired"),
        "405" | "503" | "writer-failure" => Some("HttpError"),
        "disabled" | "no-cursor" | "reset" | "bad" => Some("ProtocolError"),
        _ if !legacy => Some("ProtocolError"),
        _ => None,
    };
    if mode.starts_with("client") || mode == "sdk-client" {
        let mut client = Client::new(driver, runtime);
        let binding = run_blocking(client.fresh_tool("echo"));
        if sdk && dialect == Dialect::Auto {
            assert_eq!(binding.err().unwrap().code, "UnsupportedProtocolVersion");
            run_blocking(client.close())?;
            println!("PASS WASI response recovery {protocol} {mode}");
            return Ok(());
        }
        let binding = binding?;
        let message = if sdk { "sdk-response-resume" } else { "client" };
        let result = run_blocking(client.call_json(&binding, json!({"message":message})));
        if let Some(code) = expected {
            error(result.unwrap_err(), code);
        } else {
            check(result?, message);
        }
        run_blocking(client.close())?;
        println!("PASS WASI response recovery {protocol} {mode}");
        return Ok(());
    }
    let mut queue = WasiQueue::new(
        driver,
        runtime,
        WasiQueueOptions {
            capacity: if mode == "sixteen" { 16 } else { 64 },
            timeout: Duration::from_secs(5),
            ..Default::default()
        },
    )?;
    let shared = Rc::new(Shared::default());
    shared.sdk.set(sdk);
    queue.set_credential_provider(Provider(shared.clone()), None)?;
    let mut bind = queue.bind("echo")?;
    let binding = queue.wait(&mut bind);
    if sdk && dialect == Dialect::Auto {
        assert_eq!(binding.err().unwrap().code, "UnsupportedProtocolVersion");
        queue.close()?;
        println!("PASS WASI response recovery {protocol} {mode}");
        return Ok(());
    }
    let binding = binding?;
    if mode == "sixteen" {
        #[derive(serde::Serialize)]
        struct Typed {
            message: String,
        }
        let mut typed = Vec::new();
        let mut command = Vec::new();
        for n in 0..16 {
            let message = format!("mixed-{n}");
            if n % 3 == 0 {
                typed.push((n, queue.submit_typed(&binding, Typed { message })?));
            } else if n % 3 == 1 {
                command.push((n, queue.execute("echo", &format!("--message '{message}'"))?));
            } else {
                command.push((
                    n,
                    queue.execute_argv("echo", &["--message".into(), message])?,
                ));
            }
        }
        handoff(&mut queue, "full")?;
        assert_eq!(
            queue
                .submit_json(&binding, json!({"message":"overflow"}))
                .unwrap_err()
                .code,
            "QueueFull"
        );
        stage("overflow-checked");
        handoff(&mut queue, "release-full")?;
        for (n, mut call) in typed {
            let id = call.id();
            let value = queue.wait(&mut call)?;
            let CallOutcome::Completed(ref r) = value else {
                panic!()
            };
            assert_eq!(r.queue_operation_id, Some(id));
            check(value, &format!("mixed-{n}"));
        }
        for (n, mut call) in command {
            let Execution::Outcome(value) = queue.wait(&mut call)? else {
                panic!()
            };
            check(value, &format!("mixed-{n}"));
        }
        assert_eq!(queue.stats().active, 0);
        assert_eq!(queue.stats().input_bytes, 0);
    } else if mode == "provider" {
        let mut root = queue.submit_json(&binding, json!({"message":"frozen"}))?;
        handoff(&mut queue, "root-held")?;
        shared.second.set(true);
        shared.blocked.set(true);
        let resolved = shared.calls.get();
        let mut peer = queue.submit_json(&binding, json!({"message":"peer-provider"}))?;
        let until = Instant::now() + Duration::from_secs(5);
        while !queue.stats().resolving || shared.calls.get() == resolved {
            queue.poll(Duration::from_millis(5))?;
            assert!(Instant::now() < until);
        }
        let before = shared.calls.get();
        handoff(&mut queue, "provider-pending")?;
        check(queue.wait(&mut root)?, "frozen");
        assert_eq!(shared.calls.get(), before);
        peer.cancel();
        assert_eq!(queue.wait(&mut peer).unwrap_err().code, "Cancelled");
        shared.blocked.set(false);
        let mut next = queue.submit_json(&binding, json!({"message":"new-token"}))?;
        handoff(&mut queue, "second-held")?;
        check(queue.wait(&mut next)?, "new-token");
    } else if mode == "reactor" {
        let mut reactor = queue.into_reactor();
        let remote = reactor.remote();
        let result = reactor.run(async {
            let b = remote.bind("echo")?.await?;
            let value = remote
                .submit_json(&b, json!({"message":"reactor"}))?
                .await?;
            check(value, "reactor");
            let Execution::Outcome(value) = remote.execute("echo", "--message dependent")?.await?
            else {
                panic!()
            };
            check(value, "dependent");
            Ok(())
        });
        result?;
        reactor.close()?;
        assert_eq!(shared.drops.get(), 1);
        println!("PASS WASI response recovery {protocol} {mode}");
        return Ok(());
    } else if matches!(
        mode.as_str(),
        "cancel"
            | "cancel-backoff"
            | "deadline"
            | "close"
            | "drop"
            | "known-cancel"
            | "known-close"
            | "known-drop"
    ) {
        let mut call = queue.submit_json(&binding, json!({"message":"controlled"}))?;
        if mode == "deadline" {
            queue.set_timeout(&call, Duration::from_millis(500))?;
        }
        handoff(&mut queue, "controlled")?;
        match mode.as_str() {
            "cancel" | "cancel-backoff" => {
                call.cancel();
                error(queue.wait(&mut call).unwrap_err(), "Cancelled");
            }
            "deadline" => error(queue.wait(&mut call).unwrap_err(), "Timeout"),
            "close" => {
                queue.close()?;
                error(call.try_result().unwrap().unwrap_err(), "TransportClosed");
            }
            "drop" => {
                drop(queue);
                error(call.try_result().unwrap().unwrap_err(), "TransportClosed");
                assert_eq!(shared.drops.get(), 1);
                println!("PASS WASI response recovery {protocol} {mode}");
                return Ok(());
            }
            "known-cancel" => {
                call.cancel();
                queue.set_timeout(&call, Duration::from_millis(1))?;
                check(queue.wait(&mut call)?, "controlled");
            }
            "known-close" => {
                queue.close()?;
                check(call.try_result().unwrap()?, "controlled");
            }
            "known-drop" => {
                drop(queue);
                check(call.try_result().unwrap()?, "controlled");
                assert_eq!(shared.drops.get(), 1);
                println!("PASS WASI response recovery {protocol} {mode}");
                return Ok(());
            }
            _ => unreachable!(),
        }
    } else {
        let before = shared.calls.get();
        let message = if sdk { "sdk-response-resume" } else { "queue" };
        let mut call = queue.submit_json(&binding, json!({"message":message}))?;
        let result = queue.wait(&mut call);
        if mode == "raw" {
            let CallOutcome::Completed(value) = result? else {
                panic!()
            };
            assert_eq!(
                value.raw["structuredContent"]["value"].as_u64(),
                Some(u64::MAX)
            );
        } else if let Some(code) = expected {
            error(result.unwrap_err(), code);
        } else {
            check(result?, message);
        }
        assert_eq!(shared.calls.get(), before + 1);
        assert_eq!(queue.stats().active, 0);
        assert_eq!(queue.stats().input_bytes, 0);
    }
    queue.close()?;
    assert_eq!(shared.drops.get(), 1);
    println!("PASS WASI response recovery {protocol} {mode}");
    Ok(())
}
#[cfg(not(all(target_os = "wasi", target_env = "p2")))]
fn main() {
    eprintln!("build for wasm32-wasip2");
}
