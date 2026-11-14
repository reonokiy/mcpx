//! Real WASIp2 reconnect lifecycle fixture. The Node harness controls the peer.
#[cfg(all(target_os = "wasi", target_env = "p2"))]
mod guest {
    use mcpx_core::{
        CallOutcome, Client, Credential, CredentialFuture, CredentialProvider, CredentialRequest,
        Dialect, Error, ExecutionCertainty, HostRuntime, NotificationPhase, OperationEventKind,
        Result, SchemaRegistry,
    };
    use mcpx_wasi::{
        Execution, WasiClientExt, WasiDriver, WasiQueue, WasiQueueOptions, run_blocking,
    };
    use serde_json::{Value, json};
    use std::{cell::Cell, future::poll_fn, io::Write, rc::Rc, task::Poll, time::Duration};
    use wasip2::clocks::monotonic_clock as clock;

    #[derive(Default)]
    struct Shared {
        calls: Cell<usize>,
        active: Cell<usize>,
        drops: Cell<usize>,
        second: Cell<bool>,
        identity: Cell<bool>,
        fail: Cell<bool>,
        delay: Cell<u64>,
    }
    struct Provider(Rc<Shared>);
    struct Guard(Rc<Shared>);
    impl Drop for Provider {
        fn drop(&mut self) {
            self.0.drops.set(self.0.drops.get() + 1);
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.active.set(self.0.active.get() - 1);
        }
    }
    impl CredentialProvider for Provider {
        fn resolve<'a>(&'a mut self, _: CredentialRequest<'a>) -> CredentialFuture<'a> {
            self.0.calls.set(self.0.calls.get() + 1);
            Box::pin(async move {
                self.0.active.set(self.0.active.get() + 1);
                let _guard = Guard(self.0.clone());
                let deadline = clock::now() + self.0.delay.get();
                poll_fn(|_| {
                    if clock::now() >= deadline {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                })
                .await;
                if self.0.fail.get() {
                    return Err(Error::local("FixtureSecret", "reconnect-first"));
                }
                Credential::bearer(
                    if self.0.second.get() {
                        "reconnect-second"
                    } else {
                        "reconnect-first"
                    }
                    .into(),
                    if self.0.identity.get() {
                        "second"
                    } else {
                        "first"
                    }
                    .into(),
                )
            })
        }
    }
    fn stage(name: &str) {
        println!("{}", json!({"stage":name}));
        std::io::stdout().flush().unwrap();
    }
    fn input(mut drive: impl FnMut() -> Result<()>) -> Result<()> {
        let stdin = wasip2::cli::stdin::get_stdin();
        let mut bytes = Vec::new();
        let deadline = clock::now() + 5_000_000_000;
        while !bytes.ends_with(b"\n") {
            drive()?;
            bytes.extend(stdin.read(16).unwrap());
            assert!(clock::now() < deadline, "host handshake stalled");
            clock::subscribe_duration(1_000_000).block();
        }
        assert_eq!(bytes, b"go\n");
        Ok(())
    }
    fn handshake(queue: &mut WasiQueue, name: &str) -> Result<()> {
        stage(name);
        input(|| queue.poll(Duration::ZERO))
    }
    fn completed(outcome: CallOutcome) -> Value {
        let CallOutcome::Completed(value) = outcome else {
            panic!("expected completed")
        };
        value.raw["structuredContent"]["message"].clone()
    }
    fn executed(value: Execution) -> Value {
        let Execution::Outcome(value) = value else {
            panic!("expected outcome")
        };
        completed(value)
    }
    fn error(error: Error, code: &str) {
        assert_eq!(error.code, code, "{error:?}");
        assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
    }
    fn listening(queue: &mut WasiQueue) -> Result<()> {
        let deadline = clock::now() + 3_000_000_000;
        while queue
            .notification_status()
            .is_none_or(|s| s.phase != NotificationPhase::Listening)
        {
            queue.poll(Duration::from_millis(5))?;
            assert!(clock::now() < deadline, "listener did not restart");
        }
        Ok(())
    }
    fn registry() -> SchemaRegistry {
        SchemaRegistry::new([(
            "https://fixture.invalid/reconnect-word".into(),
            json!({"type":"string"}),
        )])
        .unwrap()
    }
    fn direct(endpoint: String, dialect: Dialect, mode: &str) -> Result<()> {
        let mut client = Client::new(
            WasiDriver::new(
                Credential::bearer("reconnect-first".into(), "first".into())?,
                Duration::from_millis(if mode == "direct-deadline" { 350 } else { 3000 }),
            )?
            .with_http_notifications(true),
            HostRuntime::with_dialect(endpoint, dialect)?,
        );
        let old = run_blocking(client.fresh_tool("echo"))?;
        assert_eq!(
            completed(run_blocking(
                client.call_json(&old, json!({"message":"before"}))
            )?),
            "before"
        );
        stage("configure");
        input(|| Ok(()))?;
        let start = clock::now();
        let result = run_blocking(client.reconnect());
        if mode == "direct-basic" {
            result?;
        } else {
            let failure = result.unwrap_err();
            if mode == "direct-deadline" {
                error(failure, "Timeout");
                assert!(
                    clock::now() - start < 480_000_000,
                    "reconnect renewed its deadline"
                );
            } else {
                assert_eq!(failure.execution_certainty, ExecutionCertainty::NotSent);
            }
            assert!(client.is_suspended());
            error(
                run_blocking(client.fresh_tool("echo")).err().unwrap(),
                "TransportClosed",
            );
            error(client.start_notifications().unwrap_err(), "TransportClosed");
            client.stop_notifications()?;
            stage("retry");
            input(|| Ok(()))?;
            run_blocking(client.reconnect())?;
        }
        assert!(!client.is_suspended());
        error(
            run_blocking(client.call_json(&old, json!({"message":"never-old"}))).unwrap_err(),
            "BindingStale",
        );
        let next = run_blocking(client.fresh_tool("echo"))?;
        assert_eq!(
            completed(run_blocking(
                client.call_json(&next, json!({"message":"after"}))
            )?),
            "after"
        );
        run_blocking(client.close())?;
        error(
            run_blocking(client.reconnect()).unwrap_err(),
            "TransportClosed",
        );
        Ok(())
    }
    pub fn run() -> Result<()> {
        let mut args = std::env::args().skip(1);
        let endpoint = args.next().unwrap();
        let protocol = args.next().unwrap();
        let mode = args.next().unwrap();
        let dialect = if protocol.starts_with("auto") {
            Dialect::Auto
        } else if protocol == "legacy" {
            Dialect::Legacy
        } else {
            Dialect::Modern
        };
        if mode.starts_with("direct-") {
            direct(endpoint, dialect, &mode)?;
            println!("{}", json!({"result":mode}));
            return Ok(());
        }
        let shared = Rc::new(Shared::default());
        let mut queue = WasiQueue::new(
            WasiDriver::new(Credential::anonymous(), Duration::from_secs(5))?
                .with_http_notifications(true),
            HostRuntime::with_dialect(endpoint, dialect)?,
            WasiQueueOptions::default(),
        )?;
        queue.set_credential_provider(Provider(shared.clone()), None)?;
        if mode == "cold" {
            let reconnect = queue.reconnect()?;
            queue.run(reconnect)?;
            assert_eq!(shared.calls.get(), 1);
            assert!(!queue.is_suspended());
            let call = queue.execute("echo", "--message after")?;
            assert_eq!(executed(queue.run(call)?), "after");
            queue.close()?;
            println!("{}", json!({"result":mode}));
            return Ok(());
        }
        if mode == "zero-ttl" {
            let set = queue.set_catalog_ttl(0)?;
            queue.run(set)?;
        }
        queue.fresh_tool("echo")?;
        let set = queue.set_schema_registry(registry())?;
        queue.run(set)?;
        let old = queue.fresh_tool("echo")?;
        listening(&mut queue)?;
        assert_eq!(shared.calls.get(), 2);
        if mode == "reactor" || mode == "reactor-drop" {
            let mut owner = queue.into_reactor();
            let remote = owner.remote();
            if mode == "reactor-drop" {
                shared.delay.set(10_000_000_000);
                let mut pending = remote.reconnect()?;
                let deadline = clock::now() + 3_000_000_000;
                while shared.active.get() == 0 {
                    owner.poll(Duration::ZERO)?;
                    assert!(clock::now() < deadline);
                }
                assert!(remote.is_suspended()?);
                drop(owner);
                error(
                    pending.try_result().unwrap().unwrap_err(),
                    "TransportClosed",
                );
                error(remote.reconnect().unwrap_err(), "TransportClosed");
                assert_eq!(shared.active.get(), 0);
                assert_eq!(shared.drops.get(), 1);
                println!("{}", json!({"result":mode}));
                return Ok(());
            }
            owner.run(async {
                remote.reconnect()?.await?;
                assert!(!remote.is_suspended()?);
                error(
                    remote
                        .submit_json(&old, json!({"message":"never-old"}))
                        .unwrap_err(),
                    "BindingStale",
                );
                let binding = remote.bind("echo")?.await?;
                assert_eq!(
                    completed(
                        remote
                            .submit_json(&binding, json!({"message":"after"}))?
                            .await?
                    ),
                    "after"
                );
                Ok(())
            })?;
            owner.close()?;
            drop(owner);
            error(remote.reconnect().unwrap_err(), "TransportClosed");
            assert_eq!(shared.drops.get(), 1);
            println!("{}", json!({"result":mode}));
            return Ok(());
        }
        let mut continuation = None;
        if mode == "resume" || mode == "retained-resume" {
            let set = queue.set_elicitation(true, false)?;
            queue.run(set)?;
            let call = queue.submit_json(&old, json!({"message":"interactive"}))?;
            if mode == "resume" {
                let CallOutcome::NeedsInput(handle) = queue.run(call)? else {
                    panic!("expected input")
                };
                continuation = Some(handle);
            } else {
                // Keep a positive result in the operation future across reset.
                let reconnect = queue.reconnect()?;
                queue.run(reconnect)?;
                let CallOutcome::NeedsInput(handle) = queue.run(call)? else {
                    panic!("expected retained input")
                };
                let mut resumed = queue.resume(
                    &handle,
                    json!({"confirm":{"action":"accept","content":{"yes":true}}}),
                )?;
                error(queue.wait(&mut resumed).unwrap_err(), "InvalidContinuation");
                queue.close()?;
                println!("{}", json!({"result":mode}));
                return Ok(());
            }
        }
        if mode == "retained" {
            let call = queue.submit_json(&old, json!({"message":"retained"}))?;
            let reconnect = queue.reconnect()?;
            queue.run(reconnect)?;
            assert_eq!(completed(queue.run(call)?), "retained");
            queue.close()?;
            println!("{}", json!({"result":mode}));
            return Ok(());
        }
        if mode == "stopped" {
            queue.stop_notifications()?;
        }
        handshake(&mut queue, "configure")?;
        if mode == "chain" {
            let mut first = queue.reconnect()?;
            let next = queue.reconnect()?;
            let call = queue.execute("echo", "--message after")?;
            error(queue.wait(&mut first).unwrap_err(), "HttpError");
            queue.run(next)?;
            assert!(!queue.is_suspended());
            assert_eq!(executed(queue.run(call)?), "after");
            queue.close()?;
            println!("{}", json!({"result":mode}));
            return Ok(());
        }
        let old_resource = queue.notification_status().map(|s| s.resource_id);
        let mut listener_resume = if mode == "listener-resume" {
            let deadline = clock::now() + 3_000_000_000;
            while queue
                .notification_status()
                .is_none_or(|s| s.phase != NotificationPhase::Failed)
            {
                queue.poll(Duration::from_millis(2))?;
                assert!(clock::now() < deadline);
            }
            Some(queue.resume_notifications(old_resource.unwrap())?)
        } else {
            None
        };
        shared
            .second
            .set(matches!(mode.as_str(), "rotate" | "identity"));
        shared.identity.set(mode == "identity");
        let resolutions = shared.calls.get();
        let mut peers = Vec::new();
        if matches!(
            mode.as_str(),
            "barrier" | "pending-cancel" | "pending-timeout"
        ) {
            for n in 0..if mode == "barrier" { 16 } else { 1 } {
                peers.push(queue.submit_json(&old, json!({"message":format!("peer-{n}")}))?);
            }
            handshake(&mut queue, "peers")?;
        }
        if mode.starts_with("provider-") {
            shared.delay.set(10_000_000_000);
        }
        if mode == "provider-error" {
            shared.delay.set(0);
            shared.fail.set(true);
        }
        let mut recovery = queue.reconnect()?;
        let recovery_id = recovery.id();
        let mut typed = if mode == "barrier" {
            Some(queue.submit_json(&old, json!({"message":"never-queued-old"}))?)
        } else {
            None
        };
        let mut named = if mode == "barrier" {
            Some(queue.execute("echo", "--message 42")?)
        } else {
            None
        };
        if mode.starts_with("pending-") {
            if mode == "pending-cancel" {
                recovery.cancel();
            } else {
                queue.set_timeout(&recovery, Duration::from_millis(100))?;
            }
            error(
                queue.wait(&mut recovery).unwrap_err(),
                if mode == "pending-cancel" {
                    "Cancelled"
                } else {
                    "Timeout"
                },
            );
            assert!(!queue.is_suspended());
            handshake(&mut queue, "release-peers")?;
            for peer in peers {
                completed(queue.run(peer)?);
            }
            let call = queue.submit_json(&old, json!({"message":"after"}))?;
            completed(queue.run(call)?);
        } else {
            if mode == "barrier" {
                handshake(&mut queue, "barrier")?;
                for peer in peers {
                    completed(queue.run(peer)?);
                }
            }
            let phase = if mode.starts_with("delete-") && mode != "delete-failure" {
                Some("delete")
            } else if mode.starts_with("provider-") && mode != "provider-error" {
                Some("provider")
            } else if mode.starts_with("metadata-") && mode != "metadata-failure" {
                Some("metadata")
            } else {
                None
            };
            if let Some(phase) = phase {
                let deadline = clock::now() + 3_000_000_000;
                while match phase {
                    "delete" => !queue.stats().authenticating,
                    "provider" => shared.active.get() == 0,
                    _ => !queue.stats().metadata,
                } {
                    queue.poll(Duration::ZERO)?;
                    assert!(clock::now() < deadline, "missing {phase} phase");
                }
                handshake(&mut queue, phase)?;
                assert!(queue.is_suspended());
                assert_eq!(
                    shared.calls.get(),
                    resolutions + usize::from(phase != "delete"),
                    "credential resolution crossed cleanup barrier"
                );
                error(queue.start_notifications().unwrap_err(), "TransportClosed");
                error(
                    queue.resume_notifications(1).unwrap_err(),
                    "TransportClosed",
                );
                let before = clock::now();
                for _ in 0..50 {
                    queue.poll(Duration::ZERO)?;
                }
                assert!(clock::now() - before < 100_000_000, "zero poll blocked");
                if mode.ends_with("cancel") {
                    recovery.cancel();
                }
                if mode.ends_with("timeout") {
                    queue.set_timeout(&recovery, Duration::from_millis(150))?;
                }
                if mode.ends_with("close") || mode.ends_with("drop") {
                    if mode.ends_with("close") {
                        queue.close()?;
                    } else {
                        drop(queue);
                    }
                    error(
                        recovery.try_result().unwrap().unwrap_err(),
                        "TransportClosed",
                    );
                    assert_eq!(shared.active.get(), 0);
                    assert_eq!(shared.drops.get(), 1);
                    println!(
                        "{}",
                        json!({"result":mode,"providerCalls":shared.calls.get()})
                    );
                    return Ok(());
                }
            }
            if mode == "stop-during" {
                while !queue.is_suspended() {
                    queue.poll(Duration::ZERO)?;
                }
                queue.stop_notifications()?;
            }
            if mode == "root-deadline" {
                queue.set_timeout(&recovery, Duration::from_millis(350))?;
            }
            let start = clock::now();
            let result = queue.wait(&mut recovery);
            if mode == "root-deadline" {
                assert!(
                    clock::now() - start < 480_000_000,
                    "queue renewed reconnect deadline"
                );
            }
            let failed = mode.starts_with("delete-")
                || mode.starts_with("provider-")
                || mode.starts_with("metadata-")
                || mode == "root-deadline";
            if failed {
                let failure = result.unwrap_err();
                let code = if mode.ends_with("cancel") {
                    "Cancelled"
                } else if mode.ends_with("timeout") || mode == "root-deadline" {
                    "Timeout"
                } else if mode == "provider-error" {
                    "CredentialUnavailable"
                } else if mode.starts_with("delete-") {
                    "SessionCleanupFailed"
                } else {
                    "HttpError"
                };
                error(failure, code);
                assert!(queue.is_suspended());
                error(
                    queue
                        .execute("echo", "--message never-suspended")
                        .unwrap_err(),
                    "TransportClosed",
                );
                let stats = queue.schema_registry_stats()?;
                queue.run(stats)?;
                queue.stop_notifications()?;
                shared.delay.set(0);
                shared.fail.set(false);
                handshake(&mut queue, "retry")?;
                let retry = queue.reconnect()?;
                queue.run(retry)?;
            } else {
                result?;
            }
            assert!(!queue.is_suspended());
            if let Some(resume) = &mut listener_resume {
                error(queue.wait(resume).unwrap_err(), "Cancelled");
                assert_ne!(
                    queue.notification_status().map(|s| s.resource_id),
                    old_resource
                );
            }
            if mode == "barrier" {
                error(
                    queue.wait(typed.as_mut().unwrap()).unwrap_err(),
                    "BindingStale",
                );
                assert_eq!(executed(queue.wait(named.as_mut().unwrap())?), 42);
            } else {
                error(
                    queue
                        .submit_json(&old, json!({"message":"never-old"}))
                        .unwrap_err(),
                    "BindingStale",
                );
            }
            if let Some(handle) = continuation {
                let mut resumed = queue.resume(
                    &handle,
                    json!({"confirm":{"action":"accept","content":{"yes":true}}}),
                )?;
                error(queue.wait(&mut resumed).unwrap_err(), "InvalidContinuation");
            }
            if mode == "twice" {
                let again = queue.reconnect()?;
                queue.run(again)?;
            }
            let stats = queue.schema_registry_stats()?;
            assert_eq!(queue.run(stats)?.resources, usize::from(mode != "identity"));
            if !failed && !matches!(mode.as_str(), "barrier" | "resume" | "twice") {
                assert_eq!(
                    shared.calls.get(),
                    resolutions + 1,
                    "reconnect resolves credentials once"
                );
            }
            if mode == "schema" {
                let current = queue.fresh_tool("echo")?;
                let call = queue.submit_json(&current, json!({"message":"after"}))?;
                completed(queue.run(call)?);
            } else {
                let call = queue.execute(
                    "echo",
                    if mode == "barrier" {
                        "--message 43"
                    } else {
                        "--message after"
                    },
                )?;
                executed(queue.run(call)?);
            }
            if !failed && !matches!(mode.as_str(), "stopped" | "stop-during" | "zero-ttl") {
                listening(&mut queue)?;
            }
        }
        assert_eq!(queue.stats().input_bytes, 0);
        let events = queue.drain_operation_events();
        let entries: Vec<_> = events
            .events
            .iter()
            .filter(|e| e.queue_operation_id == recovery_id)
            .collect();
        assert_eq!(
            entries
                .iter()
                .filter(|e| matches!(
                    e.kind,
                    OperationEventKind::Completed
                        | OperationEventKind::Failed
                        | OperationEventKind::Cancelled
                ))
                .count(),
            1,
            "reconnect must settle exactly once"
        );
        assert_eq!(
            entries
                .iter()
                .filter(|e| e.kind == OperationEventKind::Started)
                .count(),
            usize::from(!mode.starts_with("pending-"))
        );
        queue.close()?;
        error(queue.reconnect().unwrap_err(), "TransportClosed");
        assert_eq!(shared.active.get(), 0);
        assert_eq!(shared.drops.get(), 1);
        println!(
            "{}",
            json!({"result":mode,"providerCalls":shared.calls.get()})
        );
        Ok(())
    }
}
#[cfg(all(target_os = "wasi", target_env = "p2"))]
fn main() -> mcpx_core::Result<()> {
    guest::run()
}
#[cfg(not(all(target_os = "wasi", target_env = "p2")))]
fn main() {
    eprintln!("build this example for wasm32-wasip2");
}
