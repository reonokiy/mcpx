//! Fixture-driven WASI configuration barriers and refresh coalescing.
#[cfg(all(target_os = "wasi", target_env = "p2"))]
fn main() -> mcpx_core::Result<()> {
    use mcpx_core::{
        CallOutcome, Credential, CredentialFuture, CredentialProvider, CredentialRequest, Dialect,
        ExecutionCertainty, HostRuntime, OperationEventKind, SchemaRegistry,
    };
    use mcpx_wasi::{WasiDriver, WasiQueue, WasiQueueOptions};
    use serde_json::json;
    use std::{cell::Cell, io::Write, rc::Rc, time::Duration};
    struct Provider(Rc<Cell<usize>>, bool);
    impl CredentialProvider for Provider {
        fn resolve<'a>(&'a mut self, _: CredentialRequest<'a>) -> CredentialFuture<'a> {
            self.0.set(self.0.get() + 1);
            let changed = self.1 && self.0.get() >= 3;
            Box::pin(async move {
                Credential::bearer(
                    if changed {
                        "notify-second"
                    } else {
                        "notify-snapshot"
                    }
                    .into(),
                    if changed {
                        "controls-second"
                    } else {
                        "controls"
                    }
                    .into(),
                )
            })
        }
    }
    fn stage(name: &str) {
        println!("{}", json!({"stage": name}));
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
    fn registry() -> SchemaRegistry {
        SchemaRegistry::new([(
            "https://fixture.invalid/word".into(),
            json!({"type":"string"}),
        )])
        .unwrap()
    }
    fn completed(value: CallOutcome) {
        assert!(matches!(value, CallOutcome::Completed(_)));
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
    let mut queue = WasiQueue::new(
        WasiDriver::new(
            Credential::bearer("notify-snapshot".into(), "controls".into())?,
            Duration::from_secs(5),
        )?,
        HostRuntime::with_dialect(endpoint, dialect)?,
        WasiQueueOptions {
            capacity: 16,
            max_bytes: if scenario == "local" {
                1024
            } else {
                8 * 1024 * 1024
            },
            ..Default::default()
        },
    )?;
    let resolutions = Rc::new(Cell::new(0));
    queue.set_credential_provider(Provider(resolutions.clone(), scenario == "identity"), None)?;
    if scenario == "local" {
        let too_large = SchemaRegistry::new([(
            "https://fixture.invalid/large".into(),
            json!({"description":"x".repeat(1200)}),
        )])?;
        assert_eq!(
            queue.set_schema_registry(too_large).unwrap_err().code,
            "QueueFull"
        );
        assert_eq!(
            queue.set_validator_cache_capacity(4097).unwrap_err().code,
            "ConfigurationError"
        );
        let configure = queue.set_schema_registry(registry())?;
        assert_eq!(resolutions.get(), 0);
        queue.run(configure)?;
        let stats = queue.schema_registry_stats()?;
        assert_eq!(queue.run(stats)?.resources, 1);
        let capacity = queue.set_validator_cache_capacity(0)?;
        queue.run(capacity)?;
        let cache = queue.validator_cache_stats()?;
        assert_eq!(queue.run(cache)?.capacity, 0);
        let clear = queue.clear_validator_cache()?;
        queue.run(clear)?;
        let ttl = queue.set_catalog_ttl(0)?;
        queue.run(ttl)?;
        let elicitation = queue.set_elicitation(true, false)?;
        queue.run(elicitation)?;
        assert_eq!(resolutions.get(), 0);
        assert_eq!(queue.stats().input_bytes, 0);
        queue.close()?;
        assert_eq!(
            queue.schema_registry_stats().unwrap_err().code,
            "TransportClosed"
        );
        println!(
            "{}",
            json!({"result":scenario,"resolutions":resolutions.get()})
        );
        return Ok(());
    }
    if scenario == "registry" {
        let configure = queue.set_schema_registry(SchemaRegistry::new([(
            "https://fixture.invalid/word".into(),
            json!({"type":"string","minLength":2}),
        )])?)?;
        queue.run(configure)?;
        assert_eq!(resolutions.get(), 0);
    }
    let binding = queue.fresh_tool("echo")?;
    let baseline = resolutions.get();
    if ["barrier", "cancel", "timeout", "drop"].contains(&scenario.as_str()) {
        let mut old = queue.submit_json(&binding, json!({"message":"held-controls"}))?;
        let mut configure = queue.set_schema_registry(registry())?;
        let control_id = configure.id();
        let mut stats = queue.schema_registry_stats()?;
        let mut next = queue.bind("echo")?;
        if scenario == "timeout" {
            queue.set_timeout(&configure, Duration::from_millis(250))?;
        }
        handshake(&mut queue, "barrier")?;
        assert!(!configure.is_finished());
        assert!(!stats.is_finished());
        assert!(!next.is_finished());
        assert_eq!(
            resolutions.get(),
            baseline + 1,
            "local controls queried provider or later work crossed barrier"
        );
        if scenario == "drop" {
            drop(queue);
            let error = old.try_result().unwrap().unwrap_err();
            assert_eq!(error.code, "TransportClosed");
            assert_eq!(error.execution_certainty, ExecutionCertainty::Unknown);
            for error in [
                configure.try_result().unwrap().unwrap_err(),
                stats.try_result().unwrap().unwrap_err(),
                next.try_result().unwrap().err().unwrap(),
            ] {
                assert_eq!(error.code, "TransportClosed");
                assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
            }
            println!(
                "{}",
                json!({"result":scenario,"resolutions":resolutions.get()})
            );
            return Ok(());
        }
        if scenario == "cancel" {
            configure.cancel();
        }
        if scenario != "barrier" {
            let error = queue.wait(&mut configure).unwrap_err();
            assert_eq!(
                error.code,
                if scenario == "cancel" {
                    "Cancelled"
                } else {
                    "Timeout"
                }
            );
            assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
            assert_eq!(error.queue_operation_id, Some(control_id));
        }
        handshake(&mut queue, "release")?;
        completed(queue.wait(&mut old)?);
        if scenario == "barrier" {
            queue.wait(&mut configure)?;
        }
        assert_eq!(
            queue.wait(&mut stats)?.resources,
            usize::from(scenario == "barrier")
        );
        queue.wait(&mut next)?;
        if scenario == "barrier" {
            assert_eq!(
                queue
                    .submit_json(&binding, json!({"message":"stale"}))
                    .unwrap_err()
                    .code,
                "BindingStale"
            );
        } else {
            let good = queue.submit_json(&binding, json!({"message":"retained"}))?;
            completed(queue.run(good)?);
        }
        let before_controls = resolutions.get();
        let capacity = queue.set_validator_cache_capacity(1)?;
        queue.run(capacity)?;
        let cache = queue.validator_cache_stats()?;
        assert_eq!(queue.run(cache)?.capacity, 1);
        let clear = queue.clear_validator_cache()?;
        let cache = queue.validator_cache_stats()?;
        queue.run(clear)?;
        assert_eq!(queue.run(cache)?.entries, 0);
        assert_eq!(resolutions.get(), before_controls);
        let journal = queue.drain_operation_events();
        let entries: Vec<_> = journal
            .events
            .iter()
            .filter(|event| event.queue_operation_id == control_id)
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
            1
        );
        assert_eq!(
            entries
                .iter()
                .any(|e| e.kind == OperationEventKind::Started),
            scenario == "barrier"
        );
    } else if scenario == "registry" {
        assert_eq!(
            queue
                .submit_json(&binding, json!({"message":"x"}))
                .unwrap_err()
                .code,
            "ValidationFailed"
        );
        let call = queue.submit_json(&binding, json!({"message":"ok"}))?;
        completed(queue.run(call)?);
        let configure = queue.set_schema_registry(SchemaRegistry::new([(
            "https://fixture.invalid/word".into(),
            json!({"type":"string","minLength":4}),
        )])?)?;
        queue.run(configure)?;
        let next = queue.fresh_tool("echo")?;
        assert_eq!(
            queue
                .submit_json(&binding, json!({"message":"long"}))
                .unwrap_err()
                .code,
            "BindingStale"
        );
        assert_eq!(
            queue
                .submit_json(&next, json!({"message":"ok"}))
                .unwrap_err()
                .code,
            "ValidationFailed"
        );
        let call = queue.submit_json(&next, json!({"message":"long"}))?;
        completed(queue.run(call)?);
    } else if scenario == "coalesce" {
        let mut refreshes = Vec::new();
        for _ in 0..8 {
            refreshes.push(queue.refresh()?);
        }
        for refresh in &mut refreshes {
            queue.wait(refresh)?;
        }
        assert_eq!(
            resolutions.get(),
            baseline + 8,
            "coalescing skipped per-job credential checks"
        );
        let fresh = queue.refresh()?;
        queue.run(fresh)?;
    } else if scenario.starts_with("refresh-") {
        let mut leader = queue.refresh()?;
        let mut follower = queue.refresh()?;
        let mut last = queue.refresh()?;
        if scenario == "refresh-timeout" {
            queue.set_timeout(&follower, Duration::from_millis(250))?;
        }
        if scenario != "refresh-failure" {
            handshake(&mut queue, "metadata")?;
        }
        if scenario == "refresh-cancel" {
            leader.cancel();
            let error = queue.wait(&mut leader).unwrap_err();
            assert_eq!(error.code, "Cancelled");
            assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
        } else if scenario == "refresh-timeout" {
            let error = queue.wait(&mut follower).unwrap_err();
            assert_eq!(error.code, "Timeout");
            assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
            handshake(&mut queue, "release")?;
            queue.wait(&mut leader)?;
        } else {
            assert!(queue.wait(&mut leader).is_err());
        }
        if scenario != "refresh-timeout" {
            queue.wait(&mut follower)?;
        }
        queue.wait(&mut last)?;
    } else if scenario == "identity" {
        let first = queue.refresh()?;
        let second = queue.refresh()?;
        let third = queue.refresh()?;
        queue.run(first)?;
        queue.run(second)?;
        queue.run(third)?;
        assert_eq!(resolutions.get(), 4);
    } else if scenario == "context" {
        let first = queue.refresh()?;
        let registry = queue.set_schema_registry(registry())?;
        let second = queue.refresh()?;
        let third = queue.refresh()?;
        queue.run(first)?;
        queue.run(registry)?;
        queue.run(second)?;
        queue.run(third)?;
        let ttl = queue.set_catalog_ttl(0)?;
        let fourth = queue.refresh()?;
        let fifth = queue.refresh()?;
        queue.run(ttl)?;
        queue.run(fourth)?;
        queue.run(fifth)?;
    } else if scenario == "release" {
        let configure = queue.set_elicitation(true, false)?;
        queue.run(configure)?;
        let first = queue.submit_json(&binding, json!({"message":"interactive"}))?;
        let CallOutcome::NeedsInput(handle) = queue.run(first)? else {
            panic!("expected interaction")
        };
        let release = queue.release_continuation(&handle)?;
        let mut resume = queue.resume(
            &handle,
            json!({"confirm":{"action":"accept","content":{"yes":true}}}),
        )?;
        queue.run(release)?;
        let error = queue.wait(&mut resume).unwrap_err();
        assert_eq!(error.code, "InvalidContinuation");
        assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
    } else {
        panic!("unknown scenario");
    }
    assert_eq!(queue.stats().input_bytes, 0);
    queue.close()?;
    println!(
        "{}",
        json!({"result":scenario,"resolutions":resolutions.get()})
    );
    Ok(())
}
#[cfg(not(all(target_os = "wasi", target_env = "p2")))]
fn main() {
    eprintln!("build this example for wasm32-wasip2");
}
