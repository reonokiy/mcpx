//! Native, single-thread allocator-request measurement; the library installs no allocator.
#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static PROFILE_ALLOC: &stats_alloc::StatsAlloc<std::alloc::System> =
    &stats_alloc::INSTRUMENTED_SYSTEM;

#[cfg(not(target_family = "wasm"))]
fn main() -> mcpx_core::Result<()> {
    use mcpx_core::{Catalog, SchemaRegistry, ToolDescriptor};
    use serde_json::{Value, json};
    use std::time::Instant;

    fn tool(schema: Value) -> ToolDescriptor {
        serde_json::from_value(json!({"name":"profile","inputSchema":schema})).unwrap()
    }
    fn current(stats: stats_alloc::Stats) -> i128 {
        stats.bytes_allocated as i128 - stats.bytes_deallocated as i128
    }
    fn nanos(start: Instant) -> u64 {
        u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
    let rustc = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned());
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                let (key, value) = line.split_once(':')?;
                (key.trim() == "model name").then(|| value.trim().to_owned())
            })
        });
    // Warm library meta-schema statics and SHA CPU selection outside measured regions.
    let mut warm = Catalog::default();
    warm.replace(vec![tool(json!({"type":"object"}))])?;
    drop(warm.bind("profile")?);
    warm.clear_validator_cache();
    drop(warm);

    let properties: serde_json::Map<_, _> = (0..256)
        .map(|i| {
            (
                format!("p{i}"),
                json!({"type":"integer","minimum":0,"maximum":1000000}),
            )
        })
        .collect();
    let many = json!({"type":"object","properties":properties});
    let many_input = Value::Object((0..256).map(|i| (format!("p{i}"), json!(1))).collect());
    let regexes = json!({"type":"object","properties":(0..32).map(|i| (format!("p{i}"),json!({"type":"string","pattern":"^[a-z][a-z0-9_]{0,63}$"}))).collect::<serde_json::Map<_,_>>()});
    let regex_input = Value::Object(
        (0..32)
            .map(|i| (format!("p{i}"), json!("hello_123")))
            .collect(),
    );
    let external = SchemaRegistry::new([("https://schemas.example/shared".into(), many.clone())])?;
    let cases = vec![
        (
            "scalar",
            json!({"type":"object","properties":{"q":{"type":"string","minLength":1}},"required":["q"]}),
            json!({"q":"hello"}),
            SchemaRegistry::default(),
        ),
        (
            "properties_256",
            many.clone(),
            many_input.clone(),
            SchemaRegistry::default(),
        ),
        (
            "enums_1000",
            json!({"type":"object","properties":{"q":{"type":"string","enum":(0..1000).map(|i| format!("v-{i:04}")).collect::<Vec<_>>()}},"required":["q"]}),
            json!({"q":"v-0007"}),
            SchemaRegistry::default(),
        ),
        (
            "conditions_refs",
            json!({"type":"object","$defs":{"num":{"type":"integer","minimum":0,"maximum":10000}},"properties":{"q":{"$ref":"#/$defs/num"},"enabled":{"type":"boolean"}},"allOf":[{"if":{"properties":{"enabled":{"const":true}},"required":["enabled"]},"then":{"required":["q"]}},{"anyOf":[{"properties":{"q":{"multipleOf":2}}},{"properties":{"q":{"multipleOf":3}}}]}]}),
            json!({"q":6,"enabled":true}),
            SchemaRegistry::default(),
        ),
        (
            "regexes_32",
            regexes,
            regex_input,
            SchemaRegistry::default(),
        ),
        (
            "external_properties_256",
            json!({"$ref":"https://schemas.example/shared"}),
            many_input,
            external,
        ),
    ];
    let mut rows = Vec::new();
    for (name, schema, instance, registry) in cases {
        let root_bytes = serde_json::to_vec(&schema).unwrap().len();
        let registry_bytes = registry.stats().bytes;
        let mut catalog = Catalog::default();
        catalog.set_schema_registry(registry);
        catalog.replace(vec![tool(schema)])?;
        let region = stats_alloc::Region::new(PROFILE_ALLOC);
        let start = Instant::now();
        let binding = catalog.bind("profile")?;
        let cold_ns = nanos(start);
        let cold_traffic = region.change();
        drop(binding);
        let start = Instant::now();
        let binding = catalog.bind("profile")?;
        let warm_ns = nanos(start);
        let start = Instant::now();
        for _ in 0..100 {
            std::hint::black_box(binding.validate_json(instance.clone())?);
        }
        let validation_100_ns = nanos(start);
        let stats = catalog.validator_cache_stats();
        drop(binding); // No application pins remain when retained cache memory is measured.
        let before = PROFILE_ALLOC.stats();
        catalog.clear_validator_cache();
        let after = PROFILE_ALLOC.stats();
        let released = current(before) - current(after);
        assert!(
            released > 0 && catalog.validator_cache_stats().entries == 0,
            "cache must release owned allocations"
        );
        rows.push(json!({"name":name,"root_schema_bytes":root_bytes,"registry_source_charge_bytes":registry_bytes,
            "cold_bind_ns":cold_ns,"warm_rebind_ns":warm_ns,"validation_with_clone_and_size_check_100_ns":validation_100_ns,
            "cold_requested_allocation_bytes":cold_traffic.bytes_allocated,
            "cache_release_requested_bytes":released,
            "cache_release_to_source_ratio":released as f64/(root_bytes+registry_bytes) as f64,
            "cache_stats_before_clear":stats}));
    }
    let mut diagnostic_rows = Vec::new();
    for width in [64, 512, 4096] {
        let schema = json!({"type":"object","required":(0..width).map(|i| format!("p{i:04}")).collect::<Vec<_>>()});
        let binding = mcpx_core::ToolBinding::new(tool(schema.clone()))?;
        let raw = jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .build(&schema)
            .unwrap();
        let instance = json!({});
        let region = stats_alloc::Region::new(PROFILE_ALLOC);
        let start = Instant::now();
        let error = binding.validate_json(instance.clone()).err().unwrap();
        let first_ns = nanos(start);
        let first_bytes = region.change().bytes_allocated;
        assert_eq!(error.issues.len(), 1);
        drop(error);
        let region = stats_alloc::Region::new(PROFILE_ALLOC);
        let start = Instant::now();
        // Previous diagnostic strategy, with the same limit and path copies.
        let issues: Vec<_> = raw
            .iter_errors(&instance)
            .take(32)
            .map(|error| mcpx_core::ValidationIssue {
                instance_path: error.instance_path().to_string(),
                schema_path: error.schema_path().to_string(),
            })
            .collect();
        let error = mcpx_core::Error {
            issues,
            ..mcpx_core::Error::local(
                "ValidationFailed",
                "arguments do not satisfy the remote schema",
            )
        };
        let eager_ns = nanos(start);
        let eager_bytes = region.change().bytes_allocated;
        assert_eq!(error.issues.len(), 32);
        std::hint::black_box(error);
        diagnostic_rows.push(json!({"missing_fields":width,"mcpx_first_error_ns":first_ns,"mcpx_first_error_requested_bytes":first_bytes,"eager_prefix_32_ns":eager_ns,"eager_prefix_32_requested_bytes":eager_bytes}));
    }
    let report = json!({"format_version":2,"schema_policy_version":mcpx_core::SCHEMA_POLICY_VERSION,"environment":{"os":std::env::consts::OS,"arch":std::env::consts::ARCH,"pointer_bits":usize::BITS,"rustc":rustc,"cpu":cpu,"profile":if cfg!(debug_assertions) {"debug"} else {"release"},"allocator":"stats_alloc 0.1.10 / System","threads":1},
        "method":{"bytes":"requested Rust allocator bytes released by clear after dropping all binding pins; includes cache entry metadata, excludes allocator overhead/RSS and shared statics/registry resources",
            "realloc":"stats_alloc bytes_allocated/bytes_deallocated already include realloc growth/shrink; bytes_reallocated is not added again",
            "timings":"one cold bind and one warm rebind per shape after warming meta-schema statics; observational samples, not percentile or regression thresholds",
            "limits":"native measurement only; no claim of a hard compiled-memory bound or Wasm heap equivalence",
            "diagnostics":"one missing-required validation per width; compile outside regions; compare mcpx first-error path including input checks against previous raw-validator eager iter_errors/take(32) path; requested allocation traffic, not peak live heap"},"cases":rows,"diagnostic_cases":diagnostic_rows});
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    Ok(())
}

#[cfg(target_family = "wasm")]
fn main() {
    eprintln!("schema_profile requires the native instrumented System allocator");
}
