use mcpx_core::{Catalog, SchemaRegistry, ToolBinding, ToolDescriptor};
use serde_json::{Value, json};

fn tool(name: &str, schema: Value) -> ToolDescriptor {
    serde_json::from_value(json!({"name":name,"inputSchema":schema})).unwrap()
}
fn constant(name: &str, value: u32) -> ToolDescriptor {
    tool(
        name,
        json!({"type":"object","properties":{"q":{"const":value}},"required":["q"]}),
    )
}

#[test]
fn cache_is_lru_per_validator_and_a_rebind_uses_the_original_validation_rules() {
    let mut catalog = Catalog::default();
    catalog.set_validator_cache_capacity(2).unwrap();
    catalog
        .replace(vec![constant("a", 1), constant("b", 2), constant("c", 3)])
        .unwrap();
    for (name, n) in [("a", 1), ("b", 2), ("a", 1), ("c", 3), ("a", 1), ("b", 2)] {
        let binding = catalog.bind(name).unwrap();
        assert!(binding.validate_json(json!({"q":n})).is_ok());
        assert!(binding.validate_json(json!({"q":n+10})).is_err());
    }
    let stats = catalog.validator_cache_stats();
    assert_eq!(
        (
            stats.entries,
            stats.capacity,
            stats.compilations,
            stats.hits,
            stats.evictions
        ),
        (2, 2, 4, 2, 2)
    );
    catalog.set_validator_cache_capacity(1).unwrap();
    drop(catalog.bind("b").unwrap());
    drop(catalog.bind("a").unwrap());
    let shrunk = catalog.validator_cache_stats();
    assert_eq!(
        (
            shrunk.entries,
            shrunk.hits,
            shrunk.compilations,
            shrunk.evictions
        ),
        (1, 3, 5, 4)
    );
}

#[test]
fn equal_schemas_share_rules_but_arguments_still_belong_to_their_binding_and_owner() {
    let schema = json!({"type":"object"});
    let mut catalog = Catalog::default();
    catalog
        .replace(vec![tool("a", schema.clone()), tool("b", schema)])
        .unwrap();
    let first = catalog.bind("a").unwrap();
    let other = catalog.bind("b").unwrap();
    assert_eq!(catalog.validator_cache_stats().compilations, 1);
    assert_eq!(catalog.validator_cache_stats().hits, 1);
    let args = first.args().finish().unwrap();
    assert_eq!(other.check_args(&args).unwrap_err().code, "BindingMismatch");
    let detached = first.compiled_schema().bind();
    assert_eq!(
        detached.check_args(&args).unwrap_err().code,
        "BindingMismatch"
    );
    assert_eq!(catalog.check(&detached).unwrap_err().code, "BindingStale");
    let mut separate = Catalog::default();
    separate.replace(vec![first.descriptor().clone()]).unwrap();
    let separate_binding = separate.bind("a").unwrap();
    assert_eq!(separate.validator_cache_stats().compilations, 1);
    assert_eq!(separate.validator_cache_stats().hits, 0);
    assert_eq!(
        separate_binding.check_args(&args).unwrap_err().code,
        "BindingMismatch"
    );
    assert_eq!(separate.check(&first).unwrap_err().code, "BindingStale");
}

#[test]
fn live_bindings_and_args_survive_eviction_clearing_and_disabling_retention() {
    let mut catalog = Catalog::default();
    catalog.set_validator_cache_capacity(1).unwrap();
    catalog
        .replace(vec![constant("a", 1), constant("b", 2)])
        .unwrap();
    let first = catalog.bind("a").unwrap();
    let args = first.validate_json(json!({"q":1})).unwrap();
    drop(catalog.bind("b").unwrap());
    catalog.clear_validator_cache();
    catalog.set_validator_cache_capacity(0).unwrap();
    assert_eq!(catalog.validator_cache_stats().entries, 0);
    catalog.check(&first).unwrap();
    first.check_args(&args).unwrap();
    assert!(first.validate_json(json!({"q":1})).is_ok());
    let rebound = catalog.bind("a").unwrap();
    rebound.check_args(&args).unwrap();
    drop(first);
    drop(rebound);
    drop(args);
    let before = catalog.validator_cache_stats().compilations;
    drop(catalog.bind("a").unwrap());
    drop(catalog.bind("a").unwrap());
    assert_eq!(catalog.validator_cache_stats().compilations, before + 2);
    assert_eq!(catalog.validator_cache_stats().entries, 0);
}

#[test]
fn input_and_output_are_independent_cache_entries_but_identical_rules_compile_once() {
    let mut descriptor = tool("both", json!({"type":"object","required":["q"]}));
    descriptor.output_schema = Some(descriptor.input_schema.clone());
    let mut catalog = Catalog::default();
    catalog.replace(vec![descriptor]).unwrap();
    let binding = catalog.bind("both").unwrap();
    assert_eq!(
        (
            catalog.validator_cache_stats().entries,
            catalog.validator_cache_stats().compilations
        ),
        (1, 1)
    );
    assert_eq!(binding.validate_output(Some(&json!({}))), Some(false));
    assert_eq!(binding.validate_output(Some(&json!({"q":1}))), Some(true));
}

#[test]
fn resource_replacement_identity_reset_and_changed_descriptors_never_reuse_stale_rules() {
    let make = |value| {
        SchemaRegistry::new([(
            "https://schemas.example/args".into(),
            json!({"type":"object","properties":{"q":{"const":value}},"required":["q"]}),
        )])
        .unwrap()
    };
    let descriptor = tool("a", json!({"$ref":"https://schemas.example/args"}));
    let mut catalog = Catalog::default();
    catalog.set_schema_registry(make(1));
    catalog.replace(vec![descriptor.clone()]).unwrap();
    let first = catalog.bind("a").unwrap();
    catalog.set_schema_registry(make(2));
    assert_eq!(catalog.validator_cache_stats().entries, 0);
    assert_eq!(catalog.check(&first).unwrap_err().code, "BindingStale");
    let second = catalog.bind("a").unwrap();
    assert!(second.validate_json(json!({"q":2})).is_ok());
    assert!(second.validate_json(json!({"q":1})).is_err());
    catalog.invalidate();
    assert_eq!(catalog.validator_cache_stats().entries, 0);
    catalog.replace(vec![descriptor]).unwrap();
    assert_eq!(
        catalog.bind("a").err().unwrap().code,
        "SchemaReferenceUnavailable"
    );
    catalog.replace(vec![constant("a", 3)]).unwrap();
    let third = catalog.bind("a").unwrap();
    assert!(third.validate_json(json!({"q":3})).is_ok());
}

#[test]
fn failed_compilation_is_not_cached_and_invalid_capacity_preserves_the_current_cache() {
    let mut catalog = Catalog::default();
    catalog
        .replace(vec![
            constant("a", 1),
            tool("bad", json!({"type":"unknown-type"})),
        ])
        .unwrap();
    drop(catalog.bind("a").unwrap());
    for _ in 0..2 {
        assert_eq!(catalog.bind("bad").err().unwrap().code, "SchemaUnsupported");
    }
    assert_eq!(
        (
            catalog.validator_cache_stats().entries,
            catalog.validator_cache_stats().compilations
        ),
        (1, 1)
    );
    assert_eq!(
        catalog.set_validator_cache_capacity(4097).unwrap_err().code,
        "ConfigurationError"
    );
    assert_eq!(
        (
            catalog.validator_cache_stats().entries,
            catalog.validator_cache_stats().capacity
        ),
        (1, 128)
    );
    let standalone = ToolBinding::new(constant("unrelated", 7)).unwrap();
    assert!(standalone.validate_json(json!({"q":7})).is_ok());
}
