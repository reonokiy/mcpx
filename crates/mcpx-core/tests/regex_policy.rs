use mcpx_core::{
    Catalog, MAX_SCHEMA_PATTERN_BYTES, MAX_SCHEMA_PATTERN_TOTAL_BYTES, MAX_SCHEMA_PATTERNS,
    SchemaRegistry, ToolBinding, ToolDescriptor,
};
use serde_json::{Value, json};

fn tool(schema: Value) -> ToolDescriptor {
    serde_json::from_value(json!({"name":"regex","inputSchema":schema})).unwrap()
}
fn code<T>(result: mcpx_core::Result<T>) -> String {
    result.err().expect("expected policy rejection").code
}
fn definitions(count: usize, pattern: &str) -> Value {
    json!({"$defs":(0..count).map(|i| (format!("p{i}"), json!({"pattern":pattern}))).collect::<serde_json::Map<_,_>>()})
}

#[test]
fn pattern_source_limits_count_utf8_bytes_and_cover_pattern_property_names() {
    let exact = "a".repeat(MAX_SCHEMA_PATTERN_BYTES);
    ToolBinding::new(tool(json!({"properties":{"q":{"pattern":exact}}}))).unwrap();
    for pattern in [
        format!("{exact}a"),
        "界".repeat(MAX_SCHEMA_PATTERN_BYTES / 3 + 1),
    ] {
        for schema in [
            json!({"pattern":pattern}),
            json!({"patternProperties":{pattern:true}}),
        ] {
            assert_eq!(code(ToolBinding::new(tool(schema))), "LimitExceeded");
        }
    }
}

#[test]
fn count_and_combined_source_budgets_apply_to_unused_registered_definitions() {
    let uri = "https://schemas.example/patterns";
    let registry =
        SchemaRegistry::new([(uri.into(), definitions(MAX_SCHEMA_PATTERNS, ""))]).unwrap();
    assert_eq!(registry.stats().patterns, MAX_SCHEMA_PATTERNS);
    assert_eq!(registry.stats().pattern_bytes, 0);
    ToolBinding::with_schema_registry(tool(json!({"$ref":uri})), &registry).unwrap();
    assert_eq!(
        code(ToolBinding::with_schema_registry(
            tool(json!({"$ref":uri,"pattern":""})),
            &registry
        )),
        "LimitExceeded"
    );
    assert_eq!(
        code(SchemaRegistry::new([(
            uri.into(),
            definitions(MAX_SCHEMA_PATTERNS + 1, "")
        )])),
        "LimitExceeded"
    );
    let pattern = "x".repeat(MAX_SCHEMA_PATTERN_BYTES);
    let count = MAX_SCHEMA_PATTERN_TOTAL_BYTES / MAX_SCHEMA_PATTERN_BYTES;
    let registry = SchemaRegistry::new([(uri.into(), definitions(count, &pattern))]).unwrap();
    assert_eq!(
        registry.stats().pattern_bytes,
        MAX_SCHEMA_PATTERN_TOTAL_BYTES
    );
    assert_eq!(
        code(ToolBinding::with_schema_registry(
            tool(json!({"pattern":"x"})),
            &registry
        )),
        "LimitExceeded"
    );
    assert_eq!(
        code(SchemaRegistry::new([(
            uri.into(),
            definitions(count + 1, &pattern)
        )])),
        "LimitExceeded"
    );
}

#[test]
fn pattern_like_annotation_data_is_audited_only_when_referenced_as_schema() {
    let data = json!({"pattern":"x".repeat(MAX_SCHEMA_PATTERN_BYTES + 1)});
    ToolBinding::new(tool(json!({"examples":[data],"const":data}))).unwrap();
    assert_eq!(
        code(ToolBinding::new(tool(
            json!({"examples":[data],"$ref":"#/examples/0"})
        ))),
        "LimitExceeded"
    );
}

#[test]
fn short_patterns_that_expand_past_engine_limits_and_invalid_ecma_are_rejected() {
    for pattern in ["^(?:a|b){200000}$", "(", "(?R)"] {
        assert_eq!(
            code(ToolBinding::new(tool(
                json!({"properties":{"q":{"pattern":pattern}}})
            ))),
            "SchemaUnsupported"
        );
        assert_eq!(
            code(SchemaRegistry::new([(
                "https://schemas.example/pattern".into(),
                json!({"pattern":pattern})
            )])),
            "SchemaUnsupported"
        );
    }
    for pattern in [r"^(?=a)a+$", r"^(a+)\1$", r"^\S+$", r"^[a-z]{1,64}$"] {
        ToolBinding::new(tool(json!({"properties":{"q":{"pattern":pattern}}}))).unwrap();
    }
}

#[test]
fn regex_resource_failures_remain_indeterminate_under_negation_and_conditionals() {
    let costly = json!({"pattern":r"^(a|a)*\1b"});
    let value = json!({"q":format!("{}!", "a".repeat(40))});
    for (index, schema) in [
        costly.clone(),
        json!({"not":costly}),
        json!({"if":costly,"then":false,"else":true}),
    ]
    .into_iter()
    .enumerate()
    {
        let binding = ToolBinding::new(tool(json!({"properties":{"q":schema}}))).unwrap();
        let error = binding.validate_json(value.clone()).err().unwrap();
        assert_eq!(error.code, "ValidationLimitExceeded");
        assert_eq!(
            error.execution_certainty,
            mcpx_core::ExecutionCertainty::NotSent
        );
        assert_eq!(error.issues.len(), 1);
        if index == 0 {
            assert_eq!(error.issues[0].instance_path, "/q");
        } else {
            // Boolean-only applicator probes may retain only the schema site.
            assert!(["", "/q"].contains(&error.issues[0].instance_path.as_str()));
        }
        assert!(error.issues[0].schema_path.ends_with("/pattern"));
        assert!(!error.message.contains(&"a".repeat(40)));
    }
    let tautology = ToolBinding::new(tool(
        json!({"properties":{"q":{"anyOf":[costly,{"not":costly}]}}}),
    ))
    .unwrap();
    assert!(tautology.validate_json(value).is_ok());
}

#[test]
fn deterministic_zero_width_repetitions_and_nested_static_expansion_are_bounded() {
    for pattern in [
        r"((?=a)){1000000000}",
        r"((?=a)){4097,}",
        r"(?:((?=a)){4096}){4096}",
        &format!("{}a{}", "(".repeat(70), ")".repeat(70)),
    ] {
        assert_eq!(
            code(ToolBinding::new(tool(
                json!({"properties":{"q":{"pattern":pattern}}})
            ))),
            "SchemaUnsupported"
        );
    }
    let binding = ToolBinding::new(tool(
        json!({"properties":{"q":{"pattern":r"((?=a)){4096}a"}}}),
    ))
    .unwrap();
    assert!(binding.validate_json(json!({"q":"a"})).is_ok());
}

#[test]
fn output_regexes_and_registry_replacement_obey_the_same_policy() {
    let mut descriptor = tool(json!({"type":"object"}));
    descriptor.output_schema = Some(json!({"properties":{"q":{"pattern":r"^(a|a)*\1b"}}}));
    let binding = ToolBinding::new(descriptor).unwrap();
    let raw = json!({"q":format!("{}!", "a".repeat(40))});
    assert!(matches!(
        binding.validate_output_status(Some(&raw)),
        mcpx_core::OutputValidation::Indeterminate
    ));
    let response = mcpx_core::ToolResponse {
        operation_id: Some(19),
        queue_operation_id: Some(7),
        raw: json!({"content":[],"structuredContent":raw}),
        is_error: false,
        output_validation: binding.validate_output_status(Some(&raw)),
    };
    let error = response.deserialize::<Value>().unwrap_err();
    assert_eq!(error.code, "OutputValidationLimitExceeded");
    assert_eq!(
        error.execution_certainty,
        mcpx_core::ExecutionCertainty::ResultReceived
    );
    assert_eq!(
        (error.operation_id, error.queue_operation_id),
        (Some(19), Some(7))
    );
    assert_eq!(response.raw["structuredContent"], raw);
    assert_eq!(
        binding.validate_output(Some(&json!({"q":format!("{}!", "a".repeat(40))}))),
        Some(false)
    );
    let mut catalog = Catalog::default();
    catalog
        .replace(vec![tool(json!({"type":"object"}))])
        .unwrap();
    let retained = catalog.bind("regex").unwrap();
    let revision = catalog.revision_source().snapshot();
    assert_eq!(
        code(SchemaRegistry::new([(
            "https://schemas.example/too-large".into(),
            json!({"pattern":"x".repeat(MAX_SCHEMA_PATTERN_BYTES + 1)})
        )])),
        "LimitExceeded"
    );
    assert_eq!(catalog.revision_source().snapshot(), revision);
    catalog.check(&retained).unwrap();
    retained.args().finish().unwrap();
}

#[test]
fn wide_invalid_objects_return_one_path_without_collecting_a_flat_error_for_each_field() {
    let required: Vec<_> = (0..4096).map(|i| format!("p{i:04}")).collect();
    let binding = ToolBinding::new(tool(json!({"type":"object","required":required}))).unwrap();
    let error = binding.args().finish().err().unwrap();
    assert_eq!(error.code, "ValidationFailed");
    assert_eq!(error.issues.len(), 1);
    assert_eq!(error.issues[0].instance_path, "");
    assert_eq!(error.issues[0].schema_path, "/required");
}
