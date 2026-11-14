use mcpx_core::{
    Catalog, SCHEMA_VOCABULARIES, SchemaRegistry, SchemaResource, ToolBinding, ToolDescriptor,
};
use serde_json::{Value, json};

fn tool(schema: Value) -> ToolDescriptor {
    serde_json::from_value(json!({"name":"test","inputSchema":schema})).unwrap()
}
fn code<T>(result: mcpx_core::Result<T>) -> String {
    result.err().expect("expected failure").code
}

#[test]
fn registered_documents_keep_retrieval_aliases_relative_ids_anchors_and_recursive_refs() {
    let registry = SchemaRegistry::new([
        ("https://schemas.example/download.json".into(), json!({"$id":"https://schemas.example/common.json","$defs":{
            "word":{"$anchor":"word","type":"string"},
            "node":{"$id":"node.json","type":"object","properties":{"name":{"$ref":"common.json#word"},"next":{"$ref":"#"}}}
        }})),
    ]).unwrap();
    for reference in [
        "https://schemas.example/download.json#word",
        "https://schemas.example/common.json#word",
    ] {
        let binding = ToolBinding::with_schema_registry(
            tool(json!({"type":"object","properties":{"q":{"$ref":reference}}})),
            &registry,
        )
        .unwrap();
        assert!(binding.validate_json(json!({"q":"yes"})).is_ok());
        assert_eq!(
            code(binding.validate_json(json!({"q":1}))),
            "ValidationFailed"
        );
    }
    let binding = ToolBinding::with_schema_registry(
        tool(json!({"$id":"https://schemas.example/call.json","$ref":"node.json"})),
        &registry,
    )
    .unwrap();
    drop(registry);
    assert!(
        binding
            .validate_json(json!({"name":"root","next":{"name":"leaf"}}))
            .is_ok()
    );
    assert_eq!(
        code(binding.validate_json(json!({"next":{"name":3}}))),
        "ValidationFailed"
    );
}

#[test]
fn a_tool_root_already_in_a_closed_recursive_bundle_reuses_the_identical_resource() {
    let first = json!({"$id":"https://schemas.example/a","type":"object","properties":{"value":{"type":"integer"},"next":{"$ref":"b"}}});
    let second = json!({"$id":"https://schemas.example/b","type":"object","properties":{"next":{"$ref":"a"}}});
    let registry = SchemaRegistry::new([
        ("https://schemas.example/a".into(), first.clone()),
        ("https://schemas.example/b".into(), second),
    ])
    .unwrap();
    let binding = ToolBinding::with_schema_registry(tool(first), &registry).unwrap();
    assert!(
        binding
            .validate_json(json!({"next":{"next":{"value":1}}}))
            .is_ok()
    );
    assert_eq!(
        code(binding.validate_json(json!({"next":{"next":{"value":false}}}))),
        "ValidationFailed"
    );
}

#[test]
fn registration_checks_unused_schema_syntax_before_a_root_can_reuse_it_by_id() {
    let invalid = json!({"$id":"https://schemas.example/invalid","type":"object","$defs":{"unused":{"type":"not-a-type"}}});
    assert_eq!(
        code(SchemaRegistry::new([(
            "https://schemas.example/invalid".into(),
            invalid
        )])),
        "SchemaUnsupported"
    );
}

#[test]
fn absent_http_and_file_references_never_become_retrieval_capabilities() {
    for reference in ["http://127.0.0.1:9/private-schema", "file:///etc/passwd"] {
        assert_eq!(
            code(ToolBinding::new(tool(json!({"$ref":reference})))),
            "SchemaReferenceUnavailable"
        );
        assert_eq!(
            code(SchemaRegistry::new([(
                "https://schemas.example/a".into(),
                json!({"$ref":reference})
            )])),
            "SchemaReferenceUnavailable"
        );
    }
    let registry = SchemaRegistry::new([(
        "file:///schemas/explicit-memory.json".into(),
        json!({"type":"object","required":["q"]}),
    )])
    .unwrap();
    let binding = ToolBinding::with_schema_registry(
        tool(json!({"$ref":"file:///schemas/explicit-memory.json"})),
        &registry,
    )
    .unwrap();
    assert_eq!(code(binding.validate_json(json!({}))), "ValidationFailed");
}

#[test]
fn vocabulary_support_is_exact_and_format_assertion_is_not_silently_ignored() {
    for vocabulary in SCHEMA_VOCABULARIES {
        ToolBinding::new(tool(
            json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","$vocabulary":{*vocabulary:true}}),
        ))
        .unwrap();
    }
    for vocabulary in [
        "https://json-schema.org/draft/2020-12/vocab/made-up",
        "https://json-schema.org/draft/2020-12/vocab/format-assertion",
        "https://example.com/required",
    ] {
        assert_eq!(
            code(ToolBinding::new(tool(
                json!({"$vocabulary":{vocabulary:true}})
            ))),
            "SchemaUnsupported"
        );
    }
    for vocabulary in [
        json!(false),
        json!({"relative":true}),
        json!({"https://example.com/vocab":"true"}),
    ] {
        assert_eq!(
            code(ToolBinding::new(tool(json!({"$vocabulary":vocabulary})))),
            "SchemaUnsupported"
        );
    }
    let binding = ToolBinding::new(tool(json!({"type":"object","$vocabulary":{"https://example.com/optional":false},"properties":{
        "email":{"type":"string","format":"email"},"blob":{"type":"string","contentEncoding":"base64","contentMediaType":"application/json","contentSchema":{"type":"integer"}}
    }}))).unwrap();
    assert!(
        binding
            .validate_json(json!({"email":"invalid email","blob":"not base64"}))
            .is_ok()
    );
}

#[test]
fn required_vocabulary_matrix_enforces_ref_applicator_validation_and_unevaluated_keywords() {
    let cases = [
        (
            json!({"$defs":{"node":{"type":"object","properties":{"value":{"type":"integer"},"next":{"$ref":"#/$defs/node"}}}},"$ref":"#/$defs/node"}),
            json!({"next":{"value":1}}),
            vec![json!({"next":{"value":false}})],
        ),
        (
            json!({"type":"object","properties":{"q":{"type":"number","multipleOf":2,"minimum":0,"maximum":10}},"required":["q"]}),
            json!({"q":4}),
            vec![json!({"q":3}), json!({"q":12}), json!({})],
        ),
        (
            json!({"type":"object","properties":{"q":{"type":"array","contains":{"type":"integer"},"minContains":2,"maxContains":2,"minItems":2,"maxItems":4,"uniqueItems":true}}}),
            json!({"q":[1,2,"x"]}),
            vec![
                json!({"q":[1,"x"]}),
                json!({"q":[1,2,3]}),
                json!({"q":[1,1]}),
            ],
        ),
        (
            json!({"type":"object","allOf":[{"properties":{"q":{"type":"integer"}}}],"unevaluatedProperties":false}),
            json!({"q":1}),
            vec![json!({"q":1,"extra":2})],
        ),
        (
            json!({"type":"object","properties":{"q":{"type":"array","prefixItems":[{"type":"integer"}],"unevaluatedItems":false}}}),
            json!({"q":[1]}),
            vec![json!({"q":[1,2]})],
        ),
        (
            json!({"type":"object","properties":{"flag":{"type":"boolean"}},"patternProperties":{"^q$":{"type":"string","minLength":2}},"propertyNames":{"pattern":"^[a-z]+$"},"dependentSchemas":{"flag":{"required":["q"]}},"dependentRequired":{"q":["flag"]},"additionalProperties":false}),
            json!({"flag":true,"q":"ok"}),
            vec![
                json!({"flag":true}),
                json!({"q":"ok"}),
                json!({"flag":true,"q":1}),
                json!({"Q":"bad"}),
            ],
        ),
        (
            json!({"type":"object","properties":{"kind":{"type":"string"},"q":{"type":"integer"}},"if":{"properties":{"kind":{"const":"positive"}},"required":["kind"]},"then":{"properties":{"q":{"minimum":1}}},"else":{"properties":{"q":{"maximum":0}}}}),
            json!({"kind":"positive","q":2}),
            vec![
                json!({"kind":"positive","q":0}),
                json!({"kind":"other","q":2}),
            ],
        ),
    ];
    for (mut schema, valid, invalid) in cases {
        schema["$vocabulary"] = Value::Object(
            SCHEMA_VOCABULARIES
                .iter()
                .map(|uri| ((*uri).into(), json!(true)))
                .collect(),
        );
        let binding = ToolBinding::new(tool(schema)).unwrap();
        assert!(binding.validate_json(valid).is_ok());
        for input in invalid {
            assert_eq!(code(binding.validate_json(input)), "ValidationFailed");
        }
    }
}

#[test]
fn annotations_remain_data_but_explicitly_referenced_targets_are_audited() {
    assert_eq!(
        code(ToolBinding::new(tool(
            json!({"properties":{"q":{"$ref":"#/x-extension"}},"x-extension":{"$defs":{"unused":{"type":"not-a-type"}}}})
        ))),
        "SchemaUnsupported"
    );
    let data = json!({"$vocabulary":{"https://example.com/private":true},"$ref":"https://not-registered.example"});
    ToolBinding::new(tool(
        json!({"type":"object","default":data,"x-extension":data,"const":data}),
    ))
    .unwrap();
    assert_eq!(
        code(ToolBinding::new(tool(
            json!({"properties":{"q":{"$ref":"#/x-extension"}},"x-extension":{"type":"object","$vocabulary":{"https://example.com/private":true}}})
        ))),
        "SchemaUnsupported"
    );
    for schema in [
        json!({"$defs":{"nested":{"$schema":"http://json-schema.org/draft-07/schema#"}}}),
        json!({"if":{"$vocabulary":{"https://example.com/required":true}}}),
    ] {
        assert_eq!(code(ToolBinding::new(tool(schema))), "SchemaUnsupported");
    }
}

#[test]
fn duplicate_resource_and_anchor_identities_cannot_shadow_other_documents() {
    assert_eq!(
        code(SchemaRegistry::new([
            ("https://schemas.example/a".into(), json!({})),
            (
                "HTTPS://SCHEMAS.EXAMPLE/a".into(),
                json!({"type":"integer"})
            ),
        ])),
        "SchemaUnsupported"
    );
    assert_eq!(
        code(SchemaRegistry::new([
            (
                "https://schemas.example/a".into(),
                json!({"$id":"https://schemas.example/canonical"})
            ),
            (
                "https://schemas.example/b".into(),
                json!({"$id":"https://schemas.example/canonical"})
            ),
        ])),
        "SchemaUnsupported"
    );
    assert_eq!(
        code(ToolBinding::new(tool(
            json!({"$defs":{"a":{"$anchor":"same"},"b":{"$anchor":"same"}}})
        ))),
        "SchemaUnsupported"
    );
    let registry =
        SchemaRegistry::new([("https://schemas.example/a".into(), json!({"type":"string"}))])
            .unwrap();
    assert_eq!(
        code(ToolBinding::with_schema_registry(
            tool(json!({"$id":"https://schemas.example/a"})),
            &registry
        )),
        "SchemaUnsupported"
    );
}

#[test]
fn relative_embedded_resources_and_dynamic_refs_need_no_external_registration() {
    let binding = ToolBinding::new(tool(
        json!({"$id":"relative-root.json","$dynamicAnchor":"tree","type":"object","properties":{
        "q":{"$ref":"child.json"},"next":{"$dynamicRef":"#tree"}
    },"$defs":{"child":{"$id":"child.json","type":"string"}}}),
    ))
    .unwrap();
    assert!(
        binding
            .validate_json(json!({"q":"ok","next":{"q":"again"}}))
            .is_ok()
    );
    assert_eq!(
        code(binding.validate_json(json!({"next":{"q":false}}))),
        "ValidationFailed"
    );
}

#[test]
fn resource_json_is_lossless_and_boolean_schemas_are_valid_resources() {
    let registry = SchemaRegistry::from_resources([
        SchemaResource {
            uri: "https://schemas.example/large".into(),
            schema_json: r#"{"type":"integer","const":18446744073709551615}"#.into(),
        },
        SchemaResource {
            uri: "https://schemas.example/no".into(),
            schema_json: "false".into(),
        },
    ])
    .unwrap();
    let descriptor =
        tool(json!({"type":"object","properties":{"q":{"$ref":"https://schemas.example/large"}}}));
    let binding = ToolBinding::with_schema_registry(descriptor, &registry).unwrap();
    assert!(binding.validate_json(json!({"q":u64::MAX})).is_ok());
    assert_eq!(
        code(binding.validate_json(json!({"q":u64::MAX-1}))),
        "ValidationFailed"
    );
    let never = ToolBinding::with_schema_registry(
        tool(json!({"$ref":"https://schemas.example/no"})),
        &registry,
    )
    .unwrap();
    assert_eq!(code(never.validate_json(json!({}))), "ValidationFailed");
    assert_eq!(registry.stats().resources, 2);
    assert!(registry.stats().bytes < 1024 && registry.stats().schema_nodes >= 2);
}

#[test]
fn changing_or_invalidating_a_catalog_discards_its_registry_and_old_bindings() {
    let mut catalog = Catalog::default();
    catalog
        .replace(vec![tool(json!({"$ref":"https://schemas.example/args"}))])
        .unwrap();
    catalog.set_schema_registry(
        SchemaRegistry::new([(
            "https://schemas.example/args".into(),
            json!({"type":"object","required":["first"]}),
        )])
        .unwrap(),
    );
    let first = catalog.bind("test").unwrap();
    catalog.set_schema_registry(
        SchemaRegistry::new([(
            "https://schemas.example/args".into(),
            json!({"type":"object","required":["second"]}),
        )])
        .unwrap(),
    );
    assert_eq!(code(catalog.check(&first)), "BindingStale");
    let second = catalog.bind("test").unwrap();
    assert_eq!(
        code(second.validate_json(json!({"first":1}))),
        "ValidationFailed"
    );
    assert!(first.validate_json(json!({"first":1})).is_ok()); // Immutable standalone snapshot.
    catalog.invalidate();
    assert_eq!(catalog.schema_registry().stats().resources, 0);
}

#[test]
fn resource_count_source_bytes_document_size_and_graph_work_have_independent_limits() {
    let records: Vec<_> = (0..129)
        .map(|n| SchemaResource {
            uri: format!("https://schemas.example/{n}"),
            schema_json: "{}".into(),
        })
        .collect();
    assert_eq!(
        code(SchemaRegistry::from_json(
            &serde_json::to_string(&records).unwrap()
        )),
        "LimitExceeded"
    );
    assert_eq!(
        code(SchemaRegistry::from_json("[] {}")),
        "ConfigurationError"
    );
    assert_eq!(
        code(SchemaRegistry::new((0..129).map(|n| (
            format!("https://schemas.example/{n}"),
            json!({})
        )))),
        "LimitExceeded"
    );
    assert_eq!(
        code(SchemaRegistry::new([(
            "https://schemas.example/large".into(),
            json!({"$comment":"x".repeat(1024*1024)})
        )])),
        "LimitExceeded"
    );
    assert_eq!(
        code(SchemaRegistry::new((0..10).map(|n| (
            format!("https://schemas.example/{n}"),
            json!({"$comment":"x".repeat(900_000)})
        )))),
        "LimitExceeded"
    );
    let properties: serde_json::Map<_, _> =
        (0..16_384).map(|n| (format!("p{n}"), json!({}))).collect();
    assert_eq!(
        code(ToolBinding::new(tool(json!({"properties":properties})))),
        "LimitExceeded"
    );
    assert_eq!(
        code(SchemaRegistry::from_json(
            r#"[{"uri":"https://schemas.example/a","schema_json":"{}","unexpected":true}]"#
        )),
        "ConfigurationError"
    );
}
