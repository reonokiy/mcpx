use mcpx_core::{Catalog, ToolBinding, ToolDescriptor, parse_json};
use serde_json::{Value, json};

fn descriptor(schema: Value) -> ToolDescriptor {
    serde_json::from_value(json!({"name":"search", "inputSchema":schema})).unwrap()
}
fn binding(schema: Value) -> ToolBinding {
    ToolBinding::new(descriptor(schema)).unwrap()
}

#[test]
fn required_null_defaults_and_integer_types_remain_distinct() {
    let tool = binding(json!({"type":"object", "properties":{
        "query":{"type":"string"}, "limit":{"type":"integer","default":10},
        "nullable":{"type":["string","null"]}}, "required":["query"], "additionalProperties":false}));
    let args = tool.args().set("query", "rust").unwrap().finish().unwrap();
    assert_eq!(args.value(), &json!({"query":"rust"}));
    assert!(tool.args().set("query", 1).unwrap().finish().is_err());
    assert!(
        tool.args()
            .set("query", "rust")
            .unwrap()
            .set("limit", "10")
            .unwrap()
            .finish()
            .is_err()
    );
    let args = tool
        .args()
        .set("query", "rust")
        .unwrap()
        .set("nullable", Option::<String>::None)
        .unwrap()
        .finish()
        .unwrap();
    assert!(args.value()["nullable"].is_null());
    assert!(
        tool.args()
            .set("query", "rust")
            .unwrap()
            .set("query", "other")
            .is_err()
    );
}

#[test]
fn conditional_fields_are_checked_as_a_whole_object() {
    let tool = binding(json!({"type":"object", "oneOf":[
        {"properties":{"kind":{"const":"name"},"name":{"type":"string"}},"required":["kind","name"],"additionalProperties":false},
        {"properties":{"kind":{"const":"id"},"id":{"type":"integer"}},"required":["kind","id"],"additionalProperties":false}
    ]}));
    assert!(
        tool.args()
            .set("kind", "id")
            .unwrap()
            .set("id", 3)
            .unwrap()
            .finish()
            .is_ok()
    );
    assert!(
        tool.args()
            .set("kind", "id")
            .unwrap()
            .set("name", "x")
            .unwrap()
            .finish()
            .is_err()
    );
}

#[test]
fn refs_in_schema_are_distinct_from_refs_in_business_defaults() {
    let tool = binding(json!({"type":"object", "$defs":{"word":{"type":"string"}},
        "properties":{"q":{"$ref":"#/$defs/word"}}, "default":{"$ref":"https://not-a-schema.example"}}));
    assert!(tool.args().set("q", "value").unwrap().finish().is_ok());
    assert!(tool.args().set("q", 1).unwrap().finish().is_err());
    let unsupported = ToolBinding::new(descriptor(json!({"$ref":"https://example.com/schema"})));
    assert_eq!(
        unsupported.err().unwrap().code,
        "SchemaReferenceUnavailable"
    );
}

#[test]
fn bindings_are_remote_specific_and_known_schema_changes_invalidate_handles() {
    let mut first = Catalog::default();
    let descriptor = descriptor(json!({"type":"object"}));
    first.replace(vec![descriptor.clone()]).unwrap();
    let old = first.bind("search").unwrap();
    first.replace(vec![descriptor.clone()]).unwrap();
    assert!(first.check(&old).is_ok());
    let mut second = Catalog::default();
    second.replace(vec![descriptor]).unwrap();
    let other = second.bind("search").unwrap();
    let args = old.args().finish().unwrap();
    assert!(other.check_args(&args).is_err());
    first.replace(vec![]).unwrap();
    assert_eq!(first.check(&old).unwrap_err().code, "BindingStale");
}

#[test]
fn numeric_range_and_nonfinite_values_fail_without_rounding_to_null() {
    assert_eq!(
        parse_json("18446744073709551615").unwrap().as_u64(),
        Some(u64::MAX)
    );
    assert_eq!(
        parse_json("18446744073709551616").unwrap_err().code,
        "NumberOutOfRange"
    );
    assert!(parse_json("1e999").is_err());
    let tool = binding(json!({"type":"object"}));
    assert!(tool.args().set("n", f64::NAN).is_err());
    assert!(tool.args().set("n", vec![f64::INFINITY]).is_err());
}

#[test]
fn catalog_refresh_is_atomic_and_validation_errors_do_not_leak_values() {
    let mut catalog = Catalog::default();
    let descriptor = descriptor(json!({"type":"object","properties":{"token":{"type":"integer"}}}));
    catalog.replace(vec![descriptor.clone()]).unwrap();
    assert!(
        catalog
            .replace(vec![descriptor.clone(), descriptor])
            .is_err()
    );
    assert_eq!(catalog.tools().count(), 1);
    let error = catalog
        .bind("search")
        .unwrap()
        .validate_json(json!({"token":"SECRET"}))
        .err()
        .unwrap();
    assert!(!serde_json::to_string(&error).unwrap().contains("SECRET"));
    assert_eq!(error.issues[0].instance_path, "/token");
}

#[test]
fn nested_json_values_preserve_their_types_through_the_builder() {
    let tool = binding(
        json!({"type":"object","properties":{"options":{"type":"object","required":["count"],"properties":{"count":{"type":"integer"}}}}}),
    );
    let args = tool
        .args()
        .set("options", json!({"count":u64::MAX}))
        .unwrap()
        .finish()
        .unwrap();
    assert_eq!(args.value()["options"]["count"].as_u64(), Some(u64::MAX));
}
