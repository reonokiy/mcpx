use mcpx_core::{OutputValidation, ToolBinding, ToolResponse};
use serde::{
    Deserialize, Serialize, Serializer,
    ser::{SerializeMap, SerializeSeq, SerializeTuple},
};
use serde_json::{Value, json};
use std::{cell::Cell, collections::BTreeMap};

fn binding(schema: Value) -> ToolBinding {
    ToolBinding::new(serde_json::from_value(json!({"name":"typed","inputSchema":schema})).unwrap())
        .unwrap()
}
fn any() -> ToolBinding {
    binding(json!({"type":"object"}))
}
#[derive(Serialize)]
struct Root<T> {
    value: T,
}

struct Once<'a>(&'a Cell<usize>);
impl Serialize for Once<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let calls = self.0.replace(self.0.get() + 1);
        s.serialize_f64(if calls == 0 { 7.5 } else { f64::NAN })
    }
}
#[test]
fn caller_serializers_run_once_for_builders_and_complete_argument_types() {
    let tool = any();
    let calls = Cell::new(0);
    let args = tool
        .args()
        .set("value", Once(&calls))
        .unwrap()
        .finish()
        .unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(args.value(), &json!({"value":7.5}));
    calls.set(0);
    let args = tool
        .args_from(Root {
            value: Once(&calls),
        })
        .unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(args.value(), &json!({"value":7.5}));
}
#[derive(Serialize)]
struct Wrapped(f64);
#[derive(Serialize)]
struct Tuple(u32, f64);
#[derive(Serialize)]
enum Variant {
    New(f64),
    Tuple(u32, f64),
    Struct { number: f64 },
}
fn rejects_nonfinite(value: impl Serialize) {
    assert_eq!(
        any().args_from(Root { value }).err().unwrap().code,
        "NumberOutOfRange"
    );
}
#[test]
fn every_compound_shape_keeps_nonfinite_checks_instead_of_null_coercion() {
    for number in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        rejects_nonfinite(number);
        rejects_nonfinite(Some(number));
        rejects_nonfinite(vec![number]);
        rejects_nonfinite((1, number));
        rejects_nonfinite(Wrapped(number));
        rejects_nonfinite(Tuple(1, number));
        rejects_nonfinite(Variant::New(number));
        rejects_nonfinite(Variant::Tuple(1, number));
        rejects_nonfinite(Variant::Struct { number });
        rejects_nonfinite(BTreeMap::from([("nested", number)]));
    }
    rejects_nonfinite(f32::NAN);
}
struct BadMapKey;
impl Serialize for BadMapKey {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(Some(1))?;
        map.serialize_entry(&f64::NAN, &1)?;
        map.end()
    }
}
struct IgnoreError;
impl Serialize for IgnoreError {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(None)?;
        let _ = map.serialize_entry("bad", &f64::NAN);
        map.serialize_entry("good", &1)?;
        map.end()
    }
}
#[test]
fn invalid_key_events_and_ignored_serialization_errors_cannot_hide_nonfinite_data() {
    rejects_nonfinite(BadMapKey);
    rejects_nonfinite(IgnoreError);
}
#[derive(Serialize)]
struct Arguments {
    #[serde(rename = "id")]
    identifier: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    absent: Option<String>,
    nullable: Option<String>,
    nested: Variant,
}
#[test]
fn existing_types_preserve_serde_names_absence_null_enums_and_exact_numbers() {
    let tool = binding(
        json!({"type":"object","properties":{"id":{"type":"integer","const":u64::MAX},"nullable":{"type":"null"},"nested":{"type":"object"}},"required":["id","nullable","nested"],"additionalProperties":false}),
    );
    let args = tool
        .args_from(Arguments {
            identifier: u64::MAX,
            absent: None,
            nullable: None,
            nested: Variant::Struct { number: 2.5 },
        })
        .unwrap();
    assert_eq!(
        args.value(),
        &json!({"id":u64::MAX,"nullable":null,"nested":{"Struct":{"number":2.5}}})
    );
    assert!(args.value().to_string().contains("18446744073709551615"));
    let from_value = tool.args_from(args.value()).unwrap();
    assert_eq!(from_value.value(), args.value());
    assert_eq!(
        any()
            .args_from(Root { value: u128::MAX })
            .err()
            .unwrap()
            .code,
        "NumberOutOfRange"
    );
    assert_eq!(
        any()
            .args_from(Root { value: i128::MIN })
            .err()
            .unwrap()
            .code,
        "NumberOutOfRange"
    );
}
#[derive(Serialize)]
struct Search {
    query: String,
    limit: u8,
}
#[test]
fn full_object_schemas_and_binding_identity_still_apply_to_typed_arguments() {
    let schema = json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","maximum":10}},"required":["query","limit"],"additionalProperties":false});
    let first = binding(schema.clone());
    let other = binding(schema);
    let args = first
        .args_from(Search {
            query: "rust".into(),
            limit: 5,
        })
        .unwrap();
    first.check_args(&args).unwrap();
    assert_eq!(other.check_args(&args).unwrap_err().code, "BindingMismatch");
    let error = first
        .args_from(Search {
            query: "private query".into(),
            limit: 11,
        })
        .err()
        .unwrap();
    assert_eq!(error.code, "ValidationFailed");
    assert_eq!(error.issues[0].instance_path, "/limit");
    assert!(!error.message.contains("private query"));
    assert_eq!(
        first.args_from("not a command line").err().unwrap().code,
        "ValidationFailed"
    );
}
struct Failure;
impl Serialize for Failure {
    fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("private serializer data"))
    }
}
struct Recursion;
impl Serialize for Recursion {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_some(self)
    }
}
#[test]
fn serializer_errors_are_sanitized_and_final_bytes_json_and_serde_depth_remain_bounded() {
    let error = any().args_from(Failure).err().unwrap();
    assert_eq!(error.code, "SerializationFailed");
    assert!(!error.message.contains("private serializer data"));
    assert_eq!(
        any()
            .args_from(Root {
                value: "x".repeat(mcpx_core::MAX_ARGUMENT_BYTES)
            })
            .err()
            .unwrap()
            .code,
        "LimitExceeded"
    );
    assert_eq!(
        any()
            .args_from(Root { value: Recursion })
            .err()
            .unwrap()
            .code,
        "LimitExceeded"
    );
    let mut value = json!(null);
    for _ in 0..65 {
        value = json!([value]);
    }
    assert_eq!(
        any().args_from(Root { value }).err().unwrap().code,
        "LimitExceeded"
    );
}
struct HugeHint;
impl Serialize for HugeHint {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(usize::MAX))?;
        seq.serialize_element(&1)?;
        seq.end()
    }
}
struct HugeTupleHint;
impl Serialize for HugeTupleHint {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_tuple(usize::MAX)?;
        seq.serialize_element(&2)?;
        seq.end()
    }
}
#[test]
fn length_hints_do_not_preallocate_unbounded_storage_for_small_values() {
    assert_eq!(
        any().args_from(Root { value: HugeHint }).unwrap().value(),
        &json!({"value":[1]})
    );
    assert_eq!(
        any()
            .args_from(Root {
                value: HugeTupleHint
            })
            .unwrap()
            .value(),
        &json!({"value":[2]})
    );
}

#[derive(Debug, Deserialize, PartialEq)]
struct Reply {
    id: u64,
    label: Option<String>,
}
fn response(status: OutputValidation, is_error: bool) -> ToolResponse {
    ToolResponse {
        operation_id: None,
        queue_operation_id: None,
        raw: json!({"content":[{"type":"text","text":"never parsed"}],"structuredContent":{"id":u64::MAX,"label":null},"opaque":"retained"}),
        is_error,
        output_validation: status,
    }
}
#[test]
fn structured_results_deserialize_without_consuming_or_reinterpreting_raw_content() {
    for status in [OutputValidation::Valid, OutputValidation::NotDeclared] {
        let response = response(status, false);
        let raw = response.raw.clone();
        assert_eq!(
            response.deserialize::<Reply>().unwrap(),
            Reply {
                id: u64::MAX,
                label: None
            }
        );
        assert_eq!(response.raw, raw);
        assert_eq!(
            response
                .deserialize::<BTreeMap<String, u8>>()
                .unwrap_err()
                .code,
            "DeserializationFailed"
        );
        assert_eq!(response.raw, raw);
    }
}
#[test]
fn business_and_schema_failures_are_not_converted_into_successful_rust_results() {
    assert_eq!(
        response(OutputValidation::NotApplicable, true)
            .deserialize::<Reply>()
            .unwrap_err()
            .code,
        "ToolFailure"
    );
    assert_eq!(
        response(OutputValidation::Invalid, false)
            .deserialize::<Reply>()
            .unwrap_err()
            .code,
        "OutputValidation"
    );
    for value in [
        json!({"content":[{"type":"text","text":"{\"id\":1}"}]}),
        json!({"structuredContent":null}),
        json!({"structuredContent":"{\"id\":1}"}),
    ] {
        let response = ToolResponse {
            operation_id: None,
            queue_operation_id: None,
            raw: value,
            is_error: false,
            output_validation: OutputValidation::NotDeclared,
        };
        assert_eq!(
            response.deserialize::<Reply>().unwrap_err().code,
            "StructuredContentMissing"
        );
    }
}
struct Refuse;
impl<'de> Deserialize<'de> for Refuse {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom("private deserializer data"))
    }
}
#[test]
fn target_deserializer_diagnostics_do_not_leak_result_data() {
    let response = response(OutputValidation::Valid, false);
    let raw = response.raw.clone();
    let error = response.deserialize::<Refuse>().err().unwrap();
    assert_eq!(error.code, "DeserializationFailed");
    assert!(!error.message.contains("private deserializer data"));
    assert_eq!(raw, response.raw);
}
