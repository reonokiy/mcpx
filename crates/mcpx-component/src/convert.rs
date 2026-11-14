use crate::abi;
use mcpx_core::{Action, CallOutcome, Event, OutputValidation, ToolDescriptor};

pub(crate) fn action(value: Action) -> abi::HostAction {
    match value {
        Action::Http {
            id,
            url,
            headers,
            body,
            rpc_id,
            rpc_method,
            operation_id,
            cancellation,
        } => abi::HostAction::Http(abi::HttpRequest {
            action_id: id,
            rpc_id,
            rpc_method,
            operation_id,
            cancellation: cancellation
                .map(|body| serde_json::to_vec(&body).expect("serializable cancellation")),
            method: abi::HttpMethod::Post,
            url,
            headers: headers
                .into_iter()
                .map(|(name, value)| abi::Header { name, value })
                .collect(),
            body: serde_json::to_vec(&body).expect("serializable protocol body"),
        }),
        Action::Stream {
            id,
            body,
            rpc_id,
            rpc_method,
            operation_id,
            cancellation,
        } => abi::HostAction::Stream(abi::StreamRequest {
            action_id: id,
            rpc_id,
            rpc_method,
            operation_id,
            cancellation: cancellation
                .map(|body| serde_json::to_vec(&body).expect("serializable cancellation")),
            body: serde_json::to_vec(&body).expect("serializable protocol body"),
        }),
        Action::Cancel { id } => abi::HostAction::Cancel(id),
    }
}

pub(crate) fn event(value: Event) -> abi::RuntimeEvent {
    let failure = match value.kind.as_str() {
        "failed" => serde_json::from_value(value.payload.clone())
            .ok()
            .map(crate::error),
        "cancelled" => Some(crate::error(
            serde_json::from_value(value.payload.clone()).unwrap_or_else(|_| {
                mcpx_core::Error::new("Cancelled", "Operation cancelled; execution may be unknown")
            }),
        )),
        _ => None,
    };
    let kind = match value.kind.as_str() {
        "catalog_updated" => abi::EventKind::CatalogUpdated,
        "catalog_stale" => abi::EventKind::CatalogStale,
        "progress" => abi::EventKind::Progress,
        "log" => abi::EventKind::Log,
        "notification" => abi::EventKind::Notification,
        "completed" => abi::EventKind::Completed,
        "tool_failed" => abi::EventKind::ToolFailed,
        "failed" => abi::EventKind::Failed,
        "cancelled" => abi::EventKind::Cancelled,
        "needs_input" => abi::EventKind::NeedsInput,
        _ => abi::EventKind::Extension(value.kind),
    };
    abi::RuntimeEvent {
        operation_id: value.operation_id,
        queue_operation_id: value.queue_operation_id,
        sequence: value.sequence,
        kind,
        failure,
        payload_json: value.payload.to_string(),
    }
}

pub(crate) fn outcome(value: CallOutcome) -> abi::CallOutcome {
    match value {
        CallOutcome::Completed(value) => abi::CallOutcome::Completed(abi::ToolResponse {
            operation_id: value.operation_id,
            queue_operation_id: value.queue_operation_id,
            raw_json: value.raw.to_string(),
            is_error: value.is_error,
            output_validation: match value.output_validation {
                OutputValidation::NotDeclared => abi::OutputValidation::NotDeclared,
                OutputValidation::Valid => abi::OutputValidation::Valid,
                OutputValidation::Invalid => abi::OutputValidation::Invalid,
                OutputValidation::Indeterminate => abi::OutputValidation::Indeterminate,
                OutputValidation::NotApplicable => abi::OutputValidation::NotApplicable,
            },
        }),
        CallOutcome::NeedsInput(value) => abi::CallOutcome::NeedsInput(abi::Continuation {
            operation_id: value.operation_id(),
            queue_operation_id: value.queue_operation_id(),
            id: value.id(),
            expires_at_ms: value.expires_at_ms(),
            raw_json: value.raw().to_string(),
        }),
    }
}

pub(crate) fn tool(value: ToolDescriptor) -> abi::ToolDescriptor {
    abi::ToolDescriptor {
        name: value.name,
        description: value.description,
        input_schema_json: value.input_schema.to_string(),
        output_schema_json: value.output_schema.map(|schema| schema.to_string()),
        extensions_json: serde_json::to_string(&value.extra).expect("serializable tool extensions"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn typed_actions_keep_byte_bodies_and_full_business_numbers() {
        let body = json!({"n":u64::MAX,"text":"雪\0"});
        let expected = serde_json::to_vec(&body).unwrap();
        let http = action(Action::Http {
            id: mcpx_core::MAX_HOST_ID,
            rpc_id: Some(2),
            rpc_method: "tools/call".into(),
            operation_id: 3,
            cancellation: None,
            url: "http://localhost/mcp".into(),
            headers: BTreeMap::from([("x-extension".into(), "value".into())]),
            body: body.clone(),
        });
        let abi::HostAction::Http(http) = http else {
            panic!()
        };
        assert_eq!(http.action_id, mcpx_core::MAX_HOST_ID);
        assert!(matches!(http.method, abi::HttpMethod::Post));
        assert_eq!(http.body, expected);
        assert_eq!(http.headers[0].value, "value");
        let abi::HostAction::Stream(stream) = action(Action::Stream {
            id: 42,
            body,
            rpc_id: Some(2),
            rpc_method: "tools/call".into(),
            operation_id: 3,
            cancellation: None,
        }) else {
            panic!()
        };
        assert_eq!(stream.body, expected);
        assert!(matches!(
            action(Action::Cancel { id: 42 }),
            abi::HostAction::Cancel(42)
        ));
    }

    #[test]
    fn event_and_outcome_mappings_keep_extensions_and_output_validation_states() {
        let event = event(Event {
            operation_id: mcpx_core::MAX_HOST_ID,
            queue_operation_id: Some(7),
            sequence: u64::MAX,
            kind: "future_event".into(),
            payload: json!({"future":u64::MAX}),
        });
        assert_eq!(event.sequence, u64::MAX);
        assert!(matches!(event.kind, abi::EventKind::Extension(kind) if kind == "future_event"));
        assert!(event.payload_json.contains("18446744073709551615"));
        for validation in [
            OutputValidation::NotDeclared,
            OutputValidation::Valid,
            OutputValidation::Invalid,
            OutputValidation::Indeterminate,
            OutputValidation::NotApplicable,
        ] {
            let expected = serde_json::to_string(&validation)
                .unwrap()
                .replace('_', "-");
            let outcome = outcome(CallOutcome::Completed(mcpx_core::ToolResponse {
                operation_id: None,
                queue_operation_id: None,
                raw: json!({"content":[],"n":u64::MAX}),
                is_error: true,
                output_validation: validation,
            }));
            let abi::CallOutcome::Completed(response) = outcome else {
                panic!()
            };
            assert!(response.is_error && response.raw_json.contains("18446744073709551615"));
            let actual = match response.output_validation {
                abi::OutputValidation::NotDeclared => "not-declared",
                abi::OutputValidation::Valid => "valid",
                abi::OutputValidation::Invalid => "invalid",
                abi::OutputValidation::Indeterminate => "indeterminate",
                abi::OutputValidation::NotApplicable => "not-applicable",
            };
            assert_eq!(format!("\"{actual}\""), expected);
        }
    }

    #[test]
    fn terminal_events_expose_typed_failure_codes_and_validation_pointers() {
        let event = event(Event {
            operation_id: 1,
            queue_operation_id: None,
            sequence: 1,
            kind: "failed".into(),
            payload: json!({"code":"ValidationFailed","message":"Arguments violate schema","issues":[{"instance_path":"/q","schema_path":"/properties/q/type"}]}),
        });
        let failure = event.failure.unwrap();
        assert_eq!(failure.code, "ValidationFailed");
        assert_eq!(failure.issues[0].instance_path, "/q");
    }

    #[test]
    fn descriptor_schemas_and_unknown_metadata_remain_lossless_json() {
        let descriptor = tool(ToolDescriptor {
            name: "future-tool".into(),
            description: None,
            input_schema: json!({"type":"object","properties":{"n":{"default":u64::MAX}}}),
            output_schema: Some(json!({"type":"object"})),
            extra: BTreeMap::from([("future-metadata".into(), json!({"n":u64::MAX}))]),
        });
        assert!(
            descriptor
                .input_schema_json
                .contains("18446744073709551615")
        );
        assert!(descriptor.extensions_json.contains("18446744073709551615"));
        assert_eq!(
            descriptor.output_schema_json.unwrap(),
            r#"{"type":"object"}"#
        );
    }
}
