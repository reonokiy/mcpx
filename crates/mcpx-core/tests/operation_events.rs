use mcpx_core::{
    CallOutcome, Error, ErrorContext, EventLimits, ExecutionCertainty, OperationEventKind as Kind,
    OperationEvents, OutputValidation, ToolResponse,
};
use serde_json::json;
#[test]
fn stopped_consumer_blocks_admission_and_never_evicts_a_terminal() {
    let mut events = OperationEvents::new(EventLimits {
        capacity: 4,
        max_bytes: 49152,
    })
    .unwrap();
    let ids: Vec<_> = (0..3).map(|_| events.admit().unwrap()).collect();
    assert_eq!(events.admit().unwrap_err().code, "ConsumerLagged");
    for id in &ids {
        events.start(*id).unwrap();
        events.complete(*id, None).unwrap();
    }
    let batch = events.drain();
    assert!(batch.dropped > 0);
    assert_eq!(
        batch
            .events
            .iter()
            .filter(|event| event.kind == Kind::Completed)
            .count(),
        3
    );
    assert!(
        batch
            .events
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence)
    );
    assert_eq!(events.stats().reserved_terminals, 0);
    assert_eq!(events.stats().bytes, 0);
    assert_eq!(events.admit().unwrap(), 4);
}
#[test]
fn oversized_errors_keep_execution_evidence_and_never_copy_business_results() {
    let mut events = OperationEvents::default();
    let id = events.admit().unwrap();
    events.start(id).unwrap();
    let error = Error::new("Timeout", "雪\0".repeat(100000)).with_context(ErrorContext {
        operation_id: Some(35),
        queue_operation_id: Some(999),
        execution_certainty: ExecutionCertainty::Unknown,
    });
    events.fail(id, &error).unwrap();
    let batch = events.drain();
    let event = batch.events.last().unwrap();
    assert_eq!(event.kind, Kind::Failed);
    assert_eq!(event.core_operation_id, Some(35));
    assert_eq!(event.payload["queue_operation_id"], id);
    assert_eq!(event.payload["execution_certainty"], "unknown");
    assert_eq!(event.payload["retry_advice"], "inspect_execution");
    assert_eq!(event.payload["truncated"], true);
    assert!(serde_json::to_string(event).unwrap().len() < 4096);
    let id = events.admit().unwrap();
    events.start(id).unwrap();
    events
        .complete(
            id,
            Some(&CallOutcome::Completed(ToolResponse {
                operation_id: Some(40),
                queue_operation_id: None,
                raw: json!({"secret":"PRIVATE".repeat(10000)}),
                is_error: true,
                output_validation: OutputValidation::NotApplicable,
            })),
        )
        .unwrap();
    let batch = events.drain();
    let event = batch.events.last().unwrap();
    assert_eq!(event.kind, Kind::Completed);
    assert_eq!(event.core_operation_id, Some(40));
    assert_eq!(event.payload["execution_certainty"], "result_received");
    assert_eq!(event.payload["is_error"], true);
    assert!(!serde_json::to_string(&batch).unwrap().contains("PRIVATE"));
}
#[test]
fn queue_cancellation_is_distinct_from_abandoned_processing_and_unsubscription() {
    let mut events = OperationEvents::default();
    let queued = events.admit().unwrap();
    events
        .fail(queued, &Error::new("Cancelled", "fixture"))
        .unwrap();
    let active = events.admit().unwrap();
    events.start(active).unwrap();
    assert_eq!(events.start(active).unwrap_err().code, "InvalidOperation");
    events.abandoned(active).unwrap();
    assert_eq!(
        events.complete(active, None).unwrap_err().code,
        "InvalidOperation"
    );
    let batch = events.drain();
    let cancelled = batch
        .events
        .iter()
        .find(|event| event.kind == Kind::Cancelled)
        .unwrap();
    assert_eq!(cancelled.payload["execution_certainty"], "not_sent");
    let abandoned = batch
        .events
        .iter()
        .find(|event| event.kind == Kind::Failed)
        .unwrap();
    assert_eq!(abandoned.payload["execution_certainty"], "unknown");
    events.disable();
    let next = events.admit().unwrap();
    assert_eq!(next, 3);
    events.complete(next, None).unwrap();
    assert!(events.drain().events.is_empty());
    assert_eq!(events.stats().reserved_terminals, 0);
}
