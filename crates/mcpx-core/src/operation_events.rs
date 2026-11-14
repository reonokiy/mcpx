//! Host queue lifecycle, separate from protocol events and their sequence domain.
use crate::{
    CallOutcome, Error, Event, EventLimits, EventStats, ExecutionCertainty, MAX_HOST_ID, Result,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationEventKind {
    Queued,
    Started,
    Completed,
    NeedsInput,
    Failed,
    Cancelled,
}
impl OperationEventKind {
    fn wire(self) -> &'static str {
        match self {
            Self::Queued => "operation_queued",
            Self::Started => "operation_started",
            Self::Completed => "operation_completed",
            Self::NeedsInput => "operation_needs_input",
            Self::Failed => "operation_failed",
            Self::Cancelled => "operation_cancelled",
        }
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct OperationEvent {
    pub queue_operation_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub core_operation_id: Option<u64>,
    pub sequence: u64,
    pub kind: OperationEventKind,
    /// Bounded diagnostics/status only; never tool arguments, results or credentials.
    pub payload: Value,
}
#[derive(Debug, Default, Serialize)]
pub struct OperationEventBatch {
    pub events: Vec<OperationEvent>,
    pub dropped: u64,
}

/// Bounded owner-local queue journal. Admission reserves one terminal record.
/// A host supplies synchronization/wakeup; it must settle every admitted ID.
#[derive(Default)]
pub struct OperationEvents {
    queue: crate::event_queue::EventQueue,
    active: BTreeMap<u64, bool>,
    next_id: u64,
    disabled: bool,
}
impl OperationEvents {
    pub fn new(limits: EventLimits) -> Result<Self> {
        let mut value = Self::default();
        value.queue.set_limits(limits)?;
        Ok(value)
    }
    pub fn admit(&mut self) -> Result<u64> {
        let id = self
            .next_id
            .checked_add(1)
            .filter(|id| *id <= MAX_HOST_ID)
            .ok_or_else(|| Error::local("LimitExceeded", "queue operation IDs exhausted"))?;
        if !self.disabled {
            self.queue.reserve()?;
            if let Err(error) = self.push(
                id,
                None,
                OperationEventKind::Queued,
                json!({"execution_certainty":"not_sent"}),
            ) {
                self.queue.release_reservation();
                return Err(error);
            }
            self.active.insert(id, false);
        }
        self.next_id = id;
        Ok(id)
    }
    pub fn start(&mut self, id: u64) -> Result<()> {
        if self.disabled {
            return Ok(());
        }
        if self.active.get(&id) != Some(&false) {
            return Err(invalid());
        }
        self.push(
            id,
            None,
            OperationEventKind::Started,
            json!({"execution_certainty":"not_sent"}),
        )?;
        self.active.insert(id, true);
        Ok(())
    }
    pub fn complete(&mut self, id: u64, outcome: Option<&CallOutcome>) -> Result<()> {
        let (core, kind, payload) = match outcome {
            Some(CallOutcome::Completed(response)) => (
                response.operation_id,
                OperationEventKind::Completed,
                json!({"execution_certainty":"result_received", "is_error":response.is_error,"output_validation":response.output_validation}),
            ),
            Some(CallOutcome::NeedsInput(handle)) => (
                Some(handle.operation_id()),
                OperationEventKind::NeedsInput,
                json!({"execution_certainty":"needs_input"}),
            ),
            None => (
                None,
                OperationEventKind::Completed,
                json!({"execution_certainty":"not_sent"}),
            ),
        };
        self.finish(id, core, kind, payload)
    }
    pub fn fail(&mut self, id: u64, error: &Error) -> Result<()> {
        use crate::event_queue::short;
        // Bound the borrowed diagnostics before cloning/encoding, not after
        // constructing another potentially huge message/issue array.
        let started = self.active.get(&id) == Some(&true);
        let code = if error.code.len() <= 128 {
            error.code.clone()
        } else {
            "EventTooLarge".into()
        };
        let message = short(&error.message, 512);
        let issues: Vec<_> = error
            .issues
            .iter()
            .take(4)
            .filter(|issue| issue.instance_path.len() <= 64 && issue.schema_path.len() <= 64)
            .cloned()
            .collect();
        let truncated =
            code != error.code || message != error.message || issues.len() != error.issues.len();
        let core = if started {
            error
                .operation_id
                .filter(|id| (1..=MAX_HOST_ID).contains(id))
        } else {
            None
        };
        let certainty = if started {
            error.execution_certainty
        } else {
            ExecutionCertainty::NotSent
        };
        let retry = if started {
            error.retry_advice
        } else {
            Error::local(&code, "").retry_advice
        };
        let bounded = Error {
            code,
            message,
            issues,
            operation_id: core,
            queue_operation_id: Some(id),
            execution_certainty: certainty,
            retry_advice: retry,
        };
        let mut payload = serde_json::to_value(bounded).expect("serializable error");
        if truncated {
            payload["truncated"] = json!(true);
        }
        self.finish(
            id,
            core,
            if error.code == "Cancelled" {
                OperationEventKind::Cancelled
            } else {
                OperationEventKind::Failed
            },
            payload,
        )
    }

    fn finish(
        &mut self,
        id: u64,
        core: Option<u64>,
        kind: OperationEventKind,
        payload: Value,
    ) -> Result<()> {
        if self.disabled {
            return Ok(());
        }
        if !self.active.contains_key(&id) {
            return Err(invalid());
        }
        self.push(id, core, kind, payload)?;
        self.active.remove(&id);
        Ok(())
    }
    fn push(
        &mut self,
        id: u64,
        core: Option<u64>,
        kind: OperationEventKind,
        payload: Value,
    ) -> Result<()> {
        self.queue.push(Event {
            operation_id: core.unwrap_or(0),
            queue_operation_id: Some(id),
            sequence: 0,
            kind: kind.wire().into(),
            payload,
        })
    }
    pub fn drain(&mut self) -> OperationEventBatch {
        let batch = self.queue.drain();
        OperationEventBatch {
            dropped: batch.dropped,
            events: batch
                .events
                .into_iter()
                .map(|event| OperationEvent {
                    queue_operation_id: event.queue_operation_id.expect("queue event ID"),
                    core_operation_id: (event.operation_id != 0).then_some(event.operation_id),
                    sequence: event.sequence,
                    kind: match event.kind.as_str() {
                        "operation_queued" => OperationEventKind::Queued,
                        "operation_started" => OperationEventKind::Started,
                        "operation_completed" => OperationEventKind::Completed,
                        "operation_needs_input" => OperationEventKind::NeedsInput,
                        "operation_failed" => OperationEventKind::Failed,
                        "operation_cancelled" => OperationEventKind::Cancelled,
                        _ => unreachable!("queue event kind"),
                    },
                    payload: event.payload,
                })
                .collect(),
        }
    }
    pub fn stats(&self) -> EventStats {
        self.queue.stats()
    }
    /// Explicit unsubscription frees all records/reservations, without resetting IDs.
    pub fn disable(&mut self) {
        self.disabled = true;
        self.active.clear();
        self.queue = Default::default();
    }
    pub fn abandoned(&mut self, id: u64) -> Result<()> {
        let certainty = if self.active.get(&id) == Some(&true) {
            ExecutionCertainty::Unknown
        } else {
            ExecutionCertainty::NotSent
        };
        self.fail(
            id,
            &Error::new("TransportClosed", "queue owner stopped before delivery").with_context(
                crate::ErrorContext {
                    operation_id: None,
                    queue_operation_id: Some(id),
                    execution_certainty: certainty,
                },
            ),
        )
    }
}
fn invalid() -> Error {
    Error::local(
        "InvalidOperation",
        "queue operation is no longer active or already started",
    )
}

/// Owner-local host context for newly admitted core operations. Drop in LIFO order.
/// The guard retains only a scalar cell, never a Remote or transport.
#[must_use = "keep the guard alive while admitting host-associated operations"]
pub struct QueueScope {
    pub(crate) slot: std::rc::Rc<std::cell::Cell<Option<u64>>>,
    pub(crate) previous: Option<u64>,
}
impl Drop for QueueScope {
    fn drop(&mut self) {
        self.slot.set(self.previous);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn final_queue_id_settles_without_reuse_or_leaked_reservations() {
        let mut events = OperationEvents {
            next_id: MAX_HOST_ID - 1,
            ..Default::default()
        };
        let id = events.admit().unwrap();
        assert_eq!(id, MAX_HOST_ID);
        assert_eq!(events.admit().unwrap_err().code, "LimitExceeded");
        events.complete(id, None).unwrap();
        assert_eq!(events.stats().reserved_terminals, 0);
        assert_eq!(events.admit().unwrap_err().code, "LimitExceeded");
    }
}
