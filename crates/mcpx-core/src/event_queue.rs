use crate::{Error, Event, MAX_HOST_ID, Result};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    io::{self, Write},
};

pub const MAX_EVENT_RECORD_BYTES: usize = 16 * 1024;
const RECORD_ALLOWANCE: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct EventLimits {
    pub capacity: usize,
    pub max_bytes: usize,
}
impl Default for EventLimits {
    fn default() -> Self {
        Self {
            capacity: 256,
            max_bytes: 4 * 1024 * 1024,
        }
    }
}
impl EventLimits {
    pub fn validate(self) -> Result<()> {
        if !(4..=4096).contains(&self.capacity)
            || !(3 * MAX_EVENT_RECORD_BYTES..=64 * 1024 * 1024).contains(&self.max_bytes)
        {
            return Err(Error::local(
                "ConfigurationError",
                "event limits require 4..4096 records and 48 KiB..64 MiB",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Default, Serialize)]
pub struct EventBatch {
    pub events: Vec<Event>,
    pub dropped: u64,
}
#[derive(Debug, Clone, Copy, Serialize)]
pub struct EventStats {
    pub records: usize,
    pub bytes: usize,
    pub reserved_terminals: usize,
    pub dropped: u64,
    pub sequence: u64,
}

struct Counter {
    bytes: usize,
    limit: usize,
}
impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self.bytes.saturating_add(bytes.len());
        if self.bytes > self.limit {
            return Err(io::Error::other("event size limit"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encoded_len(event: &Event, limit: usize) -> Option<usize> {
    let mut counter = Counter { bytes: 0, limit };
    serde_json::to_writer(&mut counter, event).ok()?;
    Some(counter.bytes)
}
impl Event {
    /// A portable encoding allowance plus fixed record overhead; not allocator measurement.
    pub fn budget_bytes(&self) -> usize {
        encoded_len(self, usize::MAX)
            .expect("serializable event")
            .saturating_mul(2)
            .saturating_add(RECORD_ALLOWANCE)
    }
}
pub(crate) fn short(value: &str, limit: usize) -> String {
    let mut end = value.len().min(limit);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].into()
}
fn bounded(mut event: Event) -> (Event, usize) {
    if let Some(bytes) = encoded_len(&event, (MAX_EVENT_RECORD_BYTES - RECORD_ALLOWANCE) / 2) {
        return (event, bytes * 2 + RECORD_ALLOWANCE);
    }
    event.payload = if matches!(
        event.kind.as_str(),
        "failed" | "cancelled" | "operation_failed" | "operation_cancelled"
    ) {
        let code = event
            .payload
            .get("code")
            .and_then(Value::as_str)
            .filter(|code| code.len() <= 128)
            .unwrap_or("EventTooLarge");
        let message = event
            .payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("Event diagnostic exceeded its limit");
        let issues: Vec<_> = event
            .payload
            .get("issues")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .take(4)
            .filter(|issue| {
                ["instance_path", "schema_path"].iter().all(|field| {
                    issue
                        .get(field)
                        .and_then(Value::as_str)
                        .is_some_and(|path| path.len() <= 64)
                })
            })
            .map(|issue| {
                json!({
                    "instance_path":issue["instance_path"], "schema_path":issue["schema_path"]
                })
            })
            .collect();
        let mut value =
            json!({"code":code,"message":short(message,512),"issues":issues,"truncated":true});
        for key in ["operation_id", "queue_operation_id"] {
            if let Some(id) = event
                .payload
                .get(key)
                .and_then(Value::as_u64)
                .filter(|id| (1..=MAX_HOST_ID).contains(id))
            {
                value[key] = json!(id);
            }
        }
        for key in ["execution_certainty", "retry_advice"] {
            if let Some(field) = event.payload.get(key).and_then(Value::as_str) {
                let valid = if key == "execution_certainty" {
                    serde_json::from_value::<crate::ExecutionCertainty>(json!(field)).is_ok()
                } else {
                    serde_json::from_value::<crate::RetryAdvice>(json!(field)).is_ok()
                };
                if valid {
                    value[key] = json!(field);
                }
            }
        }
        value
    } else {
        json!({"truncated":true})
    };
    let bytes = event.budget_bytes();
    assert!(bytes <= MAX_EVENT_RECORD_BYTES, "bounded event metadata");
    (event, bytes)
}

struct Record {
    event: Event,
    bytes: usize,
}
#[derive(Default)]
pub(crate) struct EventQueue {
    records: VecDeque<Record>,
    bytes: usize,
    reserved: usize,
    dropped: u64,
    sequence: u64,
    limits: EventLimits,
}
impl EventQueue {
    pub(crate) fn set_limits(&mut self, limits: EventLimits) -> Result<()> {
        limits.validate()?;
        if limits == self.limits {
            return Ok(());
        }
        if !self.records.is_empty() || self.dropped != 0 || self.reserved != 0 {
            return Err(Error::local(
                "Busy",
                "drain events and finish operations before changing event limits",
            ));
        }
        self.limits = limits;
        Ok(())
    }
    pub(crate) fn can_reserve(&self) -> Result<()> {
        if self
            .sequence
            .saturating_add(self.reserved as u64)
            .saturating_add(1)
            > MAX_HOST_ID
        {
            return Err(Error::local("LimitExceeded", "event sequence exhausted"));
        }
        let (protected, protected_bytes) = self
            .records
            .iter()
            .filter(|record| record.event.is_terminal())
            .fold((0usize, 0usize), |(count, bytes), record| {
                (count + 1, bytes + record.bytes)
            });
        if protected + self.reserved + 1 > self.limits.capacity
            || protected_bytes + (self.reserved + 1) * MAX_EVENT_RECORD_BYTES
                > self.limits.max_bytes
        {
            return Err(Error::local(
                "ConsumerLagged",
                "drain terminal events before starting another operation",
            ));
        }
        Ok(())
    }
    fn drop_one(&mut self) -> Result<bool> {
        let Some(index) = self
            .records
            .iter()
            .position(|record| !record.event.is_terminal())
        else {
            return Ok(false);
        };
        self.count_drop()?;
        let record = self.records.remove(index).unwrap();
        self.bytes -= record.bytes;
        Ok(true)
    }
    fn count_drop(&mut self) -> Result<()> {
        if self.dropped == MAX_HOST_ID {
            return Err(Error::local(
                "LimitExceeded",
                "event loss counter exhausted; drain events",
            ));
        }
        self.dropped += 1;
        Ok(())
    }
    pub(crate) fn reserve(&mut self) -> Result<()> {
        self.can_reserve()?;
        while self.records.len() + self.reserved + 1 > self.limits.capacity
            || self.bytes + (self.reserved + 1) * MAX_EVENT_RECORD_BYTES > self.limits.max_bytes
        {
            assert!(
                self.drop_one()?,
                "can_reserve proved room after lossy eviction"
            );
        }
        self.reserved += 1;
        Ok(())
    }
    pub(crate) fn release_reservation(&mut self) {
        self.reserved = self.reserved.checked_sub(1).expect("reserved event");
    }
    pub(crate) fn push(&mut self, mut event: Event) -> Result<()> {
        let terminal = event.is_terminal();
        if !terminal
            && self
                .sequence
                .saturating_add(self.reserved as u64)
                .saturating_add(1)
                > MAX_HOST_ID
        {
            return Err(Error::local("LimitExceeded", "event sequence exhausted"));
        }
        event.sequence = self.sequence + 1;
        let (event, bytes) = bounded(event);
        if terminal {
            self.reserved = self
                .reserved
                .checked_sub(1)
                .expect("operation reserved its terminal event");
        }
        while self.records.len() + self.reserved + 1 > self.limits.capacity
            || self.bytes + self.reserved * MAX_EVENT_RECORD_BYTES + bytes > self.limits.max_bytes
        {
            if !self.drop_one()? {
                assert!(!terminal, "reserved terminal must fit");
                self.count_drop()?;
                return Ok(());
            }
        }
        self.sequence += 1;
        self.bytes += bytes;
        self.records.push_back(Record { event, bytes });
        Ok(())
    }
    pub(crate) fn drain(&mut self) -> EventBatch {
        self.bytes = 0;
        EventBatch {
            events: self.records.drain(..).map(|record| record.event).collect(),
            dropped: std::mem::take(&mut self.dropped),
        }
    }
    pub(crate) fn stats(&self) -> EventStats {
        EventStats {
            records: self.records.len(),
            bytes: self.bytes,
            reserved_terminals: self.reserved,
            dropped: self.dropped,
            sequence: self.sequence,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn event(kind: &str, payload: Value) -> Event {
        Event {
            operation_id: 3,
            queue_operation_id: None,
            sequence: 0,
            kind: kind.into(),
            payload,
        }
    }
    #[test]
    fn terminal_slots_survive_progress_flood_and_every_eviction_is_counted_once() {
        let mut queue = EventQueue::default();
        queue
            .set_limits(EventLimits {
                capacity: 4,
                max_bytes: 49152,
            })
            .unwrap();
        queue.reserve().unwrap();
        queue.reserve().unwrap();
        for index in 0..100 {
            queue
                .push(event("progress", json!({"progress":index})))
                .unwrap();
        }
        assert_eq!(queue.stats().records, 2);
        assert_eq!(queue.stats().dropped, 98);
        queue.push(event("completed", json!({}))).unwrap();
        queue.push(event("cancelled", json!({}))).unwrap();
        let batch = queue.drain();
        assert_eq!(batch.dropped, 98);
        assert_eq!(
            batch
                .events
                .iter()
                .filter(|event| event.is_terminal())
                .count(),
            2
        );
        assert!(
            batch
                .events
                .windows(2)
                .all(|pair| pair[0].sequence < pair[1].sequence)
        );
        assert_eq!(queue.drain().dropped, 0);
        assert_eq!(queue.stats().bytes, 0);
        assert_eq!(queue.stats().reserved_terminals, 0);
    }
    #[test]
    fn byte_budget_is_independent_of_count_and_limits_change_only_after_drain() {
        let limits = EventLimits {
            capacity: 256,
            max_bytes: 49152,
        };
        let mut queue = EventQueue::default();
        queue.set_limits(limits).unwrap();
        for _ in 0..20 {
            queue
                .push(event("log", json!({"data":"x".repeat(6000)})))
                .unwrap();
        }
        assert!(queue.stats().records < 10 && queue.stats().bytes <= limits.max_bytes);
        assert_eq!(queue.stats().dropped, 20 - queue.stats().records as u64);
        assert_eq!(
            queue.set_limits(EventLimits::default()).unwrap_err().code,
            "Busy"
        );
        queue.reserve().unwrap();
        assert!(queue.stats().bytes + MAX_EVENT_RECORD_BYTES <= limits.max_bytes);
        queue
            .push(event(
                "failed",
                json!({"code":"TransportError","message":"lost"}),
            ))
            .unwrap();
        let batch = queue.drain();
        assert_eq!(
            batch
                .events
                .iter()
                .filter(|event| event.is_terminal())
                .count(),
            1
        );
        queue.set_limits(EventLimits::default()).unwrap();
    }
    #[test]
    fn oversized_terminal_keeps_its_code_and_bounded_utf8_diagnostics() {
        let mut queue = EventQueue::default();
        queue.reserve().unwrap();
        queue.push(event("failed", json!({"code":"ValidationFailed","message":"\u{0}雪".repeat(100000),"issues":[{"instance_path":"/p".repeat(10000),"schema_path":"/schema".repeat(10000)}]}))).unwrap();
        let batch = queue.drain();
        assert_eq!(batch.events[0].payload["code"], "ValidationFailed");
        assert_eq!(batch.events[0].payload["truncated"], true);
        assert!(batch.events[0].budget_bytes() <= MAX_EVENT_RECORD_BYTES);
    }
    #[test]
    fn sequence_and_loss_counters_fail_explicitly_without_spending_terminal_reserves() {
        let mut queue = EventQueue {
            sequence: MAX_HOST_ID - 2,
            ..Default::default()
        };
        queue.reserve().unwrap();
        queue.reserve().unwrap();
        assert_eq!(
            queue.push(event("progress", json!({}))).unwrap_err().code,
            "LimitExceeded"
        );
        queue.push(event("completed", json!({}))).unwrap();
        queue
            .push(event(
                "failed",
                json!({"code":"LimitExceeded","message":"exhausted"}),
            ))
            .unwrap();
        assert_eq!(queue.stats().sequence, MAX_HOST_ID);
        assert_eq!(queue.can_reserve().unwrap_err().code, "LimitExceeded");
        let mut queue = EventQueue::default();
        queue
            .set_limits(EventLimits {
                capacity: 4,
                max_bytes: 49152,
            })
            .unwrap();
        queue.reserve().unwrap();
        queue.reserve().unwrap();
        queue.reserve().unwrap();
        queue.dropped = MAX_HOST_ID;
        assert_eq!(
            queue.push(event("progress", json!({}))).unwrap_err().code,
            "LimitExceeded"
        );
        queue.push(event("completed", json!({}))).unwrap();
        assert_eq!(queue.drain().dropped, MAX_HOST_ID);
    }
}
