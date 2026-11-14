use mcpx_core::{Error, Event, Result};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

#[derive(Debug, Clone)]
pub enum EventStreamItem {
    Event(Event),
    Lagged { dropped: u64 },
}

struct State {
    events: VecDeque<(Event, usize)>,
    bytes: usize,
    max_bytes: usize,
    capacity: usize,
    dropped: u64,
    closed: bool,
    receiver_closed: bool,
}
struct Shared {
    state: Mutex<State>,
    notify: Notify,
}
pub struct EventStream {
    shared: Arc<Shared>,
}
pub(crate) struct EventProducer {
    shared: Arc<Shared>,
}

pub(crate) fn channel(limits: mcpx_core::EventLimits) -> Result<(EventProducer, EventStream)> {
    limits.validate()?;
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            events: VecDeque::new(),
            capacity: limits.capacity,
            bytes: 0,
            max_bytes: limits.max_bytes,
            dropped: 0,
            closed: false,
            receiver_closed: false,
        }),
        notify: Notify::new(),
    });
    Ok((
        EventProducer {
            shared: shared.clone(),
        },
        EventStream { shared },
    ))
}

impl EventProducer {
    pub fn reserve_operation(&self) -> Result<()> {
        self.reserve_operations(1)
    }
    pub fn reserve_operations(&self, count: usize) -> Result<()> {
        let mut state = self.shared.state.lock().unwrap();
        if state.receiver_closed {
            return Ok(());
        }
        // An operation can refresh its catalog and then complete one tool request.
        let slots = count.saturating_mul(2);
        if slots > state.capacity
            || slots.saturating_mul(mcpx_core::MAX_EVENT_RECORD_BYTES) > state.max_bytes
        {
            return Err(Error::new(
                "ConsumerLagged",
                "event capacity cannot reserve more active operations",
            ));
        }
        while state.events.len() > state.capacity - slots
            || state.bytes + slots * mcpx_core::MAX_EVENT_RECORD_BYTES > state.max_bytes
        {
            let Some(index) = state
                .events
                .iter()
                .position(|(event, _)| !event.is_terminal())
            else {
                return Err(Error::new(
                    "ConsumerLagged",
                    "drain terminal events before starting another operation",
                ));
            };
            let (_, bytes) = state.events.remove(index).unwrap();
            state.bytes -= bytes;
            state.dropped += 1;
        }
        Ok(())
    }
    pub fn push(&self, event: Event) {
        let mut state = self.shared.state.lock().unwrap();
        if state.receiver_closed {
            return;
        }
        let bytes = event.budget_bytes();
        while state.events.len() == state.capacity || state.bytes + bytes > state.max_bytes {
            if let Some(index) = state
                .events
                .iter()
                .position(|(event, _)| !event.is_terminal())
            {
                let (_, bytes) = state.events.remove(index).unwrap();
                state.bytes -= bytes;
                state.dropped += 1;
            } else {
                // prepare reserves both terminal slots; only lossy events can reach this branch.
                assert!(
                    !event.is_terminal(),
                    "driver exceeded reserved terminal slots"
                );
                state.dropped += 1;
                self.shared.notify.notify_one();
                return;
            }
        }
        state.bytes += bytes;
        state.events.push_back((event, bytes));
        self.shared.notify.notify_one();
    }
    pub fn report_dropped(&self, dropped: u64) {
        let mut state = self.shared.state.lock().unwrap();
        if !state.receiver_closed {
            state.dropped = state.dropped.saturating_add(dropped);
            self.shared.notify.notify_one();
        }
    }
    pub fn close(&self) {
        self.shared.state.lock().unwrap().closed = true;
        self.shared.notify.notify_one();
    }
}
impl Drop for EventProducer {
    fn drop(&mut self) {
        self.close();
    }
}

impl EventStream {
    pub fn try_next(&mut self) -> Option<EventStreamItem> {
        let mut state = self.shared.state.lock().unwrap();
        if state.dropped > 0 {
            return Some(EventStreamItem::Lagged {
                dropped: std::mem::take(&mut state.dropped),
            });
        }
        state.events.pop_front().map(|(event, bytes)| {
            state.bytes -= bytes;
            EventStreamItem::Event(event)
        })
    }
    pub async fn next(&mut self) -> Option<EventStreamItem> {
        loop {
            let notified = self.shared.notify.notified();
            {
                let mut state = self.shared.state.lock().unwrap();
                if state.dropped > 0 {
                    return Some(EventStreamItem::Lagged {
                        dropped: std::mem::take(&mut state.dropped),
                    });
                }
                if let Some((event, bytes)) = state.events.pop_front() {
                    state.bytes -= bytes;
                    return Some(EventStreamItem::Event(event));
                }
                if state.closed {
                    return None;
                }
            }
            notified.await;
        }
    }
}
impl Drop for EventStream {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().unwrap();
        state.receiver_closed = true;
        state.events.clear();
        state.bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn event(sequence: u64, kind: &str, bytes: usize) -> Event {
        Event {
            operation_id: 3,
            queue_operation_id: None,
            sequence,
            kind: kind.into(),
            payload: json!({"data":"x".repeat(bytes)}),
        }
    }
    #[test]
    fn core_and_native_drops_are_counted_without_inferred_sequence_gaps() {
        let (producer, mut stream) = channel(mcpx_core::EventLimits {
            capacity: 4,
            ..Default::default()
        })
        .unwrap();
        producer.report_dropped(10);
        producer.push(event(99, "progress", 1));
        producer.reserve_operation().unwrap();
        producer.push(event(100, "completed", 0));
        producer.push(event(120, "completed", 0));
        producer.reserve_operation().unwrap(); // Evicts only the retained progress.
        assert!(matches!(
            stream.try_next(),
            Some(EventStreamItem::Lagged { dropped: 11 })
        ));
        assert!(
            matches!(stream.try_next(), Some(EventStreamItem::Event(event)) if event.sequence == 100)
        );
        assert!(
            matches!(stream.try_next(), Some(EventStreamItem::Event(event)) if event.sequence == 120)
        );
        assert!(stream.try_next().is_none());
    }
    #[test]
    fn native_stream_enforces_encoded_byte_budget_as_well_as_capacity() {
        let (producer, mut stream) = channel(mcpx_core::EventLimits {
            capacity: 256,
            max_bytes: 49152,
        })
        .unwrap();
        for sequence in 1..=20 {
            producer.push(event(sequence, "log", 6000));
        }
        {
            let state = producer.shared.state.lock().unwrap();
            assert!(state.bytes <= 49152 && state.events.len() < 10);
        }
        producer.reserve_operation().unwrap();
        producer.push(event(21, "completed", 0));
        let mut completed = 0;
        while let Some(item) = stream.try_next() {
            if let EventStreamItem::Event(event) = item {
                completed += usize::from(event.is_terminal());
            }
        }
        assert_eq!(completed, 1);
        assert_eq!(producer.shared.state.lock().unwrap().bytes, 0);
    }
}
