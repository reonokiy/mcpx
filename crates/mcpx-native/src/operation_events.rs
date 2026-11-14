use mcpx_core::{CallOutcome, Error, EventLimits, OperationEventBatch, OperationEvents, Result};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

struct State {
    journal: OperationEvents,
    closed: bool,
}
struct Shared {
    state: Mutex<State>,
    notify: Notify,
}
#[derive(Clone)]
pub(crate) struct OperationProducer(Arc<Shared>);
/// Independent, bounded lifecycle subscription for a QueuedRemote.
/// Keep draining it, or new submissions return ConsumerLagged before admission.
pub struct OperationEventStream(Arc<Shared>);
pub(crate) struct OperationTicket {
    started: std::cell::Cell<bool>,
    producer: OperationProducer,
    id: u64,
    finished: bool,
}
pub(crate) struct OperationOwner(OperationProducer);
impl Drop for OperationOwner {
    fn drop(&mut self) {
        self.0.close();
    }
}
pub(crate) fn channel(limits: EventLimits) -> Result<(OperationProducer, OperationEventStream)> {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            journal: OperationEvents::new(limits)?,
            closed: false,
        }),
        notify: Notify::new(),
    });
    Ok((
        OperationProducer(shared.clone()),
        OperationEventStream(shared),
    ))
}
impl OperationProducer {
    pub(crate) fn owner(&self) -> OperationOwner {
        OperationOwner(self.clone())
    }
    pub(crate) fn admit(&self) -> Result<OperationTicket> {
        let mut state = self.0.state.lock().unwrap();
        if state.closed {
            return Err(Error::local(
                "TransportClosed",
                "queue event owner is closed",
            ));
        }
        let id = state.journal.admit()?;
        drop(state);
        self.0.notify.notify_one();
        Ok(OperationTicket {
            started: std::cell::Cell::new(false),
            producer: self.clone(),
            id,
            finished: false,
        })
    }
    fn close(&self) {
        self.0.state.lock().unwrap().closed = true;
        self.0.notify.notify_one();
    }
}
impl OperationTicket {
    pub(crate) fn id(&self) -> u64 {
        self.id
    }
    pub(crate) fn start(&self) -> Result<()> {
        if self.started.get() {
            return Ok(());
        }
        let result = self.producer.0.state.lock().unwrap().journal.start(self.id);
        if result.is_ok() {
            self.started.set(true);
        }
        self.producer.0.notify.notify_one();
        result
    }
    pub(crate) fn finish(mut self, outcome: Option<&CallOutcome>, error: Option<&Error>) {
        let mut state = self.producer.0.state.lock().unwrap();
        match error {
            Some(error) => state.journal.fail(self.id, error),
            None => state.journal.complete(self.id, outcome),
        }
        .expect("admitted queue operation reserved its terminal");
        self.finished = true;
        drop(state);
        self.producer.0.notify.notify_one();
    }
}
impl Drop for OperationTicket {
    fn drop(&mut self) {
        if !self.finished {
            self.producer
                .0
                .state
                .lock()
                .unwrap()
                .journal
                .abandoned(self.id)
                .expect("reserved abandoned queue terminal");
            self.producer.0.notify.notify_one();
        }
    }
}
impl OperationEventStream {
    pub fn try_next_batch(&mut self) -> Option<OperationEventBatch> {
        let mut state = self.0.state.lock().unwrap();
        let stats = state.journal.stats();
        if stats.records == 0 && stats.dropped == 0 {
            None
        } else {
            Some(state.journal.drain())
        }
    }
    pub async fn next_batch(&mut self) -> Option<OperationEventBatch> {
        loop {
            let notified = self.0.notify.notified();
            {
                let mut state = self.0.state.lock().unwrap();
                let stats = state.journal.stats();
                if stats.records != 0 || stats.dropped != 0 {
                    return Some(state.journal.drain());
                }
                if state.closed {
                    return None;
                }
            }
            notified.await;
        }
    }
}
impl Drop for OperationEventStream {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().journal.disable();
    }
}
