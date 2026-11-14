use crate::{Error, MAX_HOST_ID, Result};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

/// Maximum simultaneously retained subscriptions per catalog owner.
pub const MAX_CATALOG_WATCHERS: usize = 64;
#[derive(Default)]
struct State {
    publication: Arc<()>,
    context: Arc<()>,
    subscribers: BTreeMap<u64, Option<Waker>>,
    next_id: u64,
    closed: bool,
}
#[derive(Default)]
struct Signals(Mutex<State>);
/// Read-only markers, not a reference to the catalog, schemas, credentials or client.
#[derive(Clone)]
pub struct CatalogRevisionSource(Arc<Signals>);
/// Owner-bound, in-memory freshness token. It cannot be reconstructed from a number.
#[derive(Clone)]
pub struct CatalogRevision {
    source: CatalogRevisionSource,
    publication: Arc<()>,
    context: Arc<()>,
}
impl std::fmt::Debug for CatalogRevision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogRevision").finish_non_exhaustive()
    }
}
impl PartialEq for CatalogRevision {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.source.0, &other.source.0)
            && Arc::ptr_eq(&self.publication, &other.publication)
            && Arc::ptr_eq(&self.context, &other.context)
    }
}
impl Eq for CatalogRevision {}
#[derive(Debug, Clone)]
pub struct CatalogChange {
    pub revision: CatalogRevision,
    /// A complete catalog was published, possibly with unchanged descriptors.
    pub published: bool,
    /// Authorization, schema resources, transport epoch or a remote notification changed.
    pub invalidated: bool,
}
/// Latest-state notifications; intermediate changes intentionally coalesce.
/// This is not a freshness check or a wire protocol subscription: TTL expiry,
/// refresh attempts and failures do not produce notifications. Final client close
/// or catalog drop ends the stream, after any unseen latest change is delivered.
pub struct CatalogWatcher {
    source: CatalogRevisionSource,
    id: u64,
    seen: CatalogRevision,
}
#[must_use = "futures must be polled to observe catalog changes"]
pub struct CatalogChanged<'a> {
    watcher: &'a mut CatalogWatcher,
    done: bool,
}

impl CatalogRevisionSource {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Signals::default()))
    }
    fn snapshot_with(&self, state: &State) -> CatalogRevision {
        CatalogRevision {
            source: self.clone(),
            publication: state.publication.clone(),
            context: state.context.clone(),
        }
    }
    pub fn snapshot(&self) -> CatalogRevision {
        let state = self.0.0.lock().unwrap();
        self.snapshot_with(&state)
    }
    /// Starts at the current revision; does not emit an initial snapshot.
    pub fn subscribe(&self) -> Result<CatalogWatcher> {
        let mut state = self.0.0.lock().unwrap();
        if state.closed {
            return Err(Error::local(
                "TransportClosed",
                "catalog subscription source is closed",
            ));
        }
        if state.subscribers.len() >= MAX_CATALOG_WATCHERS {
            return Err(Error::local(
                "LimitExceeded",
                "catalog subscriber limit reached",
            ));
        }
        let id = state
            .next_id
            .checked_add(1)
            .filter(|id| *id <= MAX_HOST_ID)
            .ok_or_else(|| Error::local("LimitExceeded", "catalog subscriber IDs exhausted"))?;
        state.next_id = id;
        state.subscribers.insert(id, None);
        Ok(CatalogWatcher {
            source: self.clone(),
            id,
            seen: self.snapshot_with(&state),
        })
    }
    pub(crate) fn check_owner(&self, revision: &CatalogRevision) -> Result<()> {
        if !Arc::ptr_eq(&self.0, &revision.source.0) {
            Err(Error::local(
                "CatalogMismatch",
                "catalog revision belongs to another owner",
            ))
        } else {
            Ok(())
        }
    }
    pub(crate) fn context_matches(&self, revision: &CatalogRevision) -> bool {
        Arc::ptr_eq(&self.snapshot().context, &revision.context)
    }
    /// Whether this owner published a complete snapshot in the same context
    /// since `revision`. This checks identity markers, not current freshness;
    /// hosts must also check their catalog policy before reusing that snapshot.
    pub fn published_since(&self, revision: &CatalogRevision) -> Result<bool> {
        self.check_owner(revision)?;
        let current = self.snapshot();
        Ok(Arc::ptr_eq(&current.context, &revision.context)
            && !Arc::ptr_eq(&current.publication, &revision.publication))
    }
    fn change(&self, published: bool, closed: bool) {
        let wakers = {
            let mut state = self.0.0.lock().unwrap();
            if state.closed {
                return;
            }
            if closed {
                state.closed = true;
            } else if published {
                state.publication = Arc::new(());
            } else {
                state.context = Arc::new(());
            }
            let wakers: Vec<_> = state
                .subscribers
                .values_mut()
                .filter_map(Option::take)
                .collect();
            if closed {
                state.subscribers.clear();
            }
            wakers
        };
        // Never execute an executor's wake implementation under our mutex.
        for waker in wakers {
            waker.wake();
        }
    }
    pub(crate) fn publish(&self) {
        self.change(true, false);
    }
    pub(crate) fn invalidate(&self) {
        self.change(false, false);
    }
    pub(crate) fn close(&self) {
        self.change(false, true);
    }
}
impl CatalogWatcher {
    /// Poll without retaining a waker. Ready(None) means the source is closed.
    pub fn try_changed(&mut self) -> Poll<Option<CatalogChange>> {
        let result = self.poll_changed(&mut Context::from_waker(Waker::noop()));
        self.clear_waker();
        result
    }

    /// Wait for the latest unseen revision, or None after final close.
    /// Dropping this future unregisters its waker without consuming a change.
    pub fn changed(&mut self) -> CatalogChanged<'_> {
        CatalogChanged {
            watcher: self,
            done: false,
        }
    }
    pub fn last_seen(&self) -> CatalogRevision {
        self.seen.clone()
    }
    fn clear_waker(&self) {
        let old = self
            .source
            .0
            .0
            .lock()
            .unwrap()
            .subscribers
            .get_mut(&self.id)
            .and_then(Option::take);
        drop(old);
    }
    fn poll_changed(&mut self, cx: &mut Context<'_>) -> Poll<Option<CatalogChange>> {
        let waker = cx.waker().clone();
        let mut state = self.source.0.0.lock().unwrap();
        let revision = self.source.snapshot_with(&state);
        if revision != self.seen {
            let change = CatalogChange {
                published: !Arc::ptr_eq(&revision.publication, &self.seen.publication),
                invalidated: !Arc::ptr_eq(&revision.context, &self.seen.context),
                revision: revision.clone(),
            };
            self.seen = revision;
            return Poll::Ready(Some(change));
        }
        if state.closed {
            return Poll::Ready(None);
        }
        let previous = state
            .subscribers
            .get_mut(&self.id)
            .expect("live subscriber")
            .replace(waker);
        drop(state);
        drop(previous);
        Poll::Pending
    }
}

impl Future for CatalogChanged<'_> {
    type Output = Option<CatalogChange>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let value = self.get_mut();
        assert!(!value.done, "catalog change future already completed");
        let result = value.watcher.poll_changed(cx);
        if result.is_ready() {
            value.done = true;
        }
        result
    }
}
impl Drop for CatalogChanged<'_> {
    fn drop(&mut self) {
        self.watcher.clear_waker();
    }
}
impl Drop for CatalogWatcher {
    fn drop(&mut self) {
        let old = self.source.0.0.lock().unwrap().subscribers.remove(&self.id);
        drop(old);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscriber_ids_are_checked_and_not_reused_after_unsubscribe() {
        let source = CatalogRevisionSource::new();
        source.0.0.lock().unwrap().next_id = MAX_HOST_ID - 1;
        let watcher = source.subscribe().unwrap();
        assert_eq!(watcher.id, MAX_HOST_ID);
        drop(watcher);
        assert_eq!(source.subscribe().err().unwrap().code, "LimitExceeded");
        source.close();
        assert_eq!(source.subscribe().err().unwrap().code, "TransportClosed");
    }
}
