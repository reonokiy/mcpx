use mcpx_core::{Catalog, MAX_CATALOG_WATCHERS, SchemaRegistry};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

#[derive(Default)]
struct Count(AtomicUsize);
impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn poll<T>(future: Pin<&mut impl Future<Output = T>>, waker: &Waker) -> Poll<T> {
    future.poll(&mut Context::from_waker(waker))
}
fn change(watcher: &mut mcpx_core::CatalogWatcher) -> Option<mcpx_core::CatalogChange> {
    let mut future = std::pin::pin!(watcher.changed());
    match poll(future.as_mut(), Waker::noop()) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("expected a change or closure"),
    }
}

#[test]
fn synchronous_observation_survives_lost_journal_notifications_and_drains_on_close() {
    let mut machine =
        mcpx_core::Machine::new("https://example.com/mcp".into(), mcpx_core::Dialect::Legacy)
            .unwrap();
    machine
        .set_event_limits(mcpx_core::EventLimits {
            capacity: 4,
            max_bytes: 49152,
        })
        .unwrap();
    let mut watcher = machine.catalog().revision_source().subscribe().unwrap();
    assert!(watcher.try_changed().is_pending());
    machine
        .notify(&serde_json::json!({"jsonrpc":"2.0", "method":"notifications/tools/list_changed"}))
        .unwrap();
    for _ in 0..100 {
        machine.notify(&serde_json::json!({"jsonrpc":"2.0", "method":"notifications/message", "params":{}})).unwrap();
    }
    let batch = machine.drain_events();
    assert!(batch.dropped > 0);
    assert!(
        !batch
            .events
            .iter()
            .any(|event| event.kind == "catalog_stale")
    );
    drop(machine);
    let Poll::Ready(Some(change)) = watcher.try_changed() else {
        panic!("lost catalog invalidation")
    };
    assert!(change.invalidated && !change.published);
    assert!(matches!(watcher.try_changed(), Poll::Ready(None)));
}

#[test]
fn publications_and_invalidations_coalesce_for_independent_subscribers() {
    let mut catalog = Catalog::default();
    let source = catalog.revision_source();
    let original = source.snapshot();
    let mut first = source.subscribe().unwrap();
    let mut second = source.subscribe().unwrap();
    let count = Arc::new(Count::default());
    let waker = Waker::from(count.clone());
    {
        let mut waiting = std::pin::pin!(first.changed());
        assert!(poll(waiting.as_mut(), &waker).is_pending());
        catalog.replace(vec![]).unwrap();
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        let Poll::Ready(Some(published)) = poll(waiting.as_mut(), &waker) else {
            panic!()
        };
        assert!(published.published && !published.invalidated);
        assert_ne!(published.revision, original);
    }
    catalog.replace(vec![]).unwrap();
    catalog.set_schema_registry(SchemaRegistry::default());
    catalog.replace(vec![]).unwrap();
    let latest = change(&mut second).unwrap();
    assert!(latest.published && latest.invalidated);
    assert_eq!(latest.revision, source.snapshot());
    assert_eq!(second.last_seen(), latest.revision);
    assert!(poll(std::pin::pin!(second.changed()), Waker::noop()).is_pending());
}

#[test]
fn publication_reuse_requires_same_owner_and_uninvalidated_context() {
    let mut catalog = Catalog::default();
    let source = catalog.revision_source();
    let initial = source.snapshot();
    assert!(!source.published_since(&initial).unwrap());
    catalog.replace(vec![]).unwrap();
    assert!(source.published_since(&initial).unwrap());
    let published = source.snapshot();
    catalog.set_schema_registry(SchemaRegistry::default());
    catalog.replace(vec![]).unwrap();
    assert!(!source.published_since(&initial).unwrap());
    assert!(!source.published_since(&published).unwrap());
    assert_eq!(
        Catalog::default()
            .revision_source()
            .published_since(&initial)
            .unwrap_err()
            .code,
        "CatalogMismatch"
    );
}

#[test]
fn dropping_pending_wait_releases_executor_without_dropping_subscription() {
    let mut catalog = Catalog::default();
    let mut watcher = catalog.revision_source().subscribe().unwrap();
    let count = Arc::new(Count::default());
    let weak = Arc::downgrade(&count);
    let waker = Waker::from(count);
    {
        let mut waiting = std::pin::pin!(watcher.changed());
        assert!(poll(waiting.as_mut(), &waker).is_pending());
        drop(waker);
        assert!(weak.upgrade().is_some());
    }
    assert!(weak.upgrade().is_none());
    catalog.invalidate();
    let latest = change(&mut watcher).unwrap();
    assert!(!latest.published && latest.invalidated);
}

#[test]
fn subscriber_cap_releases_slots_and_catalog_drop_wakes_and_closes_readers() {
    let mut catalog = Catalog::default();
    let source = catalog.revision_source();
    let mut watchers: Vec<_> = (0..MAX_CATALOG_WATCHERS)
        .map(|_| source.subscribe().unwrap())
        .collect();
    assert_eq!(source.subscribe().err().unwrap().code, "LimitExceeded");
    watchers.pop();
    let mut waiting_reader = source.subscribe().unwrap();
    let count = Arc::new(Count::default());
    let waker = Waker::from(count.clone());
    {
        let mut waiting = std::pin::pin!(waiting_reader.changed());
        assert!(poll(waiting.as_mut(), &waker).is_pending());
        catalog.replace(vec![]).unwrap();
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            poll(waiting.as_mut(), &waker),
            Poll::Ready(Some(_))
        ));
    }
    {
        let mut closing = std::pin::pin!(waiting_reader.changed());
        assert!(poll(closing.as_mut(), &waker).is_pending());
        drop(catalog);
        assert_eq!(count.0.load(Ordering::SeqCst), 2);
        assert!(matches!(poll(closing.as_mut(), &waker), Poll::Ready(None)));
    }
    // A retained read-only source/token does not retain the Catalog owner.
    assert_eq!(source.subscribe().err().unwrap().code, "TransportClosed");
    for watcher in &mut watchers {
        assert!(change(watcher).is_some());
        assert!(change(watcher).is_none());
    }
}

#[test]
fn failed_publication_does_not_change_revision_or_wake_waiters() {
    let mut catalog = Catalog::default();
    let source = catalog.revision_source();
    let initial = source.snapshot();
    let mut watcher = source.subscribe().unwrap();
    let tool: mcpx_core::ToolDescriptor =
        serde_json::from_value(serde_json::json!({"name":"echo","inputSchema":{"type":"object"}}))
            .unwrap();
    assert!(catalog.replace(vec![tool.clone(), tool]).is_err());
    assert_eq!(source.snapshot(), initial);
    assert!(poll(std::pin::pin!(watcher.changed()), Waker::noop()).is_pending());
    drop(catalog);
    assert!(change(&mut watcher).is_none());
}
