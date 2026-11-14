use super::*;
use mcpx_core::{CredentialFuture, CredentialProvider, CredentialRequest};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

pub(super) type Provider = Rc<RefCell<Option<Box<dyn CredentialProvider>>>>;
struct ProviderLease {
    value: Option<Box<dyn CredentialProvider>>,
    source: Provider,
}
impl Drop for ProviderLease {
    fn drop(&mut self) {
        let mut source = self.source.borrow_mut();
        debug_assert!(source.is_none());
        *source = self.value.take();
    }
}
const WAKE_INTERVAL: u64 = 10_000_000;
#[derive(Default)]
struct Signal(AtomicBool);
impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }
}
pub(super) struct Resolving {
    future: CredentialFuture<'static>,
    pub(super) job: Job,
    signal: Arc<Signal>,
    next_poll: u64,
}
pub(super) struct Authentication {
    pub(super) exchange: Exchange,
    pub(super) job: Job,
    pub(super) deadline: u64,
}
impl WasiQueue {
    /// Replace the credential source while idle. Resolving is deferred until
    /// admission; the reference is owned configuration, never a tool argument.
    /// One nonblocking provider future is polled at a time. Cancellation drops
    /// that future and returns the provider to its slot for later requests.
    pub fn set_credential_provider(
        &mut self,
        provider: impl CredentialProvider + 'static,
        reference: Option<String>,
    ) -> Result<()> {
        self.open()?;
        if !self.pending.is_empty()
            || !self.active.is_empty()
            || self.metadata.is_some()
            || self.resolving.is_some()
            || self.authentication.is_some()
            || self.notification_resume.is_some()
        {
            return Err(Error::local(
                "Busy",
                "finish queued work before replacing its credential source",
            ));
        }
        if reference
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > 1024)
        {
            return Err(Error::local(
                "ConfigurationError",
                "credential reference must contain 1..1024 bytes",
            ));
        }
        self.provider = Some(Rc::new(RefCell::new(Some(Box::new(provider)))));
        self.credential_reference = reference;
        Ok(())
    }
    pub(super) fn resolve(&mut self, job: Job) {
        let source = self.provider.as_ref().expect("configured provider").clone();
        let value = source
            .borrow_mut()
            .take()
            .expect("exclusive credential resolution");
        let lease = ProviderLease {
            value: Some(value),
            source,
        };
        let endpoint = self.client.machine().endpoint().to_owned();
        let reference = self.credential_reference.clone();
        let future = Box::pin(async move {
            let mut provider = lease;
            provider
                .value
                .as_mut()
                .unwrap()
                .resolve(CredentialRequest {
                    endpoint: &endpoint,
                    reference: reference.as_deref(),
                })
                .await
        });
        self.resolving = Some(Resolving {
            future,
            job,
            signal: Arc::default(),
            next_poll: 0,
        });
    }
    pub(super) fn poll_credentials(&mut self) -> bool {
        let Some(mut resolving) = self.resolving.take() else {
            return false;
        };
        let now = monotonic_clock::now();
        let woken = resolving.signal.0.swap(false, Ordering::AcqRel);
        if now < resolving.next_poll && !woken {
            self.resolving = Some(resolving);
            return false;
        }
        let waker = Waker::from(resolving.signal.clone());
        resolving.next_poll = now.saturating_add(WAKE_INTERVAL);
        let result = resolving
            .future
            .as_mut()
            .poll(&mut Context::from_waker(&waker));
        match result {
            Poll::Pending => {
                let woken = resolving.signal.0.load(Ordering::Acquire);
                self.resolving = Some(resolving);
                woken
            }
            Poll::Ready(result) => {
                let Resolving {
                    mut job, future, ..
                } = resolving;
                drop(future);
                match result {
                    Ok(credential)
                        if credential.token().is_none_or(|token| token.len() <= 65536) =>
                    {
                        job.credential = Some(credential);
                        self.pending.push_front(job);
                    }
                    _ => self.settle(
                        job,
                        Err(Error::local(
                            "CredentialUnavailable",
                            "credential resolution failed or exceeded its budget",
                        )),
                    ),
                }
                true
            }
        }
    }
    pub(super) fn credentials_deadline(&self) -> Option<u64> {
        self.resolving
            .as_ref()
            .map(|resolution| resolution.next_poll.min(resolution.job.deadline))
    }
    pub(super) fn prune_credentials(&mut self) -> bool {
        let mut progress = false;
        if let Some(error) = self
            .resolving
            .as_ref()
            .and_then(|r| Self::interrupted(&r.job))
        {
            let Resolving { job, future, .. } = self.resolving.take().unwrap();
            drop(future);
            self.settle(job, Err(error));
            progress = true;
        }
        if let Some(error) = self
            .authentication
            .as_ref()
            .and_then(|a| Self::interrupted(&a.job))
        {
            self.fail_authentication(error);
            progress = true;
        }
        progress
    }
    pub(super) fn begin_authentication(&mut self, job: Job) {
        if let Some(error) = Self::interrupted(&job) {
            self.settle(job, Err(error));
            return;
        }
        let wanted = self.client.driver().http_notifications;
        let _ = self.stop_notifications();
        self.client.driver_mut().http_notifications = wanted;
        self.session_dirty = true;
        let session = self
            .client
            .driver()
            .session
            .as_ref()
            .expect("session to delete");
        let headers = BTreeMap::from([
            ("mcp-session-id".into(), session.id.clone()),
            ("mcp-protocol-version".into(), "2025-11-25".into()),
        ]);
        let deadline = job
            .deadline
            .min(monotonic_clock::now().saturating_add(500_000_000));
        let exchange = Exchange::new(
            Method::Delete,
            &session.url,
            &headers,
            Vec::new(),
            &session.credential,
            deadline,
            false,
        );
        match exchange {
            Ok(exchange) => {
                self.authentication = Some(Authentication {
                    exchange,
                    job,
                    deadline,
                })
            }
            Err(_) => {
                if !matches!(job.request, Request::Reconnect { .. }) {
                    let _ = self.client.host_runtime_mut().invalidate_authorization();
                }
                self.settle(job, Err(cleanup_error()));
            }
        }
    }
    pub(super) fn fail_authentication(&mut self, error: Error) {
        if let Some(authentication) = self.authentication.take() {
            // DELETE may already have executed. Preserve its original snapshot
            // for retry, while discarding authority over that uncertain session.
            if !matches!(authentication.job.request, Request::Reconnect { .. }) {
                let _ = self.client.host_runtime_mut().invalidate_authorization();
            }
            self.settle(authentication.job, Err(error));
        }
    }
    pub(super) fn poll_authentication(&mut self) -> bool {
        let Some(authentication) = &mut self.authentication else {
            return false;
        };
        if monotonic_clock::now() >= authentication.deadline {
            let error = if monotonic_clock::now() >= authentication.job.deadline {
                Error::local("Timeout", "credential transition deadline exceeded")
            } else {
                cleanup_error()
            };
            self.fail_authentication(error);
            return true;
        }
        match authentication.exchange.step() {
            Ok(Step::Pending) => false,
            Ok(Step::Progress) => true,
            Ok(Step::Response(response))
                if response.status() == 404
                    || response.status() == 405
                    || (200..300).contains(&response.status()) =>
            {
                let authentication = self.authentication.take().unwrap();
                if !matches!(authentication.job.request, Request::Reconnect { .. }) {
                    let _ = self.client.host_runtime_mut().invalidate_authorization();
                }
                self.client.driver_mut().session = None;
                self.session_dirty = false;
                self.pending.push_front(authentication.job);
                true
            }
            _ => {
                self.fail_authentication(cleanup_error());
                true
            }
        }
    }
    pub(super) fn abort_credentials(&mut self, code: &str) {
        if let Some(Resolving { job, future, .. }) = self.resolving.take() {
            drop(future);
            self.settle(
                job,
                Err(Error::local(code, "credential resolution interrupted")),
            );
        }
        self.fail_authentication(Error::local(code, "credential transition interrupted"));
    }
    /// Drive a local, potentially non-Send Future together with this owner's I/O.
    /// External runtime-specific futures still need their own runtime services.
    /// This runner creates no task pool and never drives the synchronous Client
    /// with run_blocking; all queue I/O remains in its pollable state machine.
    pub fn run<T>(&mut self, future: impl Future<Output = Result<T>>) -> Result<T> {
        self.open()?;
        run_local(future, |wait| {
            self.poll(wait)?;
            Ok(self.stats())
        })
    }
}
pub(super) fn run_local<T>(
    future: impl Future<Output = Result<T>>,
    mut drive: impl FnMut(Duration) -> Result<WasiQueueStats>,
) -> Result<T> {
    let mut future = std::pin::pin!(future);
    let signal = Arc::<Signal>::default();
    let waker = Waker::from(signal.clone());
    loop {
        signal.0.store(false, Ordering::Release);
        if let Poll::Ready(result) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
            return result;
        }
        let next = monotonic_clock::now().saturating_add(WAKE_INTERVAL);
        let stats = drive(if signal.0.load(Ordering::Acquire) {
            Duration::ZERO
        } else {
            Duration::from_millis(10)
        })?;
        if stats.active == 0
            && stats.queued == 0
            && !stats.metadata
            && !stats.resolving
            && !stats.authenticating
            && !signal.0.load(Ordering::Acquire)
            && monotonic_clock::now() < next
        {
            monotonic_clock::subscribe_instant(next).block();
        }
    }
}
fn cleanup_error() -> Error {
    Error::local(
        "SessionCleanupFailed",
        "could not close the previous MCP session",
    )
}
