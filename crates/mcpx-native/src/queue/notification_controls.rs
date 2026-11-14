use super::*;
use mcpx_core::HostRuntime;

/// Two reserved records, independent of ordinary queue slots and byte credits.
/// A stop takes effect only when executed, so cancelling a queued stop is a no-op.
pub(super) struct Controls {
    receiver: mpsc::UnboundedReceiver<Job>,
    ready: VecDeque<Job>,
    last_stop: u64,
    retry_at: Option<Instant>,
    pub(super) enabled: bool,
}
impl Controls {
    pub(super) fn new(receiver: mpsc::UnboundedReceiver<Job>, enabled: bool) -> Self {
        Self {
            receiver,
            ready: VecDeque::new(),
            last_stop: 0,
            retry_at: None,
            enabled,
        }
    }
    pub(super) fn open(&self) -> bool {
        !self.receiver.is_closed()
    }
    pub(super) async fn receive(&mut self) {
        if let Some(job) = self.receiver.recv().await {
            self.ready.push_back(job);
        }
    }
    pub(super) fn superseded(&self, job: &Job) -> bool {
        matches!(
            job.request,
            Request::NotificationStart | Request::NotificationResume(_)
        ) && job.id < self.last_stop
    }
    pub(super) fn superseded_error() -> Error {
        Error::local("Cancelled", "notification start/resume superseded by stop")
    }
    pub(super) fn prune(&mut self) {
        super::prune(&mut self.ready);
        let mut ready = std::mem::take(&mut self.ready);
        self.prune_starts(&mut ready);
        self.ready = ready;
        if !self.resume_pending() {
            self.retry_at = None;
        }
    }
    pub(super) fn prune_starts(&self, pending: &mut VecDeque<Job>) {
        let mut keep = VecDeque::new();
        while let Some(job) = pending.pop_front() {
            if self.superseded(&job) {
                super::parallel::finish(job, Err(Self::superseded_error()));
            } else {
                keep.push_back(job);
            }
        }
        *pending = keep;
    }
    pub(super) fn deadline(&self) -> Option<Instant> {
        self.ready.iter().map(|job| job.deadline).min()
    }
    pub(super) fn wake_deadline(&self) -> Option<Instant> {
        self.deadline().into_iter().chain(self.retry_at).min()
    }
    pub(super) fn resume_pending(&self) -> bool {
        self.ready
            .iter()
            .any(|job| matches!(job.request, Request::NotificationResume(_)))
    }
    pub(super) async fn drain(
        &mut self,
        runtime: &mut HostRuntime,
        notifications: &mut super::notifications::Notifications,
    ) {
        while let Ok(job) = self.receiver.try_recv() {
            self.ready.push_back(job);
        }
        self.retry_at = None;
        if let Err(error) = notifications.poll(runtime).await {
            for job in self.ready.drain(..) {
                super::parallel::finish(job, Err(error.clone()));
            }
            return;
        }
        self.prune();
        for _ in 0..self.ready.len() {
            self.prune();
            let Some(job) = self.ready.pop_front() else {
                break;
            };
            if let Request::NotificationResume(id) = job.request {
                let delay = match notifications.resume_delay(runtime, id) {
                    Ok(delay) => delay,
                    Err(error) if error.code == "Busy" => Duration::from_millis(5),
                    Err(error) => {
                        super::parallel::finish(job, Err(error));
                        continue;
                    }
                };
                if !delay.is_zero() {
                    let next = Instant::now()
                        .checked_add(delay)
                        .unwrap_or(job.deadline)
                        .min(job.deadline);
                    self.retry_at = Some(self.retry_at.map_or(next, |old| old.min(next)));
                    self.ready.push_back(job);
                    continue;
                }
            }
            let result = match job
                .operation_event
                .as_ref()
                .map(|ticket| ticket.start())
                .transpose()
            {
                Err(error) => Err(error),
                Ok(_) => match job.request {
                    Request::NotificationStop => {
                        self.last_stop = self.last_stop.max(job.id);
                        self.enabled = false;
                        notifications
                            .disable(runtime)
                            .await
                            .map(|()| Reply::NotificationStatus(runtime.notification_status()))
                    }
                    Request::NotificationStatus => {
                        Ok(Reply::NotificationStatus(runtime.notification_status()))
                    }
                    Request::NotificationResume(id) => notifications
                        .resume_now(runtime, id, None)
                        .map(|status| Reply::NotificationStatus(Some(status))),
                    _ => unreachable!(),
                },
            };
            let cancelled = job.cancellation.is_cancelled() || job.reply.is_closed();
            let expired = Instant::now() >= job.deadline;
            if matches!(job.request, Request::NotificationResume(_))
                && result.is_ok()
                && (cancelled || expired)
            {
                // The GET task is only scheduled, not owned by the caller's
                // future. Join it before returning a late control cancellation.
                let _ = notifications.disable(runtime).await;
            }
            let result = if cancelled {
                Err(Error::local(
                    "Cancelled",
                    "notification control cancelled; cleanup completed",
                ))
            } else if expired {
                Err(Error::local(
                    "Timeout",
                    "notification control expired; cleanup completed",
                ))
            } else {
                result
            };
            super::parallel::finish(job, result);
        }
        self.prune();
    }
    pub(super) fn close(&mut self) {
        self.receiver.close();
        while let Ok(job) = self.receiver.try_recv() {
            self.ready.push_back(job);
        }
        for job in self.ready.drain(..) {
            super::parallel::finish(job, Err(super::closed_error()));
        }
    }
}
