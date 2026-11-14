use super::*;
impl WasiQueue {
    /// A FIFO logical HTTP reconnect. Existing calls and their cleanup finish
    /// first. Only metadata is sent; failed tools and old handles are not replayed.
    pub fn reconnect(&mut self) -> Result<WasiOperation<()>> {
        self.enqueue(Request::Reconnect { reset: false }, 0, unit)
    }
    pub fn is_suspended(&self) -> bool {
        self.suspended
    }
    pub(super) fn suspended_error() -> Error {
        Error::local(
            "TransportClosed",
            "reconnect must complete before new operations",
        )
    }
    pub(super) fn permit(&self, request: &Request) -> Result<()> {
        if self.suspended
            && self.reconnect_id.is_none()
            && !matches!(request, Request::Reconnect { .. } | Request::Control(_))
        {
            Err(Self::suspended_error())
        } else {
            Ok(())
        }
    }
    pub(super) fn begin_reconnect(&mut self, mut job: Job) {
        if let Some(error) = Self::interrupted(&job) {
            self.settle(job, Err(error));
            return;
        }
        if !job.started {
            if let Err(error) = self.events.start(job.id) {
                self.settle(job, Err(error));
                return;
            }
            job.started = true;
        }
        let result = (|| -> Result<()> {
            if let Some(error) = Self::interrupted(&job) {
                return Err(error);
            }
            self.suspended = true;
            self.reconnect_id = Some(job.id);
            self.client.driver_mut().reconnecting = true;
            self.client.host_runtime_mut().reset_transport()?;
            let wanted = self.client.driver().http_notifications;
            self.stop_notifications()?;
            self.client.driver_mut().http_notifications = wanted;
            job.request = Request::Reconnect { reset: true };
            self.session_dirty = false;
            Ok(())
        })();
        if let Err(error) = result {
            self.settle(job, Err(error));
            return;
        }
        if self.client.driver().session.is_some() {
            self.begin_authentication(job);
        } else {
            self.pending.push_front(job);
        }
    }
}
