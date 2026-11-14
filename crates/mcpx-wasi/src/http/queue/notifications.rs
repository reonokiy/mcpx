use super::*;

impl WasiQueue {
    /// Resume one exact legacy listener. One independently reserved record
    /// (128 accounting bytes) waits outside business admission/provider work.
    /// Stop/status remain synchronous and never compete for this permit.
    pub fn resume_notifications(
        &mut self,
        resource_id: u64,
    ) -> Result<WasiOperation<mcpx_core::NotificationStatus>> {
        self.open()?;
        if self.suspended {
            return Err(Self::suspended_error());
        }
        if resource_id == 0 || resource_id > mcpx_core::MAX_HOST_ID {
            return Err(Error::local(
                "InvalidHandle",
                "notification resource ID must be a positive exact host integer",
            ));
        }
        if self.notification_resume.is_some() {
            return Err(Error::local(
                "QueueFull",
                "a notification resume is already pending",
            ));
        }
        let id = self.events.admit()?;
        let created = monotonic_clock::now();
        let timeout = duration_ns(self.options.timeout)?.min(self.client.driver().timeout_ns);
        let state = Rc::new(RefCell::new(State::default()));
        self.notification_resume = Some(Job {
            id,
            request: Request::NotificationResume(resource_id),
            state: state.clone(),
            bytes: 128,
            created,
            deadline: created.saturating_add(timeout),
            started: false,
            refreshed: None,
            sent: false,
            credential: None,
            scoped: false,
        });
        self.client.driver_mut().hold_notification_resume(true);
        Ok(WasiOperation::new(
            self.owner.clone(),
            id,
            state,
            notification,
        ))
    }
    pub(super) fn cancel_notification_resume(&mut self, code: &str, message: &str) {
        if let Some(job) = self.notification_resume.take() {
            self.settle(job, Err(Error::local(code, message)));
        }
    }
    pub(super) fn service_notification_resume(&mut self) -> bool {
        let Some(mut job) = self.notification_resume.take() else {
            return false;
        };
        if let Some(error) = Self::interrupted(&job) {
            self.settle(job, Err(error));
            return true;
        }
        if !job.started {
            if let Err(error) = self.events.start(job.id) {
                self.settle(job, Err(error));
                return true;
            }
            job.started = true;
        }
        let Request::NotificationResume(id) = job.request else {
            unreachable!()
        };
        let (runtime, driver) = self.client.host_parts_mut();
        match driver.resume_notification_step(runtime, id) {
            Ok(Some(status)) => {
                self.settle(job, Ok(Reply::Notification(status)));
                true
            }
            Err(error) => {
                self.settle(job, Err(error));
                true
            }
            Ok(None) => {
                self.notification_resume_due =
                    driver.notification_resume_due(runtime, id).ok().flatten();
                self.notification_resume = Some(job);
                false
            }
        }
    }
}
