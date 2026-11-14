use super::*;
use mcpx_core::NotificationResume;

impl WasiDriver {
    /// Configure bounded legacy notification GET recovery. Defaults to None;
    /// this policy alone does not enable listening or create a background task.
    pub fn with_notification_recovery(
        mut self,
        policy: Option<crate::NotificationRecoveryPolicy>,
    ) -> Result<Self> {
        if let Some(policy) = policy {
            policy.validate()?;
        }
        self.recovery.policy = policy;
        Ok(self)
    }
    pub(in crate::http) fn hold_notification_resume(&mut self, hold: bool) {
        self.recovery.manual = hold;
        self.recovery.next = None;
    }
    fn resume_info(
        &mut self,
        runtime: &mut HostRuntime,
        id: u64,
    ) -> Result<Option<NotificationResume>> {
        runtime.advance_time(self.now_ms())?;
        let info = match runtime.notification_resume(id) {
            Ok(info) => Some(info),
            Err(error) if error.code == "Busy" => None,
            Err(error) => return Err(error),
        };
        let source = self
            .recovery
            .source
            .as_ref()
            .filter(|s| s.id == id)
            .ok_or_else(|| {
                Error::local("ResumeUnavailable", "no frozen notification credentials")
            })?;
        if runtime
            .catalog_status(source.credential.auth_context_id())?
            .authorization_changed
        {
            return Err(Error::local(
                "ResumeUnavailable",
                "notification authorization changed",
            ));
        }
        Ok(info)
    }
    pub(in crate::http) fn notification_resume_due(
        &mut self,
        runtime: &mut HostRuntime,
        id: u64,
    ) -> Result<Option<u64>> {
        let info = self.resume_info(runtime, id)?;
        Ok(if self.notifications.is_some() {
            None
        } else {
            info.map(|info| {
                monotonic_clock::now().saturating_add(info.retry_after_ms.saturating_mul(1_000_000))
            })
        })
    }
    pub(in crate::http) fn resume_notification_step(
        &mut self,
        runtime: &mut HostRuntime,
        id: u64,
    ) -> Result<Option<NotificationStatus>> {
        if self.closed || self.reconnecting {
            return Err(Error::local(
                "TransportClosed",
                "WASI session is unavailable",
            ));
        }
        let info = self.resume_info(runtime, id)?;
        if self.notifications.is_some() || info.is_none_or(|info| info.retry_after_ms > 0) {
            return Ok(None);
        }
        let source = self.recovery.source.as_ref().expect("checked source");
        let (credential, root) = (source.credential.clone(), source.root);
        let plan = runtime.resume_notifications(id)?;
        self.recovery.budget = None;
        self.open_notifications(runtime, plan, credential, root, self.timeout_ns)?;
        Ok(runtime.notification_status())
    }
    pub fn resume_notifications(
        &mut self,
        runtime: &mut HostRuntime,
        id: u64,
    ) -> Result<NotificationStatus> {
        self.resume_notifications_with_timeout(runtime, id, Duration::from_nanos(self.timeout_ns))
    }
    pub fn resume_notifications_with_timeout(
        &mut self,
        runtime: &mut HostRuntime,
        id: u64,
        wait: Duration,
    ) -> Result<NotificationStatus> {
        if self.closed || self.reconnecting {
            return Err(Error::local("TransportClosed", "WASI driver is closed"));
        }
        let ns = u64::try_from(wait.as_nanos())
            .ok()
            .filter(|ns| (1_000_000..=86_400_000_000_000).contains(ns))
            .ok_or_else(|| {
                Error::local(
                    "ConfigurationError",
                    "resume timeout must be 1 ms..24 hours",
                )
            })?;
        let deadline = monotonic_clock::now().saturating_add(ns);
        self.hold_notification_resume(true);
        let result = (|| {
            loop {
                if monotonic_clock::now() >= deadline {
                    return Err(Error::local(
                        "Timeout",
                        "notification recovery waiting deadline exceeded",
                    ));
                }
                self.service_notifications(runtime);
                self.deliver_events(runtime);
                if let Some(status) = self.resume_notification_step(runtime, id)? {
                    return Ok(status);
                }
                let mut ready = self.notification_pollables();
                ready.push(monotonic_clock::subscribe_instant(deadline));
                if let Some(due) = self.notification_resume_due(runtime, id)? {
                    ready.push(monotonic_clock::subscribe_instant(due));
                }
                poll(&ready.iter().collect::<Vec<_>>());
            }
        })();
        self.hold_notification_resume(false);
        result
    }
    pub(super) fn recover_notifications(&mut self, runtime: &mut HostRuntime) -> bool {
        self.recovery.next = None;
        let Some(policy) = self.recovery.policy else {
            return false;
        };
        if self.closed
            || !self.http_notifications
            || self.recovery.manual
            || self.notifications.is_some()
        {
            return false;
        }
        let Some(id) = self.recovery.source.as_ref().map(|source| source.id) else {
            return false;
        };
        let info = match self.resume_info(runtime, id) {
            Ok(Some(info)) => info,
            _ => {
                self.recovery.source = None;
                return false;
            }
        };
        let now = monotonic_clock::now();
        let source = self.recovery.source.as_ref().unwrap();
        let window = policy.timeout.as_nanos() as u64;
        let failed = source.ended.unwrap_or(now);
        let new_failure = self
            .recovery
            .budget
            .as_ref()
            .is_none_or(|b| b.failed_id != id);
        let healthy = new_failure
            && source
                .opened
                .is_some_and(|opened| failed.saturating_sub(opened) >= window);
        if healthy
            || self
                .recovery
                .budget
                .as_ref()
                .is_none_or(|budget| budget.root != source.root)
        {
            self.recovery.budget = Some(Budget {
                root: source.root,
                failed_id: id,
                deadline: failed.saturating_add(window),
                next: failed.saturating_add(policy.backoff.as_nanos() as u64),
                remaining: policy.max_attempts,
            });
        }
        let budget = self.recovery.budget.as_mut().unwrap();
        if budget.failed_id != id {
            budget.failed_id = id;
            budget.next = failed.saturating_add(policy.backoff.as_nanos() as u64);
        }
        let due = now
            .saturating_add(info.retry_after_ms.saturating_mul(1_000_000))
            .max(budget.next);
        if budget.remaining == 0 || due >= budget.deadline {
            return false;
        }
        if due > now {
            self.recovery.next = Some(due);
            return false;
        }
        budget.remaining -= 1;
        let timeout = self.timeout_ns.min(budget.deadline - now);
        let (credential, root) = (source.credential.clone(), source.root);
        let plan = match runtime.resume_notifications(id) {
            Ok(plan) => plan,
            Err(_) => {
                self.recovery.budget.as_mut().unwrap().remaining = 0;
                return false;
            }
        };
        let _ = self.open_notifications(runtime, plan, credential, root, timeout);
        true
    }
}
