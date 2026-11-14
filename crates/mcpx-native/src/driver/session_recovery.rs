use super::*;
use crate::{HttpSessionRecoveryPolicy, session_recovery::HttpRecoveryState};

impl NativeDriver {
    pub(super) async fn renew_session(
        &mut self,
        runtime: &mut HostRuntime,
        observed: u64,
        prepared: bool,
    ) -> Result<DriverContext> {
        // Retained results from the old epoch are not new health evidence.
        self.last_tool_result = runtime.machine().last_tool_result();
        let policy = self.http_session_recovery.ok_or_else(|| {
            Error::local("CapabilityUnavailable", "HTTP session recovery is disabled")
        })?;
        let caller_deadline = if prepared {
            self.deadline
        } else {
            tokio::time::Instant::now()
                .checked_add(self.timeout)
                .ok_or_else(|| {
                    Error::local("ConfigurationError", "operation exceeds clock range")
                })?
        };
        let token = self.cancellation.clone();
        if self
            .session_recovery_state
            .as_ref()
            .is_some_and(|state| state.healthy_since.elapsed() >= policy.timeout)
        {
            self.session_recovery_state = None;
        }
        if self.session_recovery_state.is_none() {
            self.session_recovery_state = Some(HttpRecoveryState {
                deadline: policy.deadline(self.origin, observed)?,
                remaining: policy.max_attempts,
                healthy_since: tokio::time::Instant::now(),
            });
        }
        let deadline = self
            .session_recovery_state
            .as_ref()
            .unwrap()
            .deadline
            .min(caller_deadline);
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::local(
                    "Timeout",
                    "HTTP session recovery budget exhausted",
                ));
            }
            let state = self.session_recovery_state.as_mut().unwrap();
            if state.remaining == 0 {
                return Err(Error::local(
                    "ReconnectExhausted",
                    "HTTP session recovery rounds exhausted",
                ));
            }
            let delayed = state.remaining < policy.max_attempts;
            state.remaining -= 1;
            let result = {
                let round = async {
                    if delayed {
                        tokio::time::sleep(policy.backoff).await;
                    }
                    self.reconnect(runtime).await?;
                    self.recovery_deadline = Some(deadline);
                    let context = self.prepare().await?;
                    runtime.advance_time(self.now_ms())?;
                    runtime.accept_authorization(&context.auth_context_id)?;
                    runtime.refresh_catalog()?;
                    mcpx_core::drive_host(self, runtime).await?;
                    Ok(context)
                };
                tokio::pin!(round);
                tokio::select! { biased;
                    _=token.cancelled()=>Err(Error::local("Cancelled", "session recovery cancelled")),
                    _=tokio::time::sleep_until(deadline)=>Err(Error::local("Timeout", "HTTP session recovery budget exhausted")),
                    result=&mut round=>result,
                }
            };
            if token.is_cancelled() {
                return Err(Error::local("Cancelled", "session recovery cancelled"));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::local(
                    "Timeout",
                    "HTTP session recovery budget exhausted",
                ));
            }
            match result {
                Ok(context) => {
                    self.session_recovery_state.as_mut().unwrap().healthy_since =
                        tokio::time::Instant::now();
                    // Metadata spends the same caller deadline. The ensuing
                    // first tool dispatch keeps its remaining operation time.
                    self.deadline = caller_deadline;
                    self.recovery_deadline = Some(caller_deadline);
                    return Ok(context);
                }
                Err(error)
                    if self.session_recovery_state.as_ref().unwrap().remaining > 0
                        && HttpSessionRecoveryPolicy::retryable(&error) =>
                {
                    runtime.abort_error(error.clone());
                    self.abort(runtime, &error).await;
                    self.finish();
                    self.cancellation = token.clone();
                    runtime.reset_transport()?;
                }
                Err(error) => return Err(error),
            }
        }
    }
}
