use mcpx_core::{Error, Result};
use std::time::Duration;

/// Opt-in logical session renewal after an observed legacy MCP session 404.
/// Applies during initial HTTP discovery, before a mutable Remote's next
/// operation, and in established queued owners. Never replays tools/call.
#[derive(Clone, Copy, Debug)]
pub struct HttpSessionRecoveryPolicy {
    /// Full reconnect/discovery rounds, 1..=8. Consecutive short lived sessions
    /// share the budget until a real tool result or a healthy timeout interval.
    pub max_attempts: u8,
    /// Delay after the first round, at most 60 seconds.
    pub backoff: Duration,
    /// Total window including draining old calls, cleanup and metadata, (0,600s].
    pub timeout: Duration,
}
impl HttpSessionRecoveryPolicy {
    pub(crate) fn retryable(error: &Error) -> bool {
        matches!(
            error.code.as_str(),
            "SessionExpired" | "TransportError" | "TransportClosed" | "Timeout"
        )
    }
    pub(crate) fn deadline(
        self,
        origin: std::time::Instant,
        observed_ms: u64,
    ) -> Result<tokio::time::Instant> {
        origin
            .checked_add(Duration::from_millis(observed_ms))
            .and_then(|at| at.checked_add(self.timeout))
            .map(Into::into)
            .ok_or_else(|| {
                Error::local(
                    "ConfigurationError",
                    "HTTP session recovery exceeds clock range",
                )
            })
    }
    pub(crate) fn validate(self) -> Result<()> {
        if !(1..=8).contains(&self.max_attempts)
            || self.backoff > Duration::from_secs(60)
            || self.timeout.is_zero()
            || self.timeout > Duration::from_secs(600)
        {
            return Err(Error::local(
                "ConfigurationError",
                "HTTP session recovery requires 1..8 rounds, backoff <=60s and total timeout in (0,600s]",
            ));
        }
        Ok(())
    }
}

/// Metadata success does not replenish an automatic recovery burst. Mutable
/// remotes retain this state; queues take it before opening background listeners.
pub(crate) struct HttpRecoveryState {
    pub(crate) deadline: tokio::time::Instant,
    pub(crate) remaining: u8,
    pub(crate) healthy_since: tokio::time::Instant,
}

pub(crate) async fn recover_startup(
    remote: &mut crate::Remote,
    token: tokio_util::sync::CancellationToken,
) -> Result<bool> {
    let Some(policy) = remote
        .driver()
        .http_session_recovery
        .filter(|_| remote.driver().process.is_none())
    else {
        return Ok(false);
    };
    let Some(observed) = remote.machine().http_session_expired_at() else {
        return Ok(false);
    };
    let deadline = policy.deadline(remote.driver().clock_origin(), observed)?;
    let mut remaining = policy.max_attempts;
    loop {
        let delayed = remaining < policy.max_attempts;
        remaining -= 1;
        let result = {
            let round = async {
                if delayed {
                    tokio::time::sleep(policy.backoff).await;
                }
                remote.reconnect().await
            };
            tokio::pin!(round);
            tokio::select! { biased;
                _ = token.cancelled() => Err(Error::local("Cancelled", "initial session recovery cancelled")),
                _ = tokio::time::sleep_until(deadline) => Err(Error::local("Timeout", "initial session recovery budget exhausted")),
                result = &mut round => result,
            }
        };
        if token.is_cancelled() {
            return Err(Error::local(
                "Cancelled",
                "initial session recovery cancelled",
            ));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::local(
                "Timeout",
                "initial session recovery budget exhausted",
            ));
        }
        match result {
            Ok(()) => {
                remote.driver_mut().session_recovery_state = Some(HttpRecoveryState {
                    deadline,
                    remaining,
                    healthy_since: tokio::time::Instant::now(),
                });
                return Ok(true);
            }
            Err(error) if remaining > 0 && HttpSessionRecoveryPolicy::retryable(&error) => {}
            Err(error) => return Err(error),
        }
    }
}
