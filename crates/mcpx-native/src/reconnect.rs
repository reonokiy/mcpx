use mcpx_core::{Error, Result};
use std::time::Duration;

/// Opt-in recovery of a failed shared stdio transport. Never retries tools/call.
#[derive(Clone, Copy, Debug)]
pub struct StdioReconnectPolicy {
    /// Consecutive reconnect/discovery rounds (1..=8). Reset after a tool result
    /// or a connection that stays healthy for `timeout`.
    pub max_attempts: u8,
    /// Delay before each round after the first (at most 60 seconds).
    pub backoff: Duration,
    /// Wall-clock recovery window, including cleanup, backoff and discovery
    /// (at most 10 minutes); also the healthy interval that resets the budget.
    pub timeout: Duration,
}
impl StdioReconnectPolicy {
    pub(crate) fn validate(self) -> Result<()> {
        if !(1..=8).contains(&self.max_attempts)
            || self.backoff > Duration::from_secs(60)
            || self.timeout.is_zero()
            || self.timeout > Duration::from_secs(600)
        {
            return Err(Error::local(
                "ConfigurationError",
                "stdio recovery requires 1..8 rounds, backoff <=60s and total timeout in (0,600s]",
            ));
        }
        Ok(())
    }
}
pub(crate) fn recoverable(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "TransportClosed" | "TransportError" | "ProcessStartFailed" | "Timeout"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_recovery_limits_are_rejected_before_connecting() {
        let valid = StdioReconnectPolicy {
            max_attempts: 2,
            backoff: Duration::from_millis(10),
            timeout: Duration::from_secs(2),
        };
        for policy in [
            StdioReconnectPolicy {
                max_attempts: 0,
                ..valid
            },
            StdioReconnectPolicy {
                max_attempts: 9,
                ..valid
            },
            StdioReconnectPolicy {
                backoff: Duration::from_secs(61),
                ..valid
            },
            StdioReconnectPolicy {
                timeout: Duration::ZERO,
                ..valid
            },
            StdioReconnectPolicy {
                timeout: Duration::from_secs(601),
                ..valid
            },
        ] {
            let error = crate::Client::new()
                .unwrap()
                .with_stdio_reconnect(policy)
                .err()
                .unwrap();
            assert_eq!(error.code, "ConfigurationError");
            assert_eq!(
                error.execution_certainty,
                mcpx_core::ExecutionCertainty::NotSent
            );
        }
    }
}
