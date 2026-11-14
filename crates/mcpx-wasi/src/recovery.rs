use mcpx_core::{Error, Result};
use std::time::Duration;

/// Opt-in bounded GET continuation of one legacy HTTP tools/call response.
/// Never replays its POST, initialization or credential lookup.
#[derive(Clone, Copy, Debug)]
pub struct ResponseRecoveryPolicy {
    /// Maximum 1..=8 GETs for the original call. EOF never resets the budget.
    pub max_attempts: u8,
    /// Minimum delay per attempt, at most 60 seconds.
    pub backoff: Duration,
    /// Fixed window (0,600s] from observed first disconnect. The original call
    /// deadline also bounds reply draining, delays, opening and reading.
    pub timeout: Duration,
}
impl ResponseRecoveryPolicy {
    pub fn validate(self) -> Result<()> {
        if !(1..=8).contains(&self.max_attempts)
            || self.backoff > Duration::from_secs(60)
            || self.timeout.is_zero()
            || self.timeout > Duration::from_secs(600)
        {
            return Err(Error::local(
                "ConfigurationError",
                "response recovery requires 1..8 GETs, backoff <=60s and window in (0,600s]",
            ));
        }
        Ok(())
    }
}

/// Opt-in recovery of legacy HTTP notification GETs. No tool POST is retried.
#[derive(Clone, Copy, Debug)]
pub struct NotificationRecoveryPolicy {
    /// Maximum 1..=8 resumed GETs in a burst.
    pub max_attempts: u8,
    /// Minimum delay per attempt, at most 60 seconds.
    pub backoff: Duration,
    /// Total recovery window (0,600s], including draining and opening.
    pub timeout: Duration,
}
impl NotificationRecoveryPolicy {
    pub fn validate(self) -> Result<()> {
        if !(1..=8).contains(&self.max_attempts)
            || self.backoff > Duration::from_secs(60)
            || self.timeout.is_zero()
            || self.timeout > Duration::from_secs(600)
        {
            return Err(Error::local(
                "ConfigurationError",
                "notification recovery requires 1..8 GETs, backoff <=60s and total timeout in (0,600s]",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn response_policy_accepts_both_exact_boundaries() {
        for max_attempts in [1, 8] {
            for backoff in [Duration::ZERO, Duration::from_secs(60)] {
                for timeout in [Duration::from_nanos(1), Duration::from_secs(600)] {
                    ResponseRecoveryPolicy {
                        max_attempts,
                        backoff,
                        timeout,
                    }
                    .validate()
                    .unwrap();
                }
            }
        }
    }
    #[test]
    fn response_policy_rejects_empty_or_unbounded_limits() {
        let valid = ResponseRecoveryPolicy {
            max_attempts: 2,
            backoff: Duration::ZERO,
            timeout: Duration::from_secs(1),
        };
        for policy in [
            ResponseRecoveryPolicy {
                max_attempts: 0,
                ..valid
            },
            ResponseRecoveryPolicy {
                max_attempts: 9,
                ..valid
            },
            ResponseRecoveryPolicy {
                backoff: Duration::from_secs(60) + Duration::from_nanos(1),
                ..valid
            },
            ResponseRecoveryPolicy {
                timeout: Duration::ZERO,
                ..valid
            },
            ResponseRecoveryPolicy {
                timeout: Duration::from_secs(600) + Duration::from_nanos(1),
                ..valid
            },
        ] {
            assert_eq!(policy.validate().unwrap_err().code, "ConfigurationError");
        }
    }
    #[test]
    fn recovery_policy_accepts_exact_boundaries() {
        for max_attempts in [1, 8] {
            for backoff in [Duration::ZERO, Duration::from_secs(60)] {
                for timeout in [Duration::from_nanos(1), Duration::from_secs(600)] {
                    assert!(
                        NotificationRecoveryPolicy {
                            max_attempts,
                            backoff,
                            timeout
                        }
                        .validate()
                        .is_ok()
                    );
                }
            }
        }
    }
    #[test]
    fn recovery_policy_rejects_unbounded_or_empty_windows() {
        let valid = NotificationRecoveryPolicy {
            max_attempts: 1,
            backoff: Duration::ZERO,
            timeout: Duration::from_secs(1),
        };
        for policy in [
            NotificationRecoveryPolicy {
                max_attempts: 0,
                ..valid
            },
            NotificationRecoveryPolicy {
                max_attempts: 9,
                ..valid
            },
            NotificationRecoveryPolicy {
                backoff: Duration::from_secs(60) + Duration::from_nanos(1),
                ..valid
            },
            NotificationRecoveryPolicy {
                timeout: Duration::ZERO,
                ..valid
            },
            NotificationRecoveryPolicy {
                timeout: Duration::from_secs(600) + Duration::from_nanos(1),
                ..valid
            },
        ] {
            assert_eq!(policy.validate().unwrap_err().code, "ConfigurationError");
        }
    }
}
