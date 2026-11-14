use super::*;

/// Opt-in recovery of resumable legacy HTTP catalog listeners. Server `retry`
/// is a minimum, never a suggestion to replay a tools/call POST.
#[derive(Clone, Copy, Debug)]
pub struct NotificationRecoveryPolicy {
    /// At most 1..=8 resumed GETs in one recovery burst.
    pub max_attempts: u8,
    /// Minimum delay before each resumed GET, at most 60 seconds.
    pub backoff: Duration,
    /// Total recovery window (0,600s], including reply draining and opening.
    /// A listener healthy for this interval resets the burst budget.
    pub timeout: Duration,
}
impl NotificationRecoveryPolicy {
    pub(super) fn validate(self) -> Result<()> {
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

pub(super) struct Budget {
    pub root: u64,
    pub failed_id: u64,
    pub deadline: Instant,
    pub next: Instant,
    pub remaining: u8,
}
