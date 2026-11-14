use super::*;
impl WasiDriver {
    pub(super) fn reopen_session(&mut self, runtime: &mut HostRuntime) -> Result<()> {
        if self.closed {
            return Err(Error::local(
                "TransportClosed",
                "WASI driver is permanently closed",
            ));
        }
        let deadline = monotonic_clock::now()
            .checked_add(self.timeout_ns)
            .ok_or_else(timeout)?;
        self.reconnect_deadline = Some(deadline);
        self.reconnecting = true;
        self.cancellation = None;
        let wanted = self.http_notifications;
        self.stop_notifications(runtime)?;
        self.http_notifications = wanted;
        self.close_session_until(Some(deadline))?;
        ensure(deadline)
    }
}
