//! WASIp2 HTTP driver for the shared MCP client.
//!
//! This driver blocks using WASI pollables. Its async trait methods complete in
//! one poll; use a dedicated host task if embedding it in an async application.
//! It creates no runtime, reads no environment/files, and prints no output.
//! Optional HTTP notifications advance during ordinary HTTP waits; idle hosts
//! explicitly call `WasiClientExt::poll_notifications`. No background thread
//! or autonomous async executor is created.
//! `WasiQueue` supplies a separate owner that multiplexes up to 16 HTTP calls,
//! asynchronous discovery/refresh and notifications through the same poll set.
//! Operation futures read results; drive the owner with `poll`, `wait` or `run`.
//! Legacy tool response GET continuation is separately opt-in through
//! `WasiDriver::with_response_recovery`; it shares the original deadline and
//! never replays a tool POST or starts a background credential lookup.
//! `WasiReactor` and weak `WasiRemote` handles allow dependent submissions from
//! local async workflows without holding a queue borrow across future polling.
//! Explicit logical HTTP reconnect is available through `Client::reconnect`,
//! `WasiQueue::reconnect` and `WasiRemote::reconnect`. The queued forms are FIFO
//! metadata barriers. They invalidate old bindings/continuations and never replay
//! tool calls. Failure after reset suspends new work until reconnect or close;
//! cancelling a reconnect that has not reached its barrier keeps the old epoch.
#[cfg(all(target_os = "wasi", target_env = "p2"))]
mod http;
mod recovery;
pub use recovery::{NotificationRecoveryPolicy, ResponseRecoveryPolicy};
#[cfg(any(all(target_os = "wasi", target_env = "p2"), test))]
#[cfg_attr(not(all(target_os = "wasi", target_env = "p2")), allow(dead_code))]
mod operation;
#[cfg(all(target_os = "wasi", target_env = "p2"))]
pub use http::{WasiClientExt, WasiDriver};
#[cfg(all(target_os = "wasi", target_env = "p2"))]
pub use http::{WasiQueue, WasiQueueOptions, WasiQueueStats, WasiReactor, WasiRemote};
#[cfg(all(target_os = "wasi", target_env = "p2"))]
pub use mcpx_command::{CommandSpec, Completion, Execution};
#[cfg(all(target_os = "wasi", target_env = "p2"))]
pub use operation::WasiOperation;

/// Drive a future whose I/O is synchronous (such as this crate's WASI driver).
/// This is not an executor for arbitrary asynchronous providers or drivers.
pub fn run_blocking<T>(
    future: impl std::future::Future<Output = mcpx_core::Result<T>>,
) -> mcpx_core::Result<T> {
    use std::task::{Context, Poll, Waker};
    match std::pin::pin!(future)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result,
        Poll::Pending => Err(mcpx_core::Error::new(
            "CapabilityUnavailable",
            "run_blocking requires a synchronously driven future",
        )),
    }
}
