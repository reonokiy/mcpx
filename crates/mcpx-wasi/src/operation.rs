use mcpx_command::{CommandSpec, Completion, Execution};
use mcpx_core::{CallOutcome, Error, Result, ToolBinding};
use std::{
    cell::RefCell,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

pub(crate) enum Reply {
    Outcome(CallOutcome),
    Execution(Execution),
    Binding(ToolBinding),
    Completion(Completion),
    Spec(CommandSpec),
    SchemaStats(mcpx_core::SchemaRegistryStats),
    CacheStats(mcpx_core::ValidatorCacheStats),
    Notification(mcpx_core::NotificationStatus),
    Unit,
}
impl Reply {
    pub(crate) fn outcome(&self) -> Option<&CallOutcome> {
        match self {
            Self::Outcome(value) | Self::Execution(Execution::Outcome(value)) => Some(value),
            _ => None,
        }
    }
}
#[derive(Default)]
pub(crate) struct State {
    pub(crate) cancelled: bool,
    pub(crate) done: bool,
    pub(crate) result: Option<Result<Reply>>,
    waker: Option<Waker>,
}
impl State {
    pub(crate) fn settle(state: &Rc<RefCell<Self>>, result: Result<Reply>) {
        let wake = {
            let mut state = state.borrow_mut();
            if state.done {
                return;
            }
            state.done = true;
            state.result = Some(result);
            state.waker.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
/// A caller-owned one-shot result. Dropping an unfinished operation requests
/// cancellation; the queue applies it on its next service pass.
/// Polling this Future never drives network I/O: its owner must also be polled.
pub struct WasiOperation<T> {
    pub(crate) owner: Rc<()>,
    pub(crate) state: Rc<RefCell<State>>,
    id: u64,
    consumed: bool,
    decode: fn(Reply) -> Result<T>,
    output: PhantomData<fn() -> T>,
}
impl<T> std::fmt::Debug for WasiOperation<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasiOperation")
            .field("id", &self.id)
            .field("finished", &self.is_finished())
            .finish_non_exhaustive()
    }
}
impl<T> WasiOperation<T> {
    pub(crate) fn new(
        owner: Rc<()>,
        id: u64,
        state: Rc<RefCell<State>>,
        decode: fn(Reply) -> Result<T>,
    ) -> Self {
        Self {
            owner,
            id,
            state,
            consumed: false,
            decode,
            output: PhantomData,
        }
    }
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn is_finished(&self) -> bool {
        self.state.borrow().done
    }
    pub fn cancel(&self) {
        let mut state = self.state.borrow_mut();
        if !state.done {
            state.cancelled = true;
        }
    }
    pub fn try_result(&mut self) -> Option<Result<T>> {
        if self.consumed {
            return Some(Err(Error::local(
                "InvalidHandle",
                "operation result was already consumed",
            )));
        }
        let result = self.state.borrow_mut().result.take()?;
        self.consumed = true;
        Some(result.and_then(self.decode))
    }
}
impl<T> Future for WasiOperation<T> {
    type Output = Result<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(result) = this.try_result() {
            return Poll::Ready(result);
        }
        let mut state = this.state.borrow_mut();
        if state
            .waker
            .as_ref()
            .is_none_or(|waker| !waker.will_wake(cx.waker()))
        {
            state.waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}
impl<T> Drop for WasiOperation<T> {
    fn drop(&mut self) {
        self.cancel();
        self.state.borrow_mut().waker = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Wake,
    };
    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn unit(reply: Reply) -> Result<()> {
        match reply {
            Reply::Unit => Ok(()),
            _ => unreachable!(),
        }
    }
    #[test]
    fn settlement_wakes_once_and_retained_result_releases_waker() {
        let state = Rc::new(RefCell::new(State::default()));
        let mut operation = WasiOperation::new(Rc::new(()), 1, state.clone(), unit);
        let wake = Arc::new(Counter(AtomicUsize::new(0)));
        let weak = Arc::downgrade(&wake);
        let waker = Waker::from(wake.clone());
        assert!(
            Pin::new(&mut operation)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        State::settle(&state, Ok(Reply::Unit));
        State::settle(&state, Err(Error::local("Cancelled", "late")));
        assert_eq!(wake.0.load(Ordering::SeqCst), 1);
        drop(waker);
        drop(wake);
        assert!(weak.upgrade().is_none());
        operation.cancel();
        assert!(!state.borrow().cancelled);
        operation.try_result().unwrap().unwrap();
        assert_eq!(
            operation.try_result().unwrap().unwrap_err().code,
            "InvalidHandle"
        );
    }
    #[test]
    fn dropping_pending_handle_requests_cancel_and_clears_waker() {
        let state = Rc::new(RefCell::new(State::default()));
        let mut operation = WasiOperation::new(Rc::new(()), 1, state.clone(), unit);
        let wake = Arc::new(Counter(AtomicUsize::new(0)));
        let weak = Arc::downgrade(&wake);
        let waker = Waker::from(wake.clone());
        assert!(
            Pin::new(&mut operation)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(operation);
        drop(waker);
        drop(wake);
        assert!(weak.upgrade().is_none());
        assert!(state.borrow().cancelled);
        State::settle(&state, Ok(Reply::Unit));
        assert!(state.borrow().done);
    }
}
