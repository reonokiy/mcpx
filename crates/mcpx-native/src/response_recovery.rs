use crate::transport::{Input, Message};
use futures_util::StreamExt;
use mcpx_core::{Action, Credential, Error, HostRuntime, Result};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::{
    collections::{BTreeMap, VecDeque},
    time::{Duration, Instant},
};
use tokio::{
    sync::{Notify, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

/// Opt-in GET continuation of one legacy HTTP tool response. No tool POST,
/// initialization, provider lookup or new business operation is retried.
#[derive(Clone, Copy, Debug)]
pub struct ResponseRecoveryPolicy {
    /// Maximum 1..=8 resumed GETs for the entire original call (no reset).
    pub max_attempts: u8,
    /// Minimum delay per GET, at most 60 seconds; server retry may be longer.
    pub backoff: Duration,
    /// Window (0,600s] from reader-observed first disconnect, including reply
    /// draining, delay, opening and reading. The original call deadline also applies.
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
pub(crate) fn eligible(action: &Action) -> bool {
    matches!(action,Action::Http{rpc_method,headers,..}if rpc_method=="tools/call"&&headers.get("mcp-protocol-version").map(String::as_str)==Some(mcpx_protocol::LEGACY_VERSION))
}
pub(crate) fn now(origin: Instant) -> u64 {
    origin.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}
fn transport(message: &str) -> Error {
    Error::new("TransportError", message)
}
fn timeout() -> Error {
    Error::new(
        "Timeout",
        "response recovery exceeded the original call deadline or recovery window",
    )
}

pub(crate) async fn reader(
    http: reqwest::Client,
    credential: Credential,
    id: u64,
    url: String,
    headers: BTreeMap<String, String>,
    body: Option<serde_json::Value>,
    sender: mpsc::Sender<Message>,
) {
    let result = async {
        let mut request = if let Some(body) = body {
            http.post(url).body(body.to_string())
        } else {
            http.get(url)
        };
        for (name, value) in headers {
            request = request.header(name, value);
        }
        if let Some(token) = credential.token() {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|_| transport("HTTP response request failed; execution may be unknown"))?;
        let status = response.status().as_u16();
        let headers: BTreeMap<String, String> = response
            .headers()
            .iter()
            .filter_map(|(k, v)| v.to_str().ok().map(|v| (k.to_string(), v.into())))
            .collect();
        if sender
            .send(Message {
                id,
                input: Input::Headers(status, serde_json::to_string(&headers).unwrap()),
            })
            .await
            .is_err()
        {
            return Ok(());
        }
        let mut stream = response.bytes_stream();
        while let Some(part) = stream.next().await {
            let part = part.map_err(|_| {
                transport("HTTP response stream interrupted; execution may be unknown")
            })?;
            for bytes in part.chunks(65536) {
                if sender
                    .send(Message {
                        id,
                        input: Input::Chunk(bytes.to_vec()),
                    })
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }
        }
        let _ = sender
            .send(Message {
                id,
                input: Input::Eof(Instant::now()),
            })
            .await;
        Ok::<(), Error>(())
    }
    .await;
    if let Err(error) = result {
        let _ = sender
            .send(Message {
                id,
                input: Input::Failed(error, Instant::now()),
            })
            .await;
    }
}
struct Budget {
    deadline: Instant,
    remaining: u8,
    next: Instant,
}
#[derive(Default)]
struct Aborts {
    read: Option<tokio::task::AbortHandle>,
    writer: Option<tokio::task::AbortHandle>,
}
pub(crate) struct AbortOnDrop(Arc<Mutex<Aborts>>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        let handles = self.0.lock().unwrap();
        if let Some(h) = &handles.read {
            h.abort();
        }
        if let Some(h) = &handles.writer {
            h.abort();
        }
    }
}
/// Shared I/O state for mutable and queued native clients. The owner alone
/// advances Core; child tasks own only frozen HTTP snapshots and bounded bytes.
pub(crate) struct ResponseIo {
    pub(crate) id: u64,
    http: reqwest::Client,
    credential: Credential,
    url: String,
    headers: BTreeMap<String, String>,
    policy: ResponseRecoveryPolicy,
    deadline: Instant,
    cancellation: CancellationToken,
    shutdown: CancellationToken,
    sender: mpsc::Sender<Message>,
    wake: Arc<Notify>,
    read: Option<JoinHandle<()>>,
    writer: Option<JoinHandle<Result<()>>>,
    writer_done: Arc<AtomicBool>,
    pending: VecDeque<serde_json::Value>,
    paused: bool,
    budget: Option<Budget>,
    next: Option<Instant>,
    terminal: Option<bool>,
    cleanup: Option<Instant>,
    aborts: Arc<Mutex<Aborts>>,
}
impl Drop for ResponseIo {
    fn drop(&mut self) {
        if let Some(task) = &self.read {
            task.abort();
        }
        if let Some(task) = &self.writer {
            task.abort();
        }
    }
}
impl ResponseIo {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        runtime: &mut HostRuntime,
        action: Action,
        http: reqwest::Client,
        credential: Credential,
        policy: ResponseRecoveryPolicy,
        deadline: Instant,
        cancellation: CancellationToken,
        shutdown: CancellationToken,
        sender: mpsc::Sender<Message>,
        wake: Arc<Notify>,
    ) -> Result<Self> {
        let Action::Http {
            id,
            url,
            headers,
            body,
            ..
        } = action
        else {
            return Err(Error::local(
                "ResumeUnavailable",
                "response recovery requires legacy HTTP",
            ));
        };
        runtime.enable_response_resumption(id)?;
        let mut io = Self {
            id,
            http,
            credential,
            url,
            headers,
            policy,
            deadline,
            cancellation,
            shutdown,
            sender,
            wake,
            read: None,
            writer: None,
            writer_done: Arc::new(AtomicBool::new(false)),
            pending: VecDeque::new(),
            paused: false,
            budget: None,
            next: None,
            terminal: None,
            cleanup: None,
            aborts: Arc::new(Mutex::new(Aborts::default())),
        };
        io.read = Some(io.spawn_reader(io.headers.clone(), Some(body)));
        io.register();
        Ok(io)
    }
    fn register(&self) {
        let mut h = self.aborts.lock().unwrap();
        h.read = self.read.as_ref().map(JoinHandle::abort_handle);
        h.writer = self.writer.as_ref().map(JoinHandle::abort_handle);
    }
    pub(crate) fn abort_on_drop(&self) -> AbortOnDrop {
        AbortOnDrop(self.aborts.clone())
    }
    fn spawn_reader(
        &self,
        headers: BTreeMap<String, String>,
        body: Option<serde_json::Value>,
    ) -> JoinHandle<()> {
        let (http, credential, id, url, sender) = (
            self.http.clone(),
            self.credential.clone(),
            self.id,
            self.url.clone(),
            self.sender.clone(),
        );
        let deadline = self
            .budget
            .as_ref()
            .map_or(self.deadline, |b| b.deadline.min(self.deadline));
        let cancellation = self.cancellation.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let error = if shutdown.is_cancelled() {
                Some(Error::new("TransportClosed", "response owner is closing"))
            } else if cancellation.is_cancelled() {
                Some(Error::new("Cancelled", "response send cancelled"))
            } else if Instant::now() >= deadline {
                Some(timeout())
            } else {
                None
            };
            if let Some(error) = error {
                let _ = sender
                    .send(Message {
                        id,
                        input: Input::Failed(error, Instant::now()),
                    })
                    .await;
                return;
            }
            reader(http, credential, id, url, headers, body, sender).await;
        })
    }
    pub(crate) fn terminal(&mut self, success: bool) {
        if self.terminal.is_none() {
            self.terminal = Some(success);
            self.cleanup = Some(Instant::now() + Duration::from_millis(500));
            self.next = None;
        }
    }
    fn interrupted(&self) -> Result<()> {
        if self.shutdown.is_cancelled() {
            return Err(Error::new("TransportClosed", "response owner is closing"));
        }
        if self.cancellation.is_cancelled() {
            return Err(Error::new(
                "Cancelled",
                "response operation cancelled; execution may be unknown",
            ));
        }
        if Instant::now() >= self.deadline
            || self
                .budget
                .as_ref()
                .is_some_and(|b| Instant::now() >= b.deadline)
        {
            return Err(timeout());
        }
        Ok(())
    }
    pub(crate) fn feed(
        &mut self,
        runtime: &mut HostRuntime,
        input: Input,
        origin: Instant,
    ) -> Result<()> {
        self.interrupted()?;
        runtime.advance_time(now(origin))?;
        match input {
            Input::Headers(status, headers) => {
                runtime.begin_response(self.id, status, &headers)?;
            }
            Input::Chunk(bytes) => {
                runtime.push_response_for(self.id, &bytes)?;
            }
            Input::Eof(ended) => {
                if runtime.interrupt_response_for(self.id).is_ok() {
                    self.pause(ended);
                } else {
                    runtime.finish_response_for(self.id)?;
                }
            }
            Input::Failed(error, ended) => {
                if runtime.interrupt_response_for(self.id).is_ok() {
                    self.pause(ended);
                } else {
                    return Err(error);
                }
            }
            _ => unreachable!("business HTTP input"),
        }
        self.drain(runtime)?;
        Ok(())
    }
    fn pause(&mut self, ended: Instant) {
        self.paused = true;
        let budget = self.budget.get_or_insert_with(|| Budget {
            deadline: (ended + self.policy.timeout).min(self.deadline),
            remaining: self.policy.max_attempts,
            next: ended + self.policy.backoff,
        });
        budget.next = ended + self.policy.backoff;
    }
    fn drain(&mut self, runtime: &mut HostRuntime) -> Result<()> {
        let replies = match runtime.response_replies(self.id) {
            Ok(replies) => replies,
            Err(error) if error.code == "InvalidHandle" => return Ok(()),
            Err(error) => return Err(error),
        };
        if self.pending.len() + replies.len() > 16 {
            return Err(Error::new("LimitExceeded", "response reply queue exceeded"));
        }
        self.pending.extend(replies);
        Ok(())
    }
    fn start_writer(&mut self) {
        if self.writer.is_some() {
            return;
        }
        let Some(reply) = self.pending.pop_front() else {
            return;
        };
        let http = self.http.clone();
        let credential = self.credential.clone();
        let url = self.url.clone();
        let headers = self.headers.clone();
        let wake = self.wake.clone();
        let mut deadline = Instant::now() + Duration::from_millis(500);
        if let Some(cleanup) = self.cleanup {
            deadline = deadline.min(cleanup);
        } else {
            deadline = deadline.min(self.deadline);
            if let Some(b) = &self.budget {
                deadline = deadline.min(b.deadline);
            }
        }
        self.writer_done.store(false, Ordering::Release);
        let done = self.writer_done.clone();
        self.writer = Some(tokio::spawn(async move {
            let result = tokio::time::timeout_at(deadline.into(), async {
                let mut request = http
                    .post(url)
                    .body(reply.to_string())
                    .header("content-type", "application/json")
                    .header("accept", "application/json, text/event-stream");
                for (name, value) in headers {
                    if !["last-event-id", "content-type", "accept"]
                        .iter()
                        .any(|field| name.eq_ignore_ascii_case(field))
                    {
                        request = request.header(name, value);
                    }
                }
                if let Some(token) = credential.token() {
                    request = request.bearer_auth(token);
                }
                let response = request
                    .send()
                    .await
                    .map_err(|_| transport("response capability reply failed"))?;
                if response.status().as_u16() != 202 {
                    return Err(transport("response capability reply rejected"));
                }
                Ok(())
            })
            .await
            .unwrap_or_else(|_| Err(transport("response capability reply timed out")));
            done.store(true, Ordering::Release);
            wake.notify_one();
            result
        }));
        self.register();
    }
    /// One bounded owner pass; never waits on remote headers/body/reply ACKs.
    pub(crate) async fn service(
        &mut self,
        runtime: &mut HostRuntime,
        origin: Instant,
    ) -> Result<()> {
        runtime.advance_time(now(origin))?;
        self.next = None;
        if self.terminal.is_none() {
            self.interrupted()?;
        }
        if self.terminal == Some(false)
            || self
                .cleanup
                .is_some_and(|deadline| Instant::now() >= deadline)
        {
            self.close().await;
            return Ok(());
        }
        if self.writer.is_some() && self.writer_done.load(Ordering::Acquire) {
            let result = self
                .writer
                .take()
                .unwrap()
                .await
                .unwrap_or_else(|_| Err(transport("response capability writer stopped")));
            if let Err(error) = result {
                self.pending.clear();
                if self.terminal.is_none() {
                    return Err(error);
                }
            }
        }
        self.drain(runtime)?;
        self.start_writer();
        if self.terminal.is_some() {
            if let Some(task) = self.read.take() {
                task.abort();
                let _ = task.await;
            }
            return Ok(());
        }
        if !self.paused {
            return Ok(());
        }
        if let Some(task) = self.read.take() {
            task.abort();
            let _ = task.await;
        }
        if self.writer.is_some() || !self.pending.is_empty() {
            return Ok(());
        }
        let info = runtime.response_resume(self.id)?;
        let budget = self.budget.as_mut().expect("paused budget");
        if budget.remaining == 0 {
            return Err(Error::new(
                "ResponseRecoveryExhausted",
                "response GET attempts exhausted; original tool execution may be unknown",
            ));
        }
        let due = Instant::now()
            .checked_add(Duration::from_millis(info.retry_after_ms))
            .unwrap_or(budget.deadline)
            .max(budget.next);
        if due >= budget.deadline {
            self.next = Some(budget.deadline);
            return Ok(());
        }
        if due > Instant::now() {
            self.next = Some(due);
            return Ok(());
        }
        self.interrupted()?;
        let plan = runtime.resume_response(self.id)?;
        self.budget.as_mut().unwrap().remaining -= 1;
        self.id = plan.action_id;
        self.paused = false;
        self.read = Some(self.spawn_reader(plan.headers, None));
        self.register();
        Ok(())
    }
    pub(crate) fn wake_deadline(&self) -> Option<Instant> {
        if self.terminal.is_some() {
            self.cleanup
        } else {
            self.next
                .into_iter()
                .chain(self.budget.as_ref().map(|b| b.deadline))
                .min()
        }
    }
    pub(crate) fn drained(&self) -> bool {
        self.read.is_none() && self.writer.is_none() && self.pending.is_empty()
    }
    pub(crate) fn is_terminal(&self) -> bool {
        self.terminal.is_some()
    }
    pub(crate) fn external_stop(&self) -> bool {
        self.cancellation.is_cancelled()
            || self.shutdown.is_cancelled()
            || Instant::now() >= self.deadline
    }
    pub(crate) async fn close(&mut self) {
        self.pending.clear();
        self.next = None;
        if let Some(task) = self.read.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = self.writer.take() {
            task.abort();
            let _ = task.await;
        }
        self.register();
    }
}
