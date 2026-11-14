use super::{
    exchange::{Exchange, Step},
    *,
};
use crate::ResponseRecoveryPolicy;
use serde_json::Value;
use std::collections::VecDeque;
use wasip2::{http::types::IncomingBody, io::streams::InputStream};

pub(super) fn eligible(action: &Action) -> bool {
    matches!(action,Action::Http{rpc_method,headers,..}if rpc_method=="tools/call"&&headers.get("mcp-protocol-version").map(String::as_str)==Some("2025-11-25"))
}
struct Incoming {
    stream: InputStream,
    _body: IncomingBody,
}
enum Phase {
    Opening(Exchange),
    Body(Incoming),
    Paused,
    Done,
}
struct Budget {
    deadline: u64,
    remaining: u8,
    next: u64,
}

/// One call's frozen transport. Every child stream/pollable is dropped before
/// its parent. A paused call retains its business/input credit and original RPC.
pub(super) struct CallIo {
    phase: Phase,
    pub(super) id: u64,
    url: String,
    headers: BTreeMap<String, String>,
    credential: Credential,
    cancellation: Option<Value>,
    original_deadline: u64,
    policy: Option<ResponseRecoveryPolicy>,
    budget: Option<Budget>,
    next: Option<u64>,
    pending: VecDeque<Vec<u8>>,
    writer: Option<Cleanup>,
    terminal: bool,
    cleanup_deadline: Option<u64>,
}
impl CallIo {
    pub(super) fn new(
        action: Action,
        credential: Credential,
        deadline: u64,
        runtime: &mut HostRuntime,
        policy: Option<ResponseRecoveryPolicy>,
    ) -> Result<Self> {
        let policy = policy.filter(|_| eligible(&action));
        let Action::Http {
            id,
            url,
            headers,
            body,
            cancellation,
            ..
        } = action
        else {
            return Err(Error::local(
                "CapabilityUnavailable",
                "WASI queue supports HTTP only",
            ));
        };
        if policy.is_some() {
            runtime.enable_response_resumption(id)?;
        }
        let phase = Phase::Opening(Exchange::new(
            Method::Post,
            &url,
            &headers,
            serde_json::to_vec(&body).expect("JSON value"),
            &credential,
            deadline,
            false,
        )?);
        Ok(Self {
            phase,
            id,
            url,
            headers,
            credential,
            cancellation,
            original_deadline: deadline,
            policy,
            budget: None,
            next: None,
            pending: VecDeque::new(),
            writer: None,
            terminal: false,
            cleanup_deadline: None,
        })
    }
    fn deadline(&self, deadline: u64) -> u64 {
        self.budget
            .as_ref()
            .map_or(deadline, |b| b.deadline.min(deadline))
    }
    fn pause(&mut self, runtime: &mut HostRuntime, error: Option<Error>) -> Result<bool> {
        let ended = monotonic_clock::now();
        if let Some(policy) = self.policy
            && runtime.interrupt_response_for(self.id).is_ok()
        {
            let budget = self.budget.get_or_insert_with(|| Budget {
                deadline: ended
                    .saturating_add(policy.timeout.as_nanos() as u64)
                    .min(self.original_deadline),
                remaining: policy.max_attempts,
                next: ended,
            });
            budget.next = ended.saturating_add(policy.backoff.as_nanos() as u64);
            self.phase = Phase::Paused;
            return Ok(true);
        }
        if let Some(error) = error {
            return Err(error);
        }
        runtime.finish_response_for(self.id)?;
        self.phase = Phase::Done;
        Ok(true)
    }
    fn read_step(
        &mut self,
        runtime: &mut HostRuntime,
        driver: &mut WasiDriver,
        deadline: u64,
    ) -> Result<bool> {
        ensure(deadline)?;
        match &mut self.phase {
            Phase::Opening(exchange) => match exchange.step() {
                Ok(Step::Pending) => Ok(false),
                Ok(Step::Progress) => Ok(true),
                Err(error) => {
                    runtime.advance_time(driver.now_ms())?;
                    self.pause(runtime, Some(error))
                }
                Ok(Step::Response(response)) => {
                    ensure(deadline)?;
                    runtime.advance_time(driver.now_ms())?;
                    let fields = response
                        .headers()
                        .entries()
                        .into_iter()
                        .map(|(name, value)| {
                            String::from_utf8(value)
                                .map(|value| (name, value))
                                .map_err(|_| {
                                    Error::new("InvalidResponse", "HTTP header is not UTF-8")
                                })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let done =
                        runtime.begin_response_headers(self.id, response.status(), fields)?;
                    if self
                        .headers
                        .get("mcp-protocol-version")
                        .is_some_and(|v| v == "2025-11-25")
                        && let Some(id) = runtime.machine().session_id()
                    {
                        driver.remember_session(&self.url, id, &self.credential);
                    }
                    if done {
                        self.phase = Phase::Done;
                    } else {
                        let body = response.consume().map_err(|_| host_error())?;
                        self.phase = Phase::Body(Incoming {
                            stream: body.stream().map_err(|_| host_error())?,
                            _body: body,
                        });
                    }
                    Ok(true)
                }
            },
            Phase::Body(incoming) => match incoming.stream.read(65536) {
                Ok(bytes) if bytes.is_empty() => Ok(false),
                Ok(bytes) => {
                    ensure(deadline)?;
                    runtime.advance_time(driver.now_ms())?;
                    if runtime.push_response_for(self.id, &bytes)? {
                        self.phase = Phase::Done;
                    }
                    Ok(true)
                }
                Err(StreamError::Closed) => {
                    ensure(deadline)?;
                    runtime.advance_time(driver.now_ms())?;
                    self.pause(runtime, None)
                }
                Err(_) => {
                    runtime.advance_time(driver.now_ms())?;
                    self.pause(runtime, Some(host_error()))
                }
            },
            Phase::Paused | Phase::Done => Ok(false),
        }
    }
    fn replies(&mut self, runtime: &mut HostRuntime) -> Result<()> {
        if self.policy.is_none() {
            return Ok(());
        }
        let replies = match runtime.response_replies(self.id) {
            Ok(replies) => replies,
            Err(error) if error.code == "InvalidHandle" => return Ok(()),
            Err(error) => return Err(error),
        };
        if self.pending.len() + replies.len() > 16 {
            return Err(Error::new("LimitExceeded", "response reply queue exceeded"));
        }
        self.pending.extend(
            replies
                .into_iter()
                .map(|reply| serde_json::to_vec(&reply).expect("JSON value")),
        );
        Ok(())
    }
    fn write_step(&mut self, deadline: u64) -> Result<bool> {
        let mut progress = false;
        if self.writer.is_none()
            && let Some(body) = self.pending.pop_front()
        {
            let deadline = monotonic_clock::now()
                .saturating_add(500_000_000)
                .min(self.cleanup_deadline.unwrap_or(deadline));
            let mut headers = self.headers.clone();
            headers.retain(|name, _| !name.eq_ignore_ascii_case("last-event-id"));
            headers.insert("content-type".into(), "application/json".into());
            headers.insert(
                "accept".into(),
                "application/json, text/event-stream".into(),
            );
            self.writer = Some(Cleanup {
                exchange: Exchange::new(
                    Method::Post,
                    &self.url,
                    &headers,
                    body,
                    &self.credential,
                    deadline,
                    false,
                )?,
                deadline,
            });
            progress = true;
        }
        if let Some(writer) = &mut self.writer {
            ensure(writer.deadline)?;
            match writer.exchange.step()? {
                Step::Pending => {}
                Step::Progress => progress = true,
                Step::Response(response) => {
                    if response.status() != 202 {
                        return Err(host_error());
                    }
                    self.writer = None;
                    progress = true;
                }
            }
        }
        Ok(progress)
    }
    pub(super) fn step(
        &mut self,
        runtime: &mut HostRuntime,
        driver: &mut WasiDriver,
        deadline: u64,
    ) -> Result<bool> {
        self.next = None;
        if self.terminal {
            if self
                .cleanup_deadline
                .is_some_and(|d| monotonic_clock::now() >= d)
            {
                let pending = !self.drained();
                self.abort();
                return Ok(pending);
            }
            return match self.write_step(deadline) {
                Ok(progress) => Ok(progress),
                Err(_) => {
                    self.abort();
                    Ok(true)
                }
            };
        }
        let deadline = self.deadline(deadline);
        ensure(deadline)?;
        let mut progress = self.read_step(runtime, driver, deadline)?;
        let deadline = self.deadline(deadline);
        self.replies(runtime)?;
        if matches!(self.phase, Phase::Done) {
            self.terminal = true;
            self.cleanup_deadline = Some(monotonic_clock::now().saturating_add(500_000_000));
        }
        match self.write_step(deadline) {
            Ok(value) => progress |= value,
            Err(_) if self.terminal => {
                self.abort();
                return Ok(true);
            }
            Err(error) => return Err(error),
        };
        if !matches!(self.phase, Phase::Paused) || self.writer.is_some() || !self.pending.is_empty()
        {
            return Ok(progress);
        }
        runtime.advance_time(driver.now_ms())?;
        let info = runtime.response_resume(self.id)?;
        let budget = self.budget.as_mut().expect("paused budget");
        if budget.remaining == 0 {
            return Err(Error::new(
                "ResponseRecoveryExhausted",
                "response GET attempts exhausted; original tool execution may be unknown",
            ));
        }
        let now = monotonic_clock::now();
        let delay = info
            .retry_after_ms
            .checked_mul(1_000_000)
            .ok_or_else(|| Error::new("LimitExceeded", "response retry exceeds clock range"))?;
        let due = now
            .checked_add(delay)
            .ok_or_else(|| Error::new("LimitExceeded", "response retry exceeds clock range"))?
            .max(budget.next);
        if due >= budget.deadline {
            self.next = Some(budget.deadline);
            return Ok(progress);
        }
        if due > now {
            self.next = Some(due);
            return Ok(progress);
        }
        ensure(deadline)?;
        let plan = runtime.resume_response(self.id)?;
        self.id = plan.action_id;
        self.budget.as_mut().unwrap().remaining -= 1;
        match Exchange::new(
            Method::Get,
            &self.url,
            &plan.headers,
            Vec::new(),
            &self.credential,
            deadline,
            false,
        ) {
            Ok(exchange) => self.phase = Phase::Opening(exchange),
            Err(error) => {
                self.pause(runtime, Some(error))?;
            }
        }
        Ok(true)
    }
    pub(super) fn drained(&self) -> bool {
        matches!(self.phase, Phase::Done) && self.writer.is_none() && self.pending.is_empty()
    }
    pub(super) fn subscribe(&self) -> Vec<Pollable> {
        let mut ready = Vec::with_capacity(5);
        match &self.phase {
            Phase::Opening(exchange) => ready.push(exchange.subscribe()),
            Phase::Body(incoming) => ready.push(incoming.stream.subscribe()),
            _ => {}
        }
        if let Some(writer) = &self.writer {
            ready.push(writer.exchange.subscribe());
            ready.push(monotonic_clock::subscribe_instant(writer.deadline));
        }
        if self.terminal {
            if !self.drained()
                && let Some(d) = self.cleanup_deadline
            {
                ready.push(monotonic_clock::subscribe_instant(d));
            }
        } else {
            ready.push(monotonic_clock::subscribe_instant(
                self.deadline(self.original_deadline),
            ));
            if let Some(next) = self.next {
                ready.push(monotonic_clock::subscribe_instant(next));
            }
        }
        ready
    }
    pub(super) fn abort(&mut self) {
        self.phase = Phase::Done;
        self.writer = None;
        self.pending.clear();
        self.next = None;
    }
    pub(super) fn cancel(&mut self) -> Result<Option<Cleanup>> {
        self.abort();
        let Some(body) = &self.cancellation else {
            return Ok(None);
        };
        let deadline = monotonic_clock::now().saturating_add(200_000_000);
        Ok(Some(Cleanup {
            exchange: Exchange::new(
                Method::Post,
                &self.url,
                &self.headers,
                serde_json::to_vec(body).expect("JSON value"),
                &self.credential,
                deadline,
                false,
            )?,
            deadline,
        }))
    }
}
pub(super) struct Cleanup {
    pub(super) exchange: Exchange,
    pub(super) deadline: u64,
}

impl WasiDriver {
    /// Configure continuation of legacy tool SSE responses, disabled by default.
    /// Queue and Reactor inherit this policy; it creates no background executor.
    pub fn with_response_recovery(
        mut self,
        policy: Option<ResponseRecoveryPolicy>,
    ) -> Result<Self> {
        if let Some(policy) = policy {
            policy.validate()?;
        }
        self.response_recovery = policy;
        Ok(self)
    }
    pub(super) fn perform_resumable(
        &mut self,
        action: Action,
        runtime: &mut HostRuntime,
    ) -> Result<()> {
        let deadline = self.deadline()?;
        if let Action::Http {
            url,
            headers,
            cancellation,
            ..
        } = &action
        {
            self.cancellation = cancellation.as_ref().map(|body| Cancellation {
                url: url.clone(),
                headers: headers.clone(),
                body: serde_json::to_vec(body).expect("JSON value"),
                credential: self.credential.clone(),
            });
        }
        let mut io = CallIo::new(
            action,
            self.credential.clone(),
            deadline,
            runtime,
            self.response_recovery,
        )?;
        loop {
            let progress = io.step(runtime, self, deadline)?;
            self.service_notifications(runtime);
            self.deliver_events(runtime);
            if io.drained() {
                self.cancellation = None;
                return Ok(());
            }
            if !progress {
                let mut ready = io.subscribe();
                ready.extend(self.notification_pollables());
                poll(&ready.iter().collect::<Vec<_>>());
                drop(ready);
            }
        }
    }
}
