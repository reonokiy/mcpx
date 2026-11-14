use crate::transport::{Input, Message};
use mcpx_core::{Error, Result};
use mcpx_protocol::MAX_EVENT;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdout, Command},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StdioConfig {
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub working_directory: Option<PathBuf>,
}

impl StdioConfig {
    pub fn new(program: impl Into<String>, args: Vec<String>) -> Self {
        Self {
            program: program.into(),
            args,
            env: BTreeMap::new(),
            working_directory: None,
        }
    }
}

struct Frame {
    bytes: Vec<u8>,
    reply: oneshot::Sender<Result<()>>,
    request: Option<StreamCancellation>,
    _budget: Option<Arc<OwnedSemaphorePermit>>,
}
#[derive(Clone)]
pub(crate) struct StreamWriter(mpsc::Sender<Frame>);
#[derive(Default)]
struct RequestState {
    written: bool,
    cancelled: bool,
    cancellation_sent: bool,
}
#[derive(Clone)]
pub(crate) struct StreamCancellation {
    notification: Value,
    writer: StreamWriter,
    state: Arc<Mutex<RequestState>>,
}
impl StreamCancellation {
    pub(crate) fn abandon(&self) {
        self.state.lock().unwrap().cancelled = true;
    }
    pub(crate) async fn cancel(&self) {
        let send = {
            let mut state = self.state.lock().unwrap();
            state.cancelled = true;
            let send = state.written && !state.cancellation_sent;
            state.cancellation_sent |= send;
            send
        };
        if send {
            let _ = tokio::time::timeout(
                Duration::from_millis(100),
                self.writer.write(&self.notification),
            )
            .await;
        }
    }
}
fn encode(message: &Value) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(message)
        .map_err(|_| Error::new("SerializationFailed", "cannot encode protocol message"))?;
    if bytes.len() > MAX_EVENT {
        return Err(Error::new(
            "LimitExceeded",
            "stdio message exceeds byte limit",
        ));
    }
    bytes.push(b'\n');
    Ok(bytes)
}
impl StreamWriter {
    async fn send(
        &self,
        bytes: Vec<u8>,
        request: Option<StreamCancellation>,
        budget: Option<Arc<OwnedSemaphorePermit>>,
    ) -> Result<()> {
        let (reply, receive) = oneshot::channel();
        self.0
            .send(Frame {
                bytes,
                reply,
                request,
                _budget: budget,
            })
            .await
            .map_err(|_| Error::new("TransportClosed", "MCP stdin is closed"))?;
        receive
            .await
            .map_err(|_| Error::new("TransportClosed", "MCP stdin writer stopped"))?
    }
    pub(crate) async fn write(&self, message: &Value) -> Result<()> {
        self.send(encode(message)?, None, None).await
    }
    pub(crate) async fn write_control_request(
        &self,
        message: &Value,
        cancellation: StreamCancellation,
    ) -> Result<()> {
        self.send(encode(message)?, Some(cancellation), None).await
    }
    pub(crate) fn cancellation(&self, notification: Value) -> StreamCancellation {
        StreamCancellation {
            notification,
            writer: self.clone(),
            state: Arc::new(Mutex::new(RequestState::default())),
        }
    }
    pub(crate) async fn write_request(
        &self,
        message: &Value,
        cancellation: StreamCancellation,
        budget: Arc<OwnedSemaphorePermit>,
    ) -> Result<()> {
        self.send(encode(message)?, Some(cancellation), Some(budget))
            .await
    }
}

pub(crate) struct ProcessDriver {
    pub config: StdioConfig,
    child: Child,
    writer: StreamWriter,
    receiver: Option<mpsc::Receiver<Message>>,
    sender: mpsc::Sender<Message>,
    stop: CancellationToken,
    discard: CancellationToken,
    reader_task: Option<JoinHandle<()>>,
    writer_task: Option<JoinHandle<()>>,
    stderr: Option<JoinHandle<()>>,
}
impl ProcessDriver {
    pub fn spawn(config: StdioConfig) -> Result<Self> {
        if config.program.is_empty() {
            return Err(Error::new(
                "ConfigurationError",
                "stdio executable is empty",
            ));
        }
        let mut command = Command::new(&config.program);
        command
            .args(&config.args)
            .envs(&config.env)
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(directory) = &config.working_directory {
            command.current_dir(directory);
        }
        let mut child = command.spawn().map_err(|_| {
            Error::new(
                "ProcessStartFailed",
                "cannot start configured MCP executable",
            )
        })?;
        let mut input = child.stdin.take().expect("piped stdin");
        let mut output = BufReader::new(child.stdout.take().expect("piped stdout"));
        let mut stderr = child.stderr.take().expect("piped stderr");
        let stderr = tokio::spawn(async move {
            let _ = tokio::io::copy(&mut stderr, &mut tokio::io::sink()).await;
        });
        let (sender, receiver) = mpsc::channel(16);
        let (write, mut frames) = mpsc::channel::<Frame>(16);
        let writer = StreamWriter(write);
        let stop = CancellationToken::new();
        let discard = CancellationToken::new();
        let reader_discard = discard.clone();
        let reader_sender = sender.clone();
        let reader_task = tokio::spawn(async move {
            loop {
                let result = read_frame(&mut output).await;
                let failed = result.is_err();
                let message = match result {
                    Ok(value) => Message {
                        id: 0,
                        input: Input::Stdio(value),
                    },
                    Err(error) => Message {
                        id: 0,
                        input: Input::StdioFailed(error),
                    },
                };
                if !reader_discard.is_cancelled() {
                    tokio::select! {
                        _ = reader_discard.cancelled() => {},
                        sent = reader_sender.send(message) => if sent.is_err() {break;},
                    }
                }
                if failed {
                    break;
                }
            }
        });
        let writer_stop = stop.clone();
        let writer_sender = sender.clone();
        let writer_task = tokio::spawn(async move {
            loop {
                let frame = tokio::select! {biased; _ = writer_stop.cancelled()=>break, frame = frames.recv()=>match frame {Some(frame)=>frame,None=>break}};
                if frame.reply.is_closed()
                    || frame
                        .request
                        .as_ref()
                        .is_some_and(|request| request.state.lock().unwrap().cancelled)
                {
                    continue;
                }
                let writing = async {
                    input.write_all(&frame.bytes).await?;
                    input.flush().await?;
                    let cancellation = frame.request.as_ref().is_some_and(|request| {
                        let mut state = request.state.lock().unwrap();
                        state.written = true;
                        let send = state.cancelled && !state.cancellation_sent;
                        state.cancellation_sent |= send;
                        send
                    });
                    if cancellation {
                        input
                            .write_all(
                                &encode(&frame.request.as_ref().unwrap().notification)
                                    .expect("small cancellation message"),
                            )
                            .await?;
                        input.flush().await?;
                    }
                    Ok::<_, std::io::Error>(())
                };
                let result = tokio::select! {biased; _ = writer_stop.cancelled()=>break, result = writing=>result};
                let result = result.map_err(|_| {
                    Error::new(
                        "TransportError",
                        "MCP stdin write failed; execution may be unknown",
                    )
                });
                let failure = result.as_ref().err().cloned();
                let _ = frame.reply.send(result);
                if let Some(error) = failure {
                    tokio::select! { _ = writer_stop.cancelled()=>{}, _ = writer_sender.send(Message {id:0,input:Input::StdioFailed(error)})=>{} }
                    break;
                }
            }
        });
        Ok(Self {
            config,
            child,
            writer,
            receiver: Some(receiver),
            sender,
            stop,
            discard,
            reader_task: Some(reader_task),
            writer_task: Some(writer_task),
            stderr: Some(stderr),
        })
    }
    pub(crate) fn multiplex_io(
        &mut self,
    ) -> Result<(StreamWriter, mpsc::Sender<Message>, mpsc::Receiver<Message>)> {
        let receiver = self
            .receiver
            .take()
            .ok_or_else(|| Error::new("Busy", "stdio reader is owned by a host"))?;
        Ok((self.writer.clone(), self.sender.clone(), receiver))
    }
    pub(crate) fn install_receiver(&mut self, receiver: mpsc::Receiver<Message>) {
        self.receiver = Some(receiver);
    }
    pub(crate) fn take_receiver(&mut self) -> Result<mpsc::Receiver<Message>> {
        self.receiver
            .take()
            .ok_or_else(|| Error::new("TransportClosed", "stdio reader is unavailable"))
    }
    pub async fn write(&mut self, message: &Value) -> Result<()> {
        self.writer.write(message).await
    }
    pub async fn read(&mut self) -> Result<Value> {
        let message = self
            .receiver
            .as_mut()
            .ok_or_else(|| Error::new("Busy", "stdio reader is owned by a host"))?
            .recv()
            .await
            .ok_or_else(|| Error::new("TransportClosed", "MCP stdout reader stopped"))?;
        match message.input {
            Input::Stdio(value) => Ok(value),
            Input::StdioFailed(error) => Err(error),
            _ => Err(Error::new("ProtocolError", "unexpected stdio input")),
        }
    }
    pub async fn close(&mut self) -> Result<()> {
        self.discard.cancel();
        self.stop.cancel();
        if let Some(task) = self.writer_task.as_mut() {
            let _ = task.await;
        }
        self.writer_task = None;
        let result =
            match tokio::time::timeout(Duration::from_millis(500), self.child.wait()).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(_)) => Err(Error::new(
                    "ProcessShutdownFailed",
                    "cannot reap MCP child process",
                )),
                Err(_) => self.child.kill().await.map_err(|_| {
                    Error::new("ProcessShutdownFailed", "cannot stop MCP child process")
                }),
            };
        if let Some(task) = self.reader_task.as_mut() {
            task.abort();
            let _ = task.await;
        }
        self.reader_task = None;
        if let Some(task) = self.stderr.as_mut() {
            task.abort();
            let _ = task.await;
        }
        self.stderr = None;
        result
    }
}
impl Drop for ProcessDriver {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(task) = &self.reader_task {
            task.abort();
        }
        if let Some(task) = &self.writer_task {
            task.abort();
        }
        if let Some(task) = &self.stderr {
            task.abort();
        }
    }
}
async fn read_frame(output: &mut BufReader<ChildStdout>) -> Result<Value> {
    let mut line = Vec::new();
    loop {
        let buffer = output
            .fill_buf()
            .await
            .map_err(|_| Error::new("TransportError", "cannot read MCP stdout"))?;
        if buffer.is_empty() {
            return Err(Error::new(
                "TransportClosed",
                "MCP server closed stdout; execution may be unknown",
            ));
        }
        let count = buffer
            .iter()
            .position(|b| *b == b'\n')
            .map_or(buffer.len(), |n| n + 1);
        if line.len().saturating_add(count) > MAX_EVENT {
            return Err(Error::new(
                "LimitExceeded",
                "stdio message exceeds byte limit",
            ));
        }
        let complete = buffer[count - 1] == b'\n';
        line.extend_from_slice(&buffer[..count]);
        output.consume(count);
        if complete {
            return serde_json::from_slice(&line)
                .map_err(|_| Error::new("ProtocolError", "invalid JSON-RPC on MCP stdout"));
        }
    }
}
