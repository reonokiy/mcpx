pub mod oauth;
mod operation_events;
use mcpx_command::ClientCommandExt;
pub use mcpx_command::{
    CandidateKind, CommandSpec, Completion, CompletionCandidate, Execution, ParameterSpec,
};
use mcpx_core::{Dialect, Error, HostRuntime, Result};
pub use operation_events::OperationEventStream;
use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
    time::Duration,
};
mod config;
mod credentials;
mod driver;
mod events;
mod queue;
mod reconnect;
mod response_recovery;
mod session_recovery;
pub use events::{EventStream, EventStreamItem};
pub use queue::{
    NotificationRecoveryPolicy, QueueOptions, QueuedArgs, QueuedArgsBuilder, QueuedBinding,
    QueuedCancellation, QueuedOperation, QueuedRemote,
};
pub use reconnect::StdioReconnectPolicy;
pub use response_recovery::ResponseRecoveryPolicy;
pub use session_recovery::HttpSessionRecoveryPolicy;
mod stdio;
mod transport;
pub use config::{Config, ProtocolMode, ServerConfig};
pub use credentials::EnvCredentialProvider;
pub use driver::{CancellationHandle, NativeDriver};
pub use mcpx_core::{
    Credential, CredentialFuture, CredentialProvider, CredentialRequest, EventBatch, EventLimits,
    EventStats, SchemaRegistry, SchemaRegistryStats, SchemaResource,
};
pub use stdio::StdioConfig;
type EventHandler = Box<dyn FnMut(&EventStreamItem)>;

pub struct Client {
    response_recovery: Option<ResponseRecoveryPolicy>,
    reconnect: Option<StdioReconnectPolicy>,
    http_session_recovery: Option<HttpSessionRecoveryPolicy>,
    http: reqwest::Client,
    timeout: Duration,
    provider: Option<Box<dyn CredentialProvider>>,
    events: Option<EventHandler>,
    catalog_ttl: Duration,
    event_stream: Option<events::EventProducer>,
    credential_reference: Option<String>,
    cancellation: tokio_util::sync::CancellationToken,
    schema_registry: Option<SchemaRegistry>,
    validator_cache_capacity: usize,
}

pub struct Remote {
    inner: mcpx_core::Client<NativeDriver>,
}
impl Deref for Remote {
    type Target = mcpx_core::Client<NativeDriver>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl DerefMut for Remote {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Client {
    pub fn new() -> Result<Self> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| Error::local("ConfigurationError", "cannot construct HTTP client"))?;
        Ok(Self {
            response_recovery: None,
            reconnect: None,
            http_session_recovery: None,
            schema_registry: None,
            validator_cache_capacity: mcpx_core::DEFAULT_VALIDATOR_CACHE_CAPACITY,
            http,
            timeout: Duration::from_secs(60),
            provider: None,
            events: None,
            catalog_ttl: Duration::from_secs(60),
            event_stream: None,
            credential_reference: None,
            cancellation: tokio_util::sync::CancellationToken::new(),
        })
    }
    pub fn with_bearer(mut self, token: String) -> Self {
        self.provider = Some(Box::new(credentials::StaticCredentialProvider { token }));
        self
    }
    /// Continue resumable legacy HTTP tool responses via bounded GETs. Also
    /// applies when this builder is returned by a queued owner's factory.
    pub fn with_response_recovery(mut self, policy: ResponseRecoveryPolicy) -> Result<Self> {
        policy.validate()?;
        self.response_recovery = Some(policy);
        Ok(self)
    }
    pub fn cancellation_handle(&self) -> CancellationHandle {
        CancellationHandle(self.cancellation.clone())
    }
    pub fn with_credential_provider(mut self, provider: impl CredentialProvider + 'static) -> Self {
        self.provider = Some(Box::new(provider));
        self
    }
    pub fn with_credential_reference(mut self, reference: impl Into<String>) -> Self {
        self.credential_reference = Some(reference.into());
        self
    }
    /// Enable bounded automatic recovery in a QueuedRemote owner.
    /// Mutable Remote clients use explicit `reconnect()` instead.
    pub fn with_stdio_reconnect(mut self, policy: StdioReconnectPolicy) -> Result<Self> {
        policy.validate()?;
        self.reconnect = Some(policy);
        Ok(self)
    }
    /// Enable bounded session renewal during initial HTTP discovery, before a
    /// mutable Remote's next operation, and in a QueuedRemote owner.
    /// Never replays a failed tool call. Disabled by default.
    pub fn with_http_session_recovery(mut self, policy: HttpSessionRecoveryPolicy) -> Result<Self> {
        policy.validate()?;
        self.http_session_recovery = Some(policy);
        Ok(self)
    }
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    pub fn with_catalog_ttl(mut self, ttl: Duration) -> Self {
        self.catalog_ttl = ttl;
        self
    }
    pub fn with_schema_registry(mut self, registry: SchemaRegistry) -> Self {
        self.schema_registry = Some(registry);
        self
    }
    pub fn with_validator_cache_capacity(mut self, capacity: usize) -> Result<Self> {
        if capacity > mcpx_core::MAX_VALIDATOR_CACHE_CAPACITY {
            return Err(Error::local(
                "ConfigurationError",
                "validator cache capacity must be 0..4096",
            ));
        }
        self.validator_cache_capacity = capacity;
        Ok(self)
    }
    pub fn with_event_handler(mut self, handler: impl FnMut(&EventStreamItem) + 'static) -> Self {
        self.events = Some(Box::new(handler));
        self
    }
    pub fn with_event_stream(self, capacity: usize) -> Result<(Self, EventStream)> {
        self.with_event_stream_options(mcpx_core::EventLimits {
            capacity,
            ..Default::default()
        })
    }
    pub fn with_event_stream_options(
        mut self,
        limits: mcpx_core::EventLimits,
    ) -> Result<(Self, EventStream)> {
        if self.event_stream.is_some() {
            return Err(Error::local(
                "ConfigurationError",
                "an event stream is already configured",
            ));
        }
        let (producer, receiver) = events::channel(limits)?;
        self.event_stream = Some(producer);
        Ok((self, receiver))
    }
    async fn build(
        mut self,
        endpoint: String,
        mut runtime: HostRuntime,
        process: Option<stdio::ProcessDriver>,
    ) -> Result<Remote> {
        if let Some(reference) = &self.credential_reference {
            if reference.is_empty() || reference.len() > 1024 {
                return Err(Error::local(
                    "ConfigurationError",
                    "credential reference must contain 1 to 1024 bytes",
                ));
            }
            if self.provider.is_none() {
                if reference == "oauth" || reference.starts_with("oauth:") {
                    self.provider = Some(Box::new(oauth::OAuthCredentialProvider::for_profile(
                        &endpoint,
                        reference.strip_prefix("oauth:").unwrap_or("default"),
                    )?));
                } else {
                    let variable = reference
                        .strip_prefix("env:")
                        .filter(|variable| !variable.is_empty())
                        .ok_or_else(|| {
                            Error::local(
                                "CredentialUnavailable",
                                "named credential reference requires a credential provider",
                            )
                        })?;
                    self.provider = Some(Box::new(EnvCredentialProvider::new(variable)));
                }
            }
        }
        if let Some(registry) = self.schema_registry.take() {
            runtime.set_schema_registry(registry)?;
        }
        runtime.set_validator_cache_capacity(self.validator_cache_capacity)?;
        let ttl = self.catalog_ttl.as_millis().min(u128::from(u64::MAX)) as u64;
        // Driver finish rotates per-operation tokens. A connection attempt and
        // its metadata renewal rounds retain the caller's original cancellation.
        let startup_cancellation = self.cancellation.clone();
        let driver = NativeDriver::new(self, endpoint, process);
        let mut remote = Remote {
            inner: mcpx_core::Client::new(driver, runtime).with_catalog_ttl(ttl),
        };
        if let Err(error) = remote.refresh().await {
            let recovery =
                session_recovery::recover_startup(&mut remote, startup_cancellation).await;
            let error = match recovery {
                Ok(true) => None,
                Ok(false) => Some(error),
                Err(error) => Some(error),
            };
            if let Some(error) = error {
                let _ = remote.close().await;
                return Err(error);
            }
        }
        remote.driver_mut().mutable_session_recovery = true;
        Ok(remote)
    }
    pub async fn connect(self, endpoint: &str, dialect: Dialect) -> Result<Remote> {
        self.build(
            endpoint.into(),
            HostRuntime::with_dialect(endpoint.into(), dialect)?,
            None,
        )
        .await
    }
    pub async fn connect_stdio(self, config: StdioConfig, dialect: Dialect) -> Result<Remote> {
        self.build(
            "stdio:".into(),
            HostRuntime::stdio(dialect),
            Some(stdio::ProcessDriver::spawn(config)?),
        )
        .await
    }
}

impl Remote {
    pub async fn complete_line(&mut self, line: &str, cursor: usize) -> Result<Completion> {
        self.inner.complete_line(line, cursor).await
    }
    pub async fn command_spec(&mut self, tool: &str) -> Result<CommandSpec> {
        self.inner.command_spec(tool).await
    }
    pub fn cancellation_handle(&self) -> CancellationHandle {
        self.inner.driver().cancellation_handle()
    }
    pub async fn execute(&mut self, tool: &str, text: &str) -> Result<Execution> {
        self.inner.execute(tool, text).await
    }
    pub async fn execute_argv(&mut self, tool: &str, argv: &[String]) -> Result<Execution> {
        self.inner.execute_argv(tool, argv).await
    }
}

#[derive(Default)]
pub struct Engine {
    remotes: BTreeMap<String, Remote>,
}

impl Engine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, alias: &str, remote: Remote) -> Result<()> {
        validate_alias(alias)?;
        if self.remotes.contains_key(alias) {
            return Err(Error::local(
                "DuplicateAlias",
                "server alias is already registered",
            ));
        }
        self.remotes.insert(alias.into(), remote);
        Ok(())
    }

    pub fn remote_mut(&mut self, alias: &str) -> Result<&mut Remote> {
        self.remotes
            .get_mut(alias)
            .ok_or_else(|| Error::local("RemoteNotFound", "server alias is not registered"))
    }

    pub async fn execute(&mut self, command: &str) -> Result<Execution> {
        self.execute_argv(&mcpx_command::tokenize(command)?).await
    }

    pub async fn execute_argv<S: AsRef<str>>(&mut self, argv: &[S]) -> Result<Execution> {
        if argv.len() < 2 {
            return Err(Error::local(
                "CommandParseError",
                "expected server alias and tool name",
            ));
        }
        let args: Vec<String> = argv[2..]
            .iter()
            .map(|arg| arg.as_ref().to_owned())
            .collect();
        self.remote_mut(argv[0].as_ref())?
            .execute_argv(argv[1].as_ref(), &args)
            .await
    }
}

pub use mcpx_config::validate_alias;
