//! Local official-SDK test service for the standalone WASI smoke matrix.
//! No mcpx client implementation or generated tool SDK is used by this server.
use rmcp::{
    ErrorData, RoleServer, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, DiscoverResult, ListToolsResult,
        PaginatedRequestParams, ProtocolVersion, ServerCapabilities, ServerConfig,
        SubscriptionFilter,
    },
    service::{RequestContext, SubscriptionContext},
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService,
        session::{SessionManager, local::LocalSessionManager},
    },
};
use serde_json::json;
use std::{
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
#[path = "../tests/support/sdk_server.rs"]
mod sdk_server;

#[derive(Default)]
struct Metrics {
    calls: AtomicUsize,
    active: AtomicUsize,
    maximum: AtomicUsize,
    lists: AtomicUsize,
    parallel: AtomicUsize,
    resumes: AtomicUsize,
}
#[derive(Clone)]
struct Server {
    inner: sdk_server::SdkServer,
    metrics: Arc<Metrics>,
    parallel: Arc<tokio::sync::Barrier>,
    resumes: Arc<tokio::sync::Barrier>,
    ttl_ms: u64,
    legacy: bool,
    changes: tokio::sync::broadcast::Sender<()>,
}
struct Active(Arc<Metrics>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}
impl ServerHandler for Server {
    fn supported_protocol_versions(&self) -> std::borrow::Cow<'static, [ProtocolVersion]> {
        std::borrow::Cow::Borrowed(if self.legacy {
            ProtocolVersion::known_up_to(&ProtocolVersion::V_2025_11_25)
        } else {
            ProtocolVersion::KNOWN_VERSIONS
        })
    }
    fn accepted_subscription_filter(
        &self,
        requested: &SubscriptionFilter,
    ) -> Option<SubscriptionFilter> {
        Some(requested.clone())
    }
    async fn listen(&self, context: SubscriptionContext) -> Result<(), ErrorData> {
        let mut changes = self.changes.subscribe();
        loop {
            tokio::select! {
                _ = context.cancelled() => return Ok(()),
                change = changes.recv() => {
                    if change.is_err() || context.sink().notify_tool_list_changed().await.is_err() { return Ok(()); }
                }
            }
        }
    }
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_tool_list_changed()
                .build(),
        )
    }
    async fn discover(&self, _: RequestContext<RoleServer>) -> Result<DiscoverResult, ErrorData> {
        Ok(DiscoverResult::from_server_info(
            self.supported_protocol_versions().into_owned(),
            self.get_info(),
        )
        .with_ttl_ms(self.ttl_ms))
    }
    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        self.metrics.lists.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .inner
            .list_tools(request, context)
            .await?
            .with_ttl_ms(self.ttl_ms))
    }
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        self.metrics.calls.fetch_add(1, Ordering::SeqCst);
        let active = self.metrics.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.metrics.maximum.fetch_max(active, Ordering::SeqCst);
        let _active = Active(self.metrics.clone());
        let message = request
            .arguments
            .as_ref()
            .and_then(|args| args.get("message"))
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        if message == "sdk-response-resume" {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        if message.starts_with("wasi-parallel-") {
            self.metrics.parallel.fetch_add(1, Ordering::SeqCst);
            tokio::time::timeout(Duration::from_secs(5), self.parallel.wait())
                .await
                .expect("all 16 requests must arrive before any response");
        }
        if request.input_responses.is_some() {
            self.metrics.resumes.fetch_add(1, Ordering::SeqCst);
            tokio::time::timeout(Duration::from_secs(5), self.resumes.wait())
                .await
                .expect("all 16 resumes must arrive before any response");
        }
        if message == "wasi-notify" {
            if self.legacy {
                context.peer.notify_tool_list_changed().await.unwrap();
            } else {
                self.changes
                    .send(())
                    .expect("an SDK subscription must be listening");
            }
        }
        if message == "wasi-json-terminal" {
            return Ok(serde_json::from_value::<CallToolResult>(
                json!({"content":[],"structuredContent":request.arguments}),
            )
            .unwrap()
            .into());
        }
        self.inner.call_tool(request, context).await
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<_> = std::env::args().collect();
    let legacy = args.get(1).is_some_and(|value| value == "legacy");
    let json_response = args.get(2).is_some_and(|value| value == "json");
    let metrics = Arc::<Metrics>::default();
    let handler = Server {
        inner: sdk_server::SdkServer(Arc::new(AtomicUsize::new(0))),
        metrics: metrics.clone(),
        parallel: Arc::new(tokio::sync::Barrier::new(16)),
        resumes: Arc::new(tokio::sync::Barrier::new(16)),
        ttl_ms: args.get(3).map_or(60_000, |value| value.parse().unwrap()),
        legacy,
        changes: tokio::sync::broadcast::channel(4).0,
    };
    let cancel = CancellationToken::new();
    let mut transport = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(legacy)
        .with_json_response(json_response)
        .with_cancellation_token(cancel.child_token());
    let mut sessions = LocalSessionManager::default();
    if args
        .get(4)
        .is_some_and(|value| value == "response-recovery")
    {
        transport = transport.with_sse_retry(Some(Duration::from_millis(10)));
        sessions.session_config.sse_retry = Some(Duration::from_millis(10));
    }
    let sessions = Arc::new(sessions);
    let service: StreamableHttpService<Server, LocalSessionManager> =
        StreamableHttpService::new(move || Ok(handler.clone()), sessions.clone(), transport);
    let session_stats = sessions.clone();
    let router = axum::Router::new().nest_service("/mcp", service).route("/stats", axum::routing::get(move || {
        let metrics = metrics.clone();
        let sessions = session_stats.clone();
        async move { axum::Json(json!({"calls":metrics.calls.load(Ordering::SeqCst),"active":metrics.active.load(Ordering::SeqCst),"maximum":metrics.maximum.load(Ordering::SeqCst),"lists":metrics.lists.load(Ordering::SeqCst),"parallel":metrics.parallel.load(Ordering::SeqCst),"resumes":metrics.resumes.load(Ordering::SeqCst),"sessions":sessions.sessions.read().await.len()})) }
    }));
    // Test-only control: terminate real manager sessions, then let the stock SDK
    // generate its own 404 for the next original MCP request.
    let router = if args.get(4).is_some_and(|mode| mode == "session-recovery") {
        router.route(
            "/expire",
            axum::routing::post(move || {
                let sessions = sessions.clone();
                async move {
                    let ids: Vec<_> = sessions.sessions.read().await.keys().cloned().collect();
                    for id in &ids {
                        sessions.close_session(id).await.unwrap();
                    }
                    axum::Json(json!({"closed":ids.len()}))
                }
            }),
        )
    } else {
        router
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    println!(
        "{}",
        json!({"endpoint":format!("http://{}/mcp",listener.local_addr().unwrap())})
    );
    std::io::stdout().flush().unwrap();
    let input = tokio::task::spawn_blocking(|| {
        let mut line = String::new();
        let _ = std::io::stdin().read_line(&mut line);
    });
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = input.await;
            cancel.cancel();
        })
        .await
        .unwrap();
}
