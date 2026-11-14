use rmcp::{
    ErrorData, RoleServer, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, InputRequiredResult,
        ListToolsResult, PaginatedRequestParams, ProgressNotificationParam, ServerCapabilities,
        ServerConfig, SubscriptionFilter,
    },
    service::{RequestContext, SubscriptionContext},
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone)]
pub struct SdkServer(pub Arc<AtomicUsize>);
impl ServerHandler for SdkServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_tool_list_changed()
                .build(),
        )
    }
    fn accepted_subscription_filter(
        &self,
        requested: &SubscriptionFilter,
    ) -> Option<SubscriptionFilter> {
        Some(requested.clone())
    }
    async fn listen(&self, context: SubscriptionContext) -> Result<(), ErrorData> {
        let mut previous = self.0.load(Ordering::SeqCst);
        let mut ticks = tokio::time::interval(std::time::Duration::from_millis(10));
        loop {
            tokio::select! {
                _ = context.cancelled() => return Ok(()),
                _ = ticks.tick() => {
                    let current = self.0.load(Ordering::SeqCst);
                    if current != previous {
                        if context.sink().notify_tool_list_changed().await.is_err() { return Ok(()); }
                        previous = current;
                    }
                }
            }
        }
    }
    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(serde_json::from_value(json!({"tools":[{"name":"echo","inputSchema":{"type":"object","required":["message"],"properties":{"message":{"type":"string"}},"additionalProperties":false},"outputSchema":{"type":"object","required":["message"]}}]})).unwrap())
    }
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let progress = context
            .meta
            .get_progress_token()
            .expect("mcpx requests progress explicitly");
        context
            .peer
            .notify_progress(ProgressNotificationParam::new(progress, 1.0))
            .await
            .unwrap();
        self.0.fetch_add(1, Ordering::SeqCst);
        let args = request.arguments.as_ref().unwrap();
        // Leave a real request-scoped SDK route live for response-resume tests.
        if args["message"] == "SDK response recovery" {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        if args["message"] == "SDK subscription change"
            && context
                .protocol_version()
                .is_some_and(|version| version.has_initialize())
        {
            context.peer.notify_tool_list_changed().await.unwrap();
        }
        if args["message"] == "interactive" {
            if request.input_responses.is_none() {
                return Ok(InputRequiredResult::new(Some(serde_json::from_value(json!({"confirm":{"method":"elicitation/create","params":{"message":"Proceed?","requestedSchema":{"type":"object","properties":{"yes":{"type":"boolean"}},"required":["yes"]}}}})).unwrap()), Some("SDK opaque state".into())).into());
            }
            assert_eq!(request.request_state.as_deref(), Some("SDK opaque state"));
            assert_eq!(
                request.input_responses.as_ref().unwrap()["confirm"],
                json!({"action":"accept","content":{"yes":true}})
            );
        }
        Ok(serde_json::from_value::<CallToolResult>(
            json!({"content":[{"type":"text","text":args["message"]}],"structuredContent":args}),
        )
        .unwrap()
        .into())
    }
}
