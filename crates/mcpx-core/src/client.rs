use crate::{
    Action, BoundArgs, CallOutcome, Error, HostRuntime, Machine, OutputValidation, Result,
    ToolBinding, ToolResponse,
};
use std::ops::ControlFlow;

#[derive(Clone, Debug)]
pub struct DriverContext {
    pub auth_context_id: String,
}

/// Implementations supply I/O and monotonic time; futures need not be Send.
/// Before feeding an incoming response, advance the runtime clock with `now_ms()`.
#[allow(async_fn_in_trait)]
pub trait Driver {
    /// Opt into metadata-only renewal before a new operation after Core has
    /// observed expiry. Concurrent owners with their own scheduler leave it off.
    fn supports_session_recovery(&self) -> bool {
        false
    }
    /// The client has suspended admission and reset the old epoch. Return a
    /// freshly discovered catalog and an unfinished credential snapshot for the
    /// current operation. `prepared` preserves an already running deadline.
    async fn recover_session(
        &mut self,
        _runtime: &mut HostRuntime,
        _observed_at_ms: u64,
        _prepared: bool,
    ) -> Result<DriverContext> {
        Err(Error::local(
            "CapabilityUnavailable",
            "driver does not support session recovery",
        ))
    }
    fn supports_reconnect(&self) -> bool {
        false
    }
    /// Rebuild only the transport. The client performs discovery afterwards.
    async fn reconnect(&mut self, _runtime: &mut HostRuntime) -> Result<()> {
        Err(Error::local(
            "CapabilityUnavailable",
            "driver does not support reconnect",
        ))
    }
    async fn prepare(&mut self) -> Result<DriverContext>;
    fn now_ms(&self) -> u64;
    async fn execute(&mut self, action: Action, runtime: &mut HostRuntime) -> Result<()>;
    async fn abort(&mut self, _runtime: &mut HostRuntime, _cause: &Error) {}
    fn finish(&mut self) {}
    /// Retain the prepared credential/scope snapshot for this operation until finished.
    fn concurrent_started(&mut self, _operation_id: u64) {}
    fn concurrent_finished(&mut self, _operation_id: u64) {}
    fn collect_events(&mut self, _runtime: &mut HostRuntime) {}
    async fn close(&mut self, _runtime: &mut HostRuntime) -> Result<()> {
        Ok(())
    }
}

pub struct Client<D> {
    runtime: HostRuntime,
    driver: D,
    closed: bool,
    close_complete: bool,
    suspended: bool,
}

#[derive(Debug)]
pub struct CheckedCallError {
    pub error: Box<Error>,
    pub outcome: Option<CallOutcome>,
}

impl std::fmt::Display for CheckedCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}
impl std::error::Error for CheckedCallError {}

impl<D: Driver> Client<D> {
    pub fn new(driver: D, runtime: HostRuntime) -> Self {
        Self {
            runtime,
            driver,
            closed: false,
            close_complete: false,
            suspended: false,
        }
    }
    pub fn with_catalog_ttl(mut self, ttl_ms: u64) -> Self {
        self.runtime.set_catalog_ttl(ttl_ms);
        self
    }
    pub fn set_catalog_ttl(&mut self, ttl_ms: u64) {
        self.runtime.set_catalog_ttl(ttl_ms);
    }
    pub fn machine(&self) -> &Machine {
        self.runtime.machine()
    }
    pub fn driver(&self) -> &D {
        &self.driver
    }
    pub fn driver_mut(&mut self) -> &mut D {
        &mut self.driver
    }
    /// Low-level hosts must supply response data and finish/cancel every admitted operation.
    pub fn host_runtime_mut(&mut self) -> &mut HostRuntime {
        &mut self.runtime
    }
    pub fn host_parts_mut(&mut self) -> (&mut HostRuntime, &mut D) {
        (&mut self.runtime, &mut self.driver)
    }
    pub fn take_concurrent_completion(&mut self, operation_id: u64) -> Option<Result<CallOutcome>> {
        let result = self.runtime.take_completion(operation_id);
        if result.is_some() {
            self.driver.collect_events(&mut self.runtime);
            self.driver.concurrent_finished(operation_id);
        }
        result
    }
    pub fn catalog_needs_refresh(&self) -> bool {
        self.expired()
    }
    /// A host must keep feeding existing calls until this preflight can run exclusively.
    pub fn prepared_needs_barrier(&self, context: &DriverContext) -> bool {
        self.runtime
            .catalog_status_at(&context.auth_context_id, self.driver.now_ms())
            .is_ok_and(|status| status.requires_exclusive)
    }
    pub fn flush_host_events(&mut self) {
        self.driver.collect_events(&mut self.runtime);
    }
    pub fn tool(&mut self, name: &str) -> Result<ToolBinding> {
        self.runtime.tool_binding(name)
    }
    pub fn drain_events(&mut self) -> crate::EventBatch {
        self.runtime.take_events()
    }
    pub async fn set_schema_registry(&mut self, registry: crate::SchemaRegistry) -> Result<()> {
        self.prepare().await?;
        let result = self.runtime.set_schema_registry(registry);
        self.driver.finish();
        result
    }
    pub fn schema_registry_stats(&self) -> crate::SchemaRegistryStats {
        self.runtime.schema_registry_stats()
    }
    pub fn validator_cache_stats(&self) -> crate::ValidatorCacheStats {
        self.runtime.validator_cache_stats()
    }
    pub fn set_validator_cache_capacity(&mut self, capacity: usize) -> Result<()> {
        self.runtime.set_validator_cache_capacity(capacity)
    }
    pub fn clear_validator_cache(&mut self) {
        self.runtime.clear_validator_cache();
    }
    pub fn set_elicitation(&mut self, form: bool, url: bool) -> Result<()> {
        self.runtime.set_elicitation(form, url)
    }
    pub fn release_continuation(&mut self, handle: &crate::Continuation) -> Result<()> {
        self.runtime.machine().check_continuation_owner(handle)?;
        self.runtime.release_continuation(handle.id());
        Ok(())
    }

    async fn prepare(&mut self) -> Result<bool> {
        self.prepare_mode(false).await
    }
    async fn prepare_mode(&mut self, concurrent: bool) -> Result<bool> {
        if self.suspended {
            return Err(Error::local(
                "TransportClosed",
                "reconnect must complete before new operations",
            ));
        }
        if self.closed {
            return Err(Error::local("TransportClosed", "remote is closed"));
        }
        if self.runtime.busy() && (!concurrent || self.machine().active_action_id().is_some()) {
            return Err(Error::local(
                "Busy",
                "an operation is active; cancel or close it before reuse",
            ));
        }
        let preparation = match self.recover_session(false).await {
            Ok(Some(context)) => Ok(context),
            Ok(None) => self.driver.prepare().await,
            Err(error) => Err(error),
        };
        let context = match preparation {
            Ok(context) => context,
            Err(error) => {
                self.driver.finish();
                return Err(error.with_context(crate::ErrorContext::not_sent()));
            }
        };
        let result = self.accept_context(context);
        if result.is_err() {
            self.driver.finish();
        }
        result
    }
    async fn recover_session(&mut self, prepared: bool) -> Result<Option<DriverContext>> {
        let Some(observed) = self
            .runtime
            .machine()
            .http_session_expired_at()
            .filter(|_| self.driver.supports_session_recovery())
        else {
            return Ok(None);
        };
        if self.runtime.busy() {
            return Err(Error::local(
                "Busy",
                "finish active operations before session recovery",
            ));
        }
        self.runtime.reset_transport()?;
        // Reset can reject an unconsumed serial result without changing state.
        // No await separates reset from suspension, so dropped renewal remains
        // suspended while a rejected reset keeps the old owner usable.
        self.suspended = true;
        match self
            .driver
            .recover_session(&mut self.runtime, observed, prepared)
            .await
        {
            Ok(context) => {
                self.suspended = false;
                Ok(Some(context))
            }
            Err(error) => {
                let context = crate::ErrorContext {
                    operation_id: error.operation_id,
                    queue_operation_id: error.queue_operation_id,
                    execution_certainty: crate::ExecutionCertainty::NotSent,
                };
                let error = error.with_context(context);
                self.runtime.abort_error(error.clone());
                self.driver.abort(&mut self.runtime, &error).await;
                Err(error)
            }
        }
    }
    async fn accept_prepared_context(&mut self, context: DriverContext) -> Result<bool> {
        let context = if self.closed || self.suspended {
            context
        } else {
            self.recover_session(true).await?.unwrap_or(context)
        };
        self.accept_context(context)
    }
    fn accept_context(&mut self, context: DriverContext) -> Result<bool> {
        if self.suspended {
            return Err(Error::local(
                "TransportClosed",
                "reconnect must complete before admission",
            ));
        }
        self.apply_context(context)
    }
    fn apply_context(&mut self, context: DriverContext) -> Result<bool> {
        if self.closed {
            return Err(Error::local("TransportClosed", "remote is closed"));
        }
        self.runtime.advance_time(self.driver.now_ms())?;
        self.runtime.accept_authorization(&context.auth_context_id)
    }

    async fn drive(&mut self) -> Result<()> {
        drive_host(&mut self.driver, &mut self.runtime).await
    }

    async fn refresh_prepared(&mut self) -> Result<()> {
        self.runtime.refresh_catalog()?;
        let result = self.drive().await;
        if result.is_err() && self.recover_session(true).await?.is_some() {
            return Ok(());
        }
        result
    }
    fn expired(&self) -> bool {
        self.runtime.catalog_needs_refresh_at(self.driver.now_ms())
    }
    pub fn catalog_revision_source(&self) -> crate::CatalogRevisionSource {
        self.machine().catalog().revision_source()
    }
    pub fn catalog_revision(&self) -> crate::CatalogRevision {
        self.catalog_revision_source().snapshot()
    }
    pub fn subscribe_catalog(&self) -> Result<crate::CatalogWatcher> {
        self.catalog_revision_source().subscribe()
    }
    /// Reuse a newer complete, fresh publication in the same catalog context.
    /// Returns true only if this caller actually performed discovery/list I/O.
    pub async fn refresh_if_unchanged(
        &mut self,
        observed: &crate::CatalogRevision,
    ) -> Result<bool> {
        let source = self.catalog_revision_source();
        source.check_owner(observed)?;
        let changed = self.prepare().await?;
        let result = async {
            if !changed && !self.expired() && source.published_since(observed)? {
                return Ok(false);
            }
            self.refresh_prepared().await?;
            Ok(true)
        }
        .await;
        self.driver.finish();
        result
    }
    pub async fn refresh(&mut self) -> Result<()> {
        self.prepare().await?;
        let result = self.refresh_prepared().await;
        self.driver.finish();
        result
    }
    pub async fn fresh_tool(&mut self, name: &str) -> Result<ToolBinding> {
        self.prepare().await?;
        let result = async {
            if self.expired() {
                self.refresh_prepared().await?;
            }
            self.tool(name)
        }
        .await;
        self.driver.finish();
        result
    }
    /// Resolves authorization and refreshes an expired catalog before a local read.
    /// Opt-in session renewal can replace the credential snapshot before the read.
    /// The closure cannot perform transport I/O or retain a borrow of this client.
    pub async fn with_fresh_catalog<T>(
        &mut self,
        read: impl FnOnce(&Machine) -> Result<T>,
    ) -> Result<T> {
        self.prepare().await?;
        let result = async {
            if self.expired() {
                self.refresh_prepared().await?;
            }
            read(self.machine())
        }
        .await;
        self.driver.finish();
        result
    }
    /// Read a fresh catalog using a context already resolved by `Driver::prepare`.
    /// Hosts must pass that operation's unchanged context and drain existing
    /// operations first. This consumes the prepared snapshot and finishes it on
    /// every return path. Opt-in session renewal replaces it with fresh credentials.
    pub async fn with_prepared_catalog<T>(
        &mut self,
        context: DriverContext,
        read: impl FnOnce(&Machine) -> Result<T>,
    ) -> Result<T> {
        let result = async {
            if self.runtime.busy() {
                return Err(Error::local(
                    "Busy",
                    "catalog read requires an idle runtime",
                ));
            }
            self.accept_prepared_context(context).await?;
            if self.expired() {
                self.refresh_prepared().await?;
            }
            read(self.machine())
        }
        .await;
        self.driver.finish();
        result
    }
    pub async fn call(&mut self, binding: &ToolBinding, args: &BoundArgs) -> Result<CallOutcome> {
        binding.check_args(args)?;
        let changed = self.prepare().await?;
        let result = async {
            if changed {
                return Err(Error::local(
                    "BindingStale",
                    "authorization changed; refresh and bind the tool again",
                ));
            }
            if self.expired() {
                self.refresh_prepared().await?;
            }
            self.runtime.call_bound(binding, args)?;
            self.drive().await?;
            self.runtime
                .take_outcome()
                .ok_or_else(|| Error::local("ProtocolError", "call completed without an outcome"))
        }
        .await;
        self.driver.finish();
        result
    }
    pub async fn call_json(
        &mut self,
        binding: &ToolBinding,
        value: serde_json::Value,
    ) -> Result<CallOutcome> {
        let args = binding.validate_json(value)?;
        self.call(binding, &args).await
    }
    /// Caller-owned Serialize arguments, with the same Schema/owner/preflight
    /// checks as JSON and builder calls. Business/interactive states stay explicit.
    pub async fn call_typed(
        &mut self,
        binding: &ToolBinding,
        value: impl serde::Serialize,
    ) -> Result<CallOutcome> {
        let args = binding.args_from(value)?;
        self.call(binding, &args).await
    }
    async fn build_tool_call<T>(
        &mut self,
        name: &str,
        build: impl FnOnce(&ToolBinding) -> Result<ControlFlow<T, BoundArgs>>,
    ) -> Result<ControlFlow<T, (ToolBinding, BoundArgs)>> {
        if self.expired() {
            if self.runtime.busy() {
                return Err(Error::local(
                    "Busy",
                    "wait for in-flight calls before refreshing the catalog",
                ));
            }
            self.refresh_prepared().await?;
        }
        let binding = self.tool(name)?;
        match build(&binding)? {
            ControlFlow::Break(value) => Ok(ControlFlow::Break(value)),
            ControlFlow::Continue(args) => {
                binding.check_args(&args)?;
                Ok(ControlFlow::Continue((binding, args)))
            }
        }
    }
    /// Prepare credentials, bind the fresh remote schema, and build arguments locally.
    /// Opt-in session renewal can replace credentials before the builder runs.
    /// `Break` returns an immediate value (for example help) without a business call.
    pub async fn call_tool_with<T>(
        &mut self,
        name: &str,
        build: impl FnOnce(&ToolBinding) -> Result<ControlFlow<T, BoundArgs>>,
    ) -> Result<ControlFlow<T, CallOutcome>> {
        self.prepare().await?;
        let result = async {
            let (binding, args) = match self.build_tool_call(name, build).await? {
                ControlFlow::Break(value) => return Ok(ControlFlow::Break(value)),
                ControlFlow::Continue(pair) => pair,
            };
            self.runtime.call_bound(&binding, &args)?;
            self.drive().await?;
            Ok(ControlFlow::Continue(
                self.runtime.take_outcome().ok_or_else(|| {
                    Error::local("ProtocolError", "call completed without an outcome")
                })?,
            ))
        }
        .await;
        self.driver.finish();
        result
    }
    /// Admit a named tool using an externally resolved preflight. Opt-in session
    /// renewal replaces that snapshot before building arguments when necessary.
    pub async fn admit_prepared_tool<T>(
        &mut self,
        context: DriverContext,
        name: &str,
        build: impl FnOnce(&ToolBinding) -> Result<ControlFlow<T, BoundArgs>>,
    ) -> Result<ControlFlow<T, u64>> {
        let result = async {
            self.accept_prepared_context(context).await?;
            match self.build_tool_call(name, build).await? {
                ControlFlow::Break(value) => Ok(ControlFlow::Break(value)),
                ControlFlow::Continue((binding, args)) => self
                    .runtime
                    .call_concurrent_bound(&binding, &args)
                    .map(ControlFlow::Continue),
            }
        }
        .await;
        if let Ok(ControlFlow::Continue(operation)) = result {
            self.driver.concurrent_started(operation);
        }
        self.driver.finish();
        result
    }
    pub async fn begin_concurrent_call(
        &mut self,
        binding: &ToolBinding,
        args: &BoundArgs,
    ) -> Result<u64> {
        binding.check_args(args)?;
        let changed = self.prepare_mode(true).await?;
        self.admit_concurrent(binding, args, changed).await
    }
    /// Complete preflight after the host has resolved `Driver::prepare` while feeding other I/O.
    pub async fn admit_prepared_call(
        &mut self,
        context: DriverContext,
        binding: &ToolBinding,
        args: &BoundArgs,
    ) -> Result<u64> {
        let changed = match self.accept_prepared_context(context).await {
            Ok(changed) => changed,
            Err(error) => {
                self.driver.finish();
                return Err(error);
            }
        };
        self.admit_concurrent(binding, args, changed).await
    }
    async fn admit_concurrent(
        &mut self,
        binding: &ToolBinding,
        args: &BoundArgs,
        changed: bool,
    ) -> Result<u64> {
        let result = async {
            if changed {
                return Err(Error::local(
                    "BindingStale",
                    "authorization changed; bind the tool again",
                ));
            }
            if self.expired() {
                if self.runtime.busy() {
                    return Err(Error::local(
                        "Busy",
                        "wait for in-flight calls before refreshing the catalog",
                    ));
                }
                self.refresh_prepared().await?;
            }
            self.runtime.call_concurrent_bound(binding, args)
        }
        .await;
        if let Ok(operation_id) = result {
            self.driver.concurrent_started(operation_id);
        }
        self.driver.finish();
        result
    }
    /// Explicitly continue one pending interaction. No tool executes automatically.
    pub async fn resume(
        &mut self,
        handle: &crate::Continuation,
        responses: impl serde::Serialize,
    ) -> Result<CallOutcome> {
        self.machine().check_continuation_owner(handle)?;
        let responses = crate::serialize_value(responses)?;
        let changed = self.prepare().await?;
        let result = async {
            if changed {
                return Err(Error::local(
                    "InvalidContinuation",
                    "authorization changed; continuation was invalidated",
                ));
            }
            self.machine().check_continuation(handle)?;
            if self.expired() {
                self.refresh_prepared().await?;
            }
            self.runtime.resume_bound(handle, responses)?;
            self.drive().await?;
            self.runtime
                .take_outcome()
                .ok_or_else(|| Error::local("ProtocolError", "resume completed without an outcome"))
        }
        .await;
        self.driver.finish();
        result
    }
    /// Concurrent continuation admission after one externally resolved credential preflight.
    pub async fn admit_prepared_resume(
        &mut self,
        context: DriverContext,
        handle: &crate::Continuation,
        responses: serde_json::Value,
    ) -> Result<u64> {
        let result = async {
            self.machine().check_continuation_owner(handle)?;
            if self.accept_prepared_context(context).await? {
                return Err(Error::local(
                    "InvalidContinuation",
                    "authorization changed; continuation was invalidated",
                ));
            }
            self.machine().check_continuation(handle)?;
            if self.expired() {
                if self.runtime.busy() {
                    return Err(Error::local(
                        "Busy",
                        "wait for in-flight calls before refreshing the catalog",
                    ));
                }
                self.refresh_prepared().await?;
            }
            self.runtime.resume_concurrent_bound(handle, responses)
        }
        .await;
        if let Ok(operation) = result {
            self.driver.concurrent_started(operation);
        }
        self.driver.finish();
        result
    }
    pub async fn call_checked(
        &mut self,
        binding: &ToolBinding,
        args: &BoundArgs,
    ) -> std::result::Result<ToolResponse, CheckedCallError> {
        let outcome = self
            .call(binding, args)
            .await
            .map_err(|error| CheckedCallError {
                error: Box::new(error),
                outcome: None,
            })?;
        let code = match &outcome {
            CallOutcome::NeedsInput(_) => Some("NeedsInput"),
            CallOutcome::Completed(response) if response.is_error => Some("ToolFailure"),
            CallOutcome::Completed(response)
                if matches!(response.output_validation, OutputValidation::Indeterminate) =>
            {
                Some("OutputValidationLimitExceeded")
            }
            CallOutcome::Completed(response)
                if matches!(response.output_validation, OutputValidation::Invalid) =>
            {
                Some("OutputValidation")
            }
            _ => None,
        };
        if let Some(code) = code {
            let context = match &outcome {
                CallOutcome::Completed(response) => response.error_context(),
                CallOutcome::NeedsInput(handle) => crate::ErrorContext {
                    operation_id: Some(handle.operation_id()),
                    queue_operation_id: handle.queue_operation_id(),
                    execution_certainty: crate::ExecutionCertainty::NeedsInput,
                },
            };
            return Err(CheckedCallError {
                error: Box::new(
                    Error::local(
                        code,
                        "tool did not produce a successful schema-valid result",
                    )
                    .with_context(context),
                ),
                outcome: Some(outcome),
            });
        }
        let CallOutcome::Completed(response) = outcome else {
            unreachable!()
        };
        Ok(response)
    }
    pub async fn cancel_active(&mut self) {
        self.abort_active(Error::local(
            "Cancelled",
            "operation cancelled; execution may be unknown",
        ))
        .await;
    }
    /// Clean up a dropped operation future with its actual terminal cause.
    pub async fn abort_active(&mut self, cause: Error) {
        if self.runtime.busy() {
            if cause.code == "Cancelled" {
                self.runtime.cancel();
            } else {
                self.runtime.abort_error(cause.clone());
            }
            self.driver.abort(&mut self.runtime, &cause).await;
            self.runtime.take_actions();
        }
        self.driver.finish();
    }
    pub fn is_closed(&self) -> bool {
        self.closed
    }
    pub fn is_suspended(&self) -> bool {
        self.suspended
    }
    /// Explicit, metadata-only recovery. Never replays calls or consumes old continuations.
    /// Failed/dropped recovery keeps public operations suspended; retry recovery or close.
    pub async fn reconnect(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::local("TransportClosed", "remote is closed"));
        }
        if !self.driver.supports_reconnect() {
            return Err(Error::local(
                "CapabilityUnavailable",
                "driver does not support reconnect",
            ));
        }
        if self.runtime.busy()
            && (!self.suspended || self.runtime.machine().has_business_operation())
        {
            return Err(Error::local(
                "Busy",
                "finish or cancel active operations before reconnecting",
            ));
        }
        self.suspended = true;
        if self.runtime.busy() {
            self.abort_active(Error::local("Cancelled", "abandoned reconnect cancelled"))
                .await;
        }
        self.runtime.reset_transport()?;
        let mut driving = false;
        let result = async {
            self.driver.reconnect(&mut self.runtime).await?;
            let context = self.driver.prepare().await?;
            self.apply_context(context)?;
            self.runtime.refresh_catalog()?;
            driving = true;
            self.drive().await
        }
        .await;
        let result = result.map_err(|error| {
            let context = crate::ErrorContext {
                operation_id: error.operation_id,
                queue_operation_id: error.queue_operation_id,
                execution_certainty: crate::ExecutionCertainty::NotSent,
            };
            error.with_context(context)
        });
        if let Err(error) = &result
            && !driving
        {
            self.driver.abort(&mut self.runtime, error).await;
        }
        self.driver.finish();
        if result.is_ok() {
            self.suspended = false;
        }
        result
    }
    pub async fn close(&mut self) -> Result<()> {
        self.runtime.cancel_notification_context("TransportClosed");
        if self.close_complete {
            return Ok(());
        }
        if !self.closed {
            self.closed = true;
            self.machine().catalog().close_watchers();
            self.cancel_active().await;
        }
        // Closing blocks new operations immediately; failed or dropped cleanup
        // remains explicitly retryable instead of becoming a false success.
        self.driver.close(&mut self.runtime).await?;
        self.close_complete = true;
        Ok(())
    }
}

/// Drive an already admitted serial operation without preparing credentials,
/// finishing their snapshot, or retrying a request. Drivers may use this for
/// metadata-only recovery; the owner retains admission and suspension control.
pub async fn drive_host<D: Driver>(driver: &mut D, runtime: &mut HostRuntime) -> Result<()> {
    while runtime.busy() {
        let actions = runtime.take_actions();
        if actions.is_empty() {
            let error = Error::new("ProtocolError", "active operation has no host action");
            let context = runtime
                .machine()
                .active_action_id()
                .and_then(|id| runtime.action_error_context(id));
            let error = context.map_or_else(
                || error.clone(),
                |context| error.clone().with_context(context),
            );
            runtime.abort_error(error.clone());
            driver.abort(runtime, &error).await;
            return Err(error);
        }
        for action in actions {
            let context = match &action {
                crate::Action::Http { id, .. } | crate::Action::Stream { id, .. } => {
                    runtime.machine().action_error_context(*id)
                }
                crate::Action::Cancel { .. } => None,
            };
            let result = match runtime.advance_time(driver.now_ms()) {
                Ok(()) => driver.execute(action, runtime).await,
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                let error = context.map_or_else(
                    || error.clone(),
                    |context| error.clone().with_context(context),
                );
                if error.code == "Cancelled" {
                    runtime.cancel();
                } else {
                    runtime.abort_error(error.clone());
                }
                driver.abort(runtime, &error).await;
                return Err(error);
            }
        }
    }
    Ok(())
}
