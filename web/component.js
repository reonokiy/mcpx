function hostId(value) {
  if (typeof value === "bigint" && value > 0n && value <= 9_007_199_254_740_991n) return value;
  if (typeof value === "number" && Number.isSafeInteger(value) && value > 0) return BigInt(value);
  throw Object.assign(new Error("Handle must be a positive exact host integer"), { code: "InvalidHandle" });
}

const text = new TextDecoder("utf-8", { fatal: true });
const json = JSON.stringify;
function actionJson(action) {
  if (action.tag === "cancel") return `{"kind":"cancel","id":${action.val}}`;
  const value = action.val;
  const body = json(text.decode(value.body));
  const metadata = `"rpc_id":${value.rpcId ?? "null"},"rpc_method":${json(value.rpcMethod)},"operation_id":${value.operationId},"cancellation":${value.cancellation == null ? "null" : json(text.decode(value.cancellation))}`;
  if (action.tag === "stream") return `{"kind":"stream","id":${value.actionId},${metadata},"body":${body}}`;
  return `{"kind":"http","id":${value.actionId},${metadata},"method":${json(value.method.toUpperCase())},"url":${json(value.url)},"headers":${json(Object.fromEntries(value.headers.map(header => [header.name, header.value])))},"body":${body}}`;
}
function outcomeJson(outcome) {
  if (outcome === undefined || outcome === null) return undefined;
  const value = outcome.val;
  const metadata = `"operation_id":${value.operationId ?? "null"},"queue_operation_id":${value.queueOperationId ?? "null"},`;
  if (outcome.tag === "completed") return `{"kind":"completed","value":{${metadata}"raw":${value.rawJson},"is_error":${value.isError},"output_validation":${json(value.outputValidation.replaceAll("-", "_"))}}}`;
  return `{"kind":"needs_input","value":{${metadata}"id":${value.id},"expires_at_ms":${value.expiresAtMs},"raw":${value.rawJson}}}`;
}
function eventJson(value) {
  const kind = value.kind.tag === "extension" ? value.kind.val : value.kind.tag.replaceAll("-", "_");
  const queue = value.queueOperationId == null ? "" : `,"queue_operation_id":${value.queueOperationId}`;
  return `{"operation_id":${value.operationId},"sequence":${value.sequence}${queue},"kind":${json(kind)},"payload_json":${json(value.payloadJson)}}`;
}
function toolJson(value) {
  const extension = value.extensionsJson.trim().slice(1, -1).trim();
  return `{${extension ? extension + "," : ""}"name":${json(value.name)},"description":${json(value.description ?? null)},"inputSchema":${value.inputSchemaJson},"outputSchema":${value.outputSchemaJson ?? "null"}}`;
}
function hostError(error) {
  if (error instanceof Error || typeof error?.code !== "string") return error;
  return Object.assign(new Error(error.message), {
    code: error.code,
    operation_id: error.operationId == null ? undefined : Number(hostId(error.operationId)),
    queue_operation_id: error.queueOperationId == null ? undefined : Number(hostId(error.queueOperationId)),
    execution_certainty: error.executionCertainty?.replaceAll("-", "_"),
    retry_advice: error.retryAdvice?.replaceAll("-", "_"),
    issues: error.issues.map(issue => ({ instance_path: issue.instancePath, schema_path: issue.schemaPath })),
  });
}
function responseHeaders(value) {
  const invalid = () => Object.assign(new Error("Invalid response header JSON"), { code: "InvalidJson" });
  if (typeof value !== "string") throw invalid();
  if (value.length > 65536 || new TextEncoder().encode(value).byteLength > 65536) throw Object.assign(new Error("Response headers exceed byte limit"), { code: "LimitExceeded" });
  let fields;
  try { fields = JSON.parse(value); } catch { throw invalid(); }
  if (!fields || Array.isArray(fields) || typeof fields !== "object" || Object.values(fields).some(field => typeof field !== "string")) throw invalid();
  return Object.entries(fields).map(([name, value]) => ({ name, value }));
}

// Fixed host ABI adapter; this does not generate bindings for remote tools.
function notificationPlan(r) {
  return JSON.stringify({resource_id:Number(hostId(r.resourceId)),rpc_id:r.rpcId==null?null:Number(hostId(r.rpcId)),method:r.method.toUpperCase(),url:r.url,headers:Object.fromEntries(r.headers.map(h=>[h.name,h.value])),body:r.body==null?null:JSON.parse(text.decode(r.body))});
}
export function componentRuntime(component) {
  const api = {
    enable_response_resumption: id => component.enableResponseResumption(hostId(id)),
    interrupt_response_for: id => component.interruptResponseFor(hostId(id)),
    response_resume: id => {
      const info=component.responseResume(hostId(id)),delay=Number(info.retryAfterMs);
      if(!Number.isSafeInteger(delay)||delay<0)throw Object.assign(new Error("Invalid response retry delay"),{code:"LimitExceeded"});
      return JSON.stringify({action_id:Number(hostId(info.actionId)),rpc_id:Number(hostId(info.rpcId)),operation_id:Number(hostId(info.operationId)),retry_after_ms:delay});
    },
    resume_response: id => {
      const r=component.resumeResponse(hostId(id));
      return JSON.stringify({action_id:Number(hostId(r.actionId)),rpc_id:Number(hostId(r.rpcId)),operation_id:Number(hostId(r.operationId)),method:r.method.toUpperCase(),url:r.url,headers:Object.fromEntries(r.headers.map(h=>[h.name,h.value])),body:r.body==null?null:JSON.parse(text.decode(r.body))});
    },
    response_replies: id => `[${component.responseReplies(hostId(id)).join(",")}]`,
    release_response_resumption: id => component.releaseResponseResumption(hostId(id)),
    exhaust_response_resumption: id => component.exhaustResponseResumption(hostId(id)),
    start_notifications: () => notificationPlan(component.startNotifications()),
    resume_notifications: id => notificationPlan(component.resumeNotifications(hostId(id))),
    notification_resume: id => {
      const info=component.notificationResume(hostId(id));
      const delay=Number(info.retryAfterMs);
      if (!Number.isSafeInteger(delay)||delay<0)throw Object.assign(new Error("Invalid notification retry delay"),{code:"LimitExceeded"});
      return JSON.stringify({resource_id:Number(hostId(info.resourceId)),retry_after_ms:delay});
    },
    notification_status: () => {
      const s=component.notificationStatus();
      return JSON.stringify(s==null?null:{resource_id:Number(hostId(s.resourceId)),rpc_id:s.rpcId==null?null:Number(hostId(s.rpcId)),phase:s.phase,error_code:s.errorCode??null});
    },
    begin_notifications: (id,status,mime) => {
      if (!Number.isSafeInteger(status)||status<100||status>599) throw Object.assign(new Error("Invalid notification HTTP status"),{code:"InvalidResponse"});
      component.beginNotifications(hostId(id),status,mime);
    },
    push_notifications: (id,bytes) => component.pushNotifications(hostId(id),bytes),
    finish_notifications: id => component.finishNotifications(hostId(id)),
    stop_notifications: id => component.stopNotifications(hostId(id)),
    abort_notifications: (id,timeout) => component.abortNotifications(hostId(id),timeout),
    notification_replies: id => `[${component.notificationReplies(hostId(id)).join(",")}]`,
    catalog_change: () => {
      const change = component.catalogChange();
      return JSON.stringify(change.tag === "changed" ? {kind:"changed", ...change.val} : {kind:change.tag});
    },
    discover: () => component.discover(), bind: name => component.bind(name),
    tools: () => `[${component.tools().map(toolJson).join(",")}]`,
    set_schema_registry: text => {
      if (typeof text !== "string") throw Object.assign(new Error("schema bundle must be JSON text"), { code:"ConfigurationError" });
      if (new TextEncoder().encode(text).byteLength > 16 * 1024 * 1024) throw Object.assign(new Error("schema bundle exceeds 16 MiB"), { code:"LimitExceeded" });
      let resources;
      try { resources = JSON.parse(text); } catch { throw Object.assign(new Error("schema bundle must be a list of uri/schema_json string records"), { code:"ConfigurationError" }); }
      if (!Array.isArray(resources) || resources.some(r => !r || typeof r.uri !== "string" || typeof r.schema_json !== "string" || Object.keys(r).some(key => !["uri", "schema_json"].includes(key)))) throw Object.assign(new Error("schema resources require uri/schema_json string records"), { code: "ConfigurationError" });
      if (resources.length > 128) throw Object.assign(new Error("schema registry exceeds 128 resources"), { code:"LimitExceeded" });
      component.setSchemaRegistry(resources.map(r => ({ uri: r.uri, schemaJson: r.schema_json })));
    },
    schema_registry_stats: () => { const stats = component.schemaRegistryStats(); return `{"resources":${stats.resources},"bytes":${stats.bytes},"schema_nodes":${stats.schemaNodes},"patterns":${stats.patterns},"pattern_bytes":${stats.patternBytes}}`; },
    validator_cache_stats: () => { const s=component.validatorCacheStats(); return `{"capacity":${s.capacity},"entries":${s.entries},"hits":${s.hits},"misses":${s.misses},"compilations":${s.compilations},"evictions":${s.evictions},"counters_saturated":${s.countersSaturated}}`; },
    set_validator_cache_capacity: capacity => {
      if (!Number.isSafeInteger(capacity) || capacity<0 || capacity>4096) throw Object.assign(new Error("validator cache capacity must be 0..4096"),{code:"ConfigurationError"});
      component.setValidatorCacheCapacity(capacity);
    },
    clear_validator_cache: () => component.clearValidatorCache(),
    command_spec: tool => {
      const spec = component.commandSpec(tool);
      return JSON.stringify({ tool: spec.tool, description: spec.description, json_only: spec.jsonOnly,
        parameters: spec.parameters.map(p => ({ name: p.name, flag: p.flag, description: p.description, value_kind: p.valueKind, required: p.required, values: p.values })) });
    },
    complete_line: (line, cursor) => {
      if (!Number.isSafeInteger(cursor) || cursor < 0 || cursor > 65536) throw Object.assign(new Error("completion cursor must be a UTF-8 byte boundary"), { code: "InvalidCursor" });
      return JSON.stringify(component.completeLine(line, cursor));
    },
    endpoint: () => component.endpoint(),
    set_catalog_ttl: ttl => component.setCatalogTtl(BigInt(ttl)),
    catalog_status: scope => {
      const value = component.catalogStatus(scope);
      return { authorization_changed: value.authorizationChanged, needs_refresh: value.needsRefresh, requires_exclusive: value.requiresExclusive };
    },
    accept_authorization: scope => component.acceptAuthorization(scope),
    refresh_catalog: () => component.refreshCatalog(),
    reset_transport: () => component.resetTransport(),
    http_session_status: () => {
      const status=component.httpSessionStatus();
      const at=status.expiredAtMs==null?null:Number(status.expiredAtMs);
      if(at!==null&&(!Number.isSafeInteger(at)||at<0))throw Object.assign(new Error("Session clock exceeds host range"),{code:"LimitExceeded"});
      return JSON.stringify({expired_at_ms:at,last_tool_result:status.lastToolResult==null?null:String(status.lastToolResult)});
    },
    advance_time: now => component.advanceTime(BigInt(now)),
    set_elicitation: (form, url) => component.setElicitation(form, url),
    resume: (id, responses) => component.resume(hostId(id), responses),
    resume_concurrent: (id, responses) => component.resumeConcurrent(hostId(id), responses),
    release_continuation: id => component.releaseContinuation(hostId(id)),
    release_binding: id => component.releaseBinding(hostId(id)),
    validate: (id, args) => component.validate(hostId(id), args), call: (id, args) => component.call(hostId(id), args),
    call_concurrent: (id, args) => component.callConcurrent(hostId(id), args),
    completion: operation => outcomeJson(component.completion(hostId(operation))),
    cancel_operation: operation => component.cancelOperation(hostId(operation)),
    command: (tool, argv) => component.command(tool, JSON.parse(argv)),
    command_line: line => component.commandLine(line),
    command_concurrent: (tool, argv) => {
      const value = component.commandConcurrent(tool, JSON.parse(argv));
      return { kind: value.tag, value: value.val };
    },
    command_line_concurrent: line => {
      const value = component.commandLineConcurrent(line);
      return { kind: value.tag, value: value.val };
    },
    actions: () => `[${component.actions().map(actionJson).join(",")}]`,
    events: () => { const batch = component.events(); return `{"events":[${batch.events.map(eventJson).join(",")}],"dropped":${batch.dropped}}`; },
    set_event_limits: (capacity, maxBytes) => {
      if (!Number.isInteger(capacity) || capacity < 4 || capacity > 4096 || !Number.isInteger(maxBytes) || maxBytes < 49152 || maxBytes > 67108864) throw Object.assign(new Error("Invalid event limits"), { code: "ConfigurationError" });
      return component.setEventLimits(capacity, maxBytes);
    },
    event_stats: () => { const stats = component.eventStats(); return `{"records":${stats.records},"bytes":${stats.bytes},"reserved_terminals":${stats.reservedTerminals},"dropped":${stats.dropped},"sequence":${stats.sequence}}`; },
    outcome: () => outcomeJson(component.outcome()) ?? "null",
    busy: () => component.busy(), cancel: () => component.cancel(), fail: message => component.fail(message),
    fail_request: (id, message) => component.failRequest(hostId(id), message),
    timeout_request: id => component.timeoutRequest(hostId(id)),
    begin_response: (id, status, headers) => {
      if (!Number.isInteger(status) || status < 100 || status > 599) throw Object.assign(new Error("HTTP status must be in 100..599"), { code: "InvalidResponse" });
      return component.beginResponse(hostId(id), status, responseHeaders(headers));
    },
    push_response: bytes => component.pushResponse(bytes), finish_response: () => component.finishResponse(),
    push_response_for: (id, bytes) => component.pushResponseFor(hostId(id), bytes),
    finish_response_for: id => component.finishResponseFor(hostId(id)),
  };
  return Object.fromEntries(Object.entries(api).map(([name, run]) => [name, (...args) => {
    try { return run(...args); } catch (error) { throw hostError(error); }
  }]));
}
