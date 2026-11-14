import { contextError } from "./errors.js";
import init, { Runtime } from "../dist/web/mcpx_web.js";
import { Remote } from "./client.js";
import { WorkerEvents } from "./worker-events.js";
import { WorkerCatalog } from "./worker-catalog.js";
import { WorkerFetch } from "./worker-fetch.js";
import { CredentialPort } from "./worker-credentials.js";
import { VERSION, monotonicNow, failure, encodeError, integer } from "./worker-protocol.js";

let runtime, remote, events, initializing, closing;
let lastRequest = 0, nextBinding = 1, inputBytes = 0, normalCount = 0, cleanupCount = 0, cleanupBytes = 0, resumeCount = 0;
let capacity = 64, byteLimit = 8 * 1024 * 1024;
const bindings = new Map(), operations = new Map(), links = new Map();
let resetBindingCutoff=1,transportEpoch=0;
const startup = new AbortController();
const post = (message, transfer = []) => self.postMessage(message, transfer);
const io = new WorkerFetch(post);
const credentials = new CredentialPort(post);
const catalog = new WorkerCatalog(post);
const reply = (id, value) => post({ v: VERSION, kind: "result", id, value, context: operations.get(id)?.context,epoch:operations.get(id)?.transportEpoch });
const errorReply = (id, error) => {
  const operation = operations.get(id);
  const certainty = operation?.promise && ["call", "execute", "execute_line", "resume"].includes(operation.method) ? "unknown" : "not_sent";
  post({ v: VERSION, kind: "error", id, error: encodeError(contextError(error, { queueOperationId: id, certainty })) });
};

async function connect(payload, operation) {
  if (initializing || runtime) throw failure("AlreadyConnected", "Worker already has a remote");
  const { endpoint, protocol = "auto", wasmURL, options = {} } = payload;
  if (!["auto", "modern", "legacy"].includes(protocol)) throw failure("InvalidConfig", "Unsupported protocol mode");
  capacity = integer(options.queueCapacity ?? 64, 1, 1024, "queueCapacity");
  byteLimit = integer(options.maxInputBytes ?? 8 * 1024 * 1024, 1024, 64 * 1024 * 1024, "maxInputBytes");
  events = new WorkerEvents(post, options);
  initializing = (async () => {
    let bytes;
    try {
      const response = await io.fetch(wasmURL ?? new URL("../dist/web/mcpx_web_bg.wasm", import.meta.url), { signal: startup.signal, maxResponseBytes: 64 * 1024 * 1024 });
      if (!response.ok) throw new Error();
      bytes = await response.arrayBuffer();
      await init({ module_or_path: bytes });
    } catch {
      if (startup.signal.aborted) throw operation.cause ?? failure("TransportClosed", "Worker startup interrupted");
      throw failure("WasmLoadError", "Could not load the mcpx WebAssembly runtime");
    }
    if (closing || operation.cause) throw operation.cause ?? failure("TransportClosed", "Worker is closing");
    runtime = protocol === "auto" ? Runtime.auto(endpoint) : new Runtime(endpoint, protocol === "legacy");
    remote = new Remote(runtime, {
      ...options,
      credentialProvider: request => credentials.resolve(request),
      fetch: (url, init) => io.fetch(url, init),
      eventGate: signal => events.wait(signal),
      onCatalogChange: change => catalog.change(change),
      onTransportReset: (id,{automatic}) => {
        if(transportEpoch>=Number.MAX_SAFE_INTEGER)throw failure("LimitExceeded","Worker transport epochs exhausted");
        for(const binding of bindings.values())binding.dispose();bindings.clear();
        resetBindingCutoff=nextBinding;
        transportEpoch++;
        post({v:VERSION,kind:"transport_reset",id:automatic?null:links.get(id),epoch:transportEpoch,automatic});
      },
      onEvent: event => {
        const request = links.get(event.queue_operation_id);
        const operation = operations.get(request);
        if (operation && ["completed", "tool_failed", "needs_input"].includes(event.kind)) {
          operation.transportEpoch=transportEpoch;
          operation.context = { operation_id: event.operation_id, execution_certainty: event.kind === "needs_input" ? "needs_input" : "result_received", is_error:event.payload?.is_error,output_validation:event.payload?.output_validation };
        }
        events.push(request === undefined ? event : { ...event, worker_operation_id: event.queue_operation_id, queue_operation_id: request, payload: ["failed","cancelled"].includes(event.kind) && typeof event.payload?.code === "string" ? {...event.payload,queue_operation_id:request} : event.payload });
      },
      onEventError: () => { void fatal(failure("ConsumerLagged", "Worker could not retain a terminal event")); },
    });
    const remaining = Math.ceil(operation.deadline - monotonicNow());
    if (remaining <= 0) throw failure("Timeout", "Worker connection expired before discovery");
    operation.promise = remote.connect({ timeout: remaining });
    await operation.promise;
  })();
  await initializing;
  return null;
}

function requireRemote() {
  if (!remote || closing) throw failure("TransportClosed", "Worker remote is unavailable or closing");
  return remote;
}
function binding(id) {
  const value = bindings.get(id);
    if (!value) throw failure(Number.isSafeInteger(id)&&id>0&&id<resetBindingCutoff?"BindingStale":"InvalidHandle", "Worker binding is unavailable");
  return value;
}
async function dispatch(method, payload, operation) {
  operation.transportEpoch=transportEpoch;
  post({v:VERSION,kind:"operation_started",id:operation.id});
  if (method === "connect") return connect(payload, operation);
  const value = requireRemote();
  if(!["reconnect","stop_notifications","notification_status","resume_notifications","release_binding","release_continuation","stats"].includes(method)
    &&(!value.reconnectJob||["bind","validate","tools"].includes(method)))value.assertReady();
  const remaining = Math.ceil(operation.deadline - monotonicNow());
  if (remaining <= 0) throw failure("Timeout", "Operation expired in the worker message queue");
  const options = { timeout: remaining };
  switch (method) {
    case "bind": {
      if (bindings.size >= 128 || nextBinding > Number.MAX_SAFE_INTEGER) throw failure("LimitExceeded", "Too many worker binding handles");
      const id = nextBinding++;
      bindings.set(id, value.tool(payload.name));
      return id;
    }
    case "fresh_bind":
      if (bindings.size >= 128 || nextBinding > Number.MAX_SAFE_INTEGER) throw failure("LimitExceeded", "Too many worker binding handles");
      operation.promise = value.freshTool(payload.name, options); break;
    case "refresh": operation.promise = value.refresh(options); break;
    case "reconnect": operation.promise = value.reconnect(options); break;
    case "start_notifications": operation.promise = value.startNotifications(options); break;
    case "stop_notifications": operation.promise = value.stopNotifications(options); break;
    case "notification_status": operation.promise = value.notificationStatus(options); break;
    case "resume_notifications": operation.promise = value.resumeNotifications(payload.resourceId,options); break;
    case "schema_registry": operation.promise = value.setSchemaRegistry(payload.resources, options); break;
    case "schema_registry_stats": operation.promise = value.schemaRegistryStats(options); break;
    case "cache_stats": operation.promise = value.validatorCacheStats(options); break;
    case "cache_clear": operation.promise = value.clearValidatorCache(options); break;
    case "cache_capacity": operation.promise = value.setValidatorCacheCapacity(payload.capacity,options); break;
    case "complete_line": operation.promise = value.completeLine(payload.line, payload.cursor, options); break;
    case "command_spec": operation.promise = value.commandSpec(payload.tool, options); break;
    case "release_binding": {
      bindings.get(payload.id)?.dispose(); bindings.delete(payload.id); return null;
    }
    case "validate": return binding(payload.id).validateRaw(payload.json);
    case "tools": return runtime.tools();
    case "call": operation.promise = binding(payload.id).callRaw(payload.json, options); break;
    case "execute": operation.promise = value.execute(payload.tool, JSON.parse(payload.argv), options); break;
    case "execute_line": operation.promise = value.executeLine(payload.line, options); break;
    case "resume": operation.promise = value.resumeRaw(payload.id, payload.json, options); break;
    case "release_continuation": runtime.release_continuation(payload.id); return null;
    case "stats": return { ...events.snapshot(), bindings: bindings.size, operations: operations.size - 1, inputBytes: inputBytes - operation.bytes, workerContext: typeof document === "undefined" };
    default: throw failure("UnsupportedOperation", "Unknown worker operation");
  }
  links.set(operation.promise.operationId, operation.id);
  if (operation.cause) value.cancelOperation(operation.promise.operationId, operation.cause.code);
  try {
    const result = await operation.promise;
    if (method === "fresh_bind") {
      if(!value.bindingCurrent(result)){result.dispose();throw failure("BindingStale","Binding completed in an earlier connection context");}
      if (bindings.size >= 128) { result.dispose(); throw failure("LimitExceeded", "Too many worker binding handles"); }
      operation.transportEpoch=transportEpoch;
      const id = nextBinding++; bindings.set(id, result); return id;
    }
    return ["execute", "execute_line"].includes(method) ? JSON.stringify(result) : result;
  }
  finally { links.delete(operation.promise.operationId); }
}

async function close() {
  if (closing) return closing;
  closing = (async () => {
    startup.abort();
    let closeError;
    try { if (remote) await remote.close(); } catch (error) { closeError = error; }
    await initializing?.catch(() => {});
    const pending = [...operations.values()].filter(operation => operation.method !== "close");
    await Promise.allSettled(pending.map(operation => operation.finished));
    bindings.clear();
    const loss = events?.close() ?? { dropped: 0, coreDropped: 0 };
    // A final pending cell travels before the close reply, even if cleanup failed.
    const final = catalog.close();
    if (final) post({v:VERSION, kind:"catalog", ...final});
    runtime?.free(); runtime = undefined;
    if (closeError) throw closeError;
    return loss;
  })();
  return closing;
}
async function fatal(error) {
  if (closing) return;
  post({ v: VERSION, kind: "fatal", error: encodeError(error) });
  await close();
  self.close();
}

self.addEventListener("message", event => {
  const message = event.data;
  if (message?.v !== VERSION) { void fatal(failure("WorkerProtocolError", "Unsupported worker message version")); return; }
  if (message.kind === "credential") {
    try { credentials.receive(message); } catch { void fatal(failure("WorkerProtocolError", "Invalid host credential reply")); }
    return;
  }
  if (message.kind === "io") {
    try { io.receive(message); } catch { void fatal(failure("WorkerProtocolError", "Invalid host I/O message")); }
    return;
  }
  if (message.kind === "ack") { events?.acknowledge(message.batch); return; }
  if (message.kind === "catalog_ack") {
    try { catalog.acknowledge(message.id); } catch { void fatal(failure("LimitExceeded", "Catalog message IDs exhausted")); }
    return;
  }
  if (message.kind === "cancel") {
    const operation = operations.get(message.id);
    if (!operation || !["Cancelled", "Timeout"].includes(message.code)) return;
    operation.cause ??= failure(message.code, message.code === "Timeout" ? "Worker operation deadline exceeded; execution may be unknown" : "Worker operation cancelled; execution may be unknown");
    if (operation.promise && remote) remote.cancelOperation(operation.promise.operationId, operation.cause.code);
    else if (operation.method === "connect") startup.abort();
    return;
  }
  if (message.kind !== "request" || !Number.isSafeInteger(message.id) || message.id <= lastRequest) {
    void fatal(failure("WorkerProtocolError", "Invalid or reused worker request ID")); return;
  }
  lastRequest = message.id;
  if (message.method === "close") {
    post({v:VERSION,kind:"operation_started",id:message.id});
    void close().then(loss => { reply(message.id, loss); self.close(); }, error => { errorReply(message.id, error); self.close(); });
    return;
  }
  if (closing) { errorReply(message.id, failure("TransportClosed", "Worker is closing")); return; }
  const bytes = new TextEncoder().encode(JSON.stringify(message)).byteLength + 128;
  const cleanup = ["release_binding", "release_continuation", "stop_notifications", "notification_status", "resume_notifications"].includes(message.method);
  if(message.method==="resume_notifications"&&resumeCount){errorReply(message.id,failure("QueueFull","A notification resume is already pending"));return;}
  if (cleanup ? cleanupCount >= 160 || cleanupBytes + bytes > 65536 : normalCount >= capacity || inputBytes + bytes > byteLimit) {
    errorReply(message.id, failure("QueueFull", "Worker request queue is full")); return;
  }
  if (!Number.isFinite(message.deadline)) { errorReply(message.id, failure("InvalidInput", "Missing monotonic deadline")); return; }
  const operation = { id: message.id, method: message.method, deadline: message.deadline, bytes };
  operations.set(message.id, operation);
  if(message.method==="resume_notifications")resumeCount++;
  if (cleanup) { cleanupCount++; cleanupBytes += bytes; }
  else { normalCount++; inputBytes += bytes; }
  operation.finished = Promise.resolve().then(() => dispatch(message.method, message.payload, operation)).then(
    value => reply(message.id, value), error => errorReply(message.id, error),
  ).finally(() => {
    operations.delete(message.id);
    if(message.method==="resume_notifications")resumeCount--;
    if (cleanup) { cleanupCount--; cleanupBytes -= bytes; }
    else { normalCount--; inputBytes -= bytes; }
  });
});
self.addEventListener("messageerror", () => { void fatal(failure("WorkerProtocolError", "Worker message could not be decoded")); });
