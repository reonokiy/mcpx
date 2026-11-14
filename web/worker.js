import { OperationObserver } from "./operation-events.js";
import { CatalogSource } from "./catalog.js";
import { copyErrorMetadata, contextError } from "./errors.js";
import { parseSafe, stringifySafe } from "./client.js";
import { VERSION, monotonicNow, failure, decodeError, integer } from "./worker-protocol.js";
import { WorkerFetchHost } from "./worker-fetch-host.js";
import { CredentialHost } from "./worker-credentials.js";
import { schemaRegistryJson } from "./schemas.js";
import { notificationRecoveryPolicy } from "./notifications.js";
import { responseRecoveryPolicy } from "./responses.js";
import { httpSessionRecoveryPolicy } from "./session-recovery.js";
import { attachCancellation, releaseOperation, materializeError, rejectOperation } from "./operations.js";

// A dedicated worker owns the Rust runtime. Only generic handles/JSON cross the port.
export class WorkerRemote {
  constructor({
    workerURL = new URL("./worker-entry.js", import.meta.url), wasmURL,
    protocol = "auto", timeout = 60_000, shutdownTimeout = 2_000,
    maxInFlight = 16, queueCapacity = 64, maxInputBytes = 8 * 1024 * 1024,
    token, credentialProvider, credentialReference, catalogTtl = 60_000, coreEventCapacity = 256, coreEventBytes = 4 * 1024 * 1024, schemaRegistry, validatorCacheCapacity = 128,
    elicitation = {}, eventCapacity = 256, eventBytes = 4 * 1024 * 1024,
    onOperationEvent, onEvent = () => {}, onEventError = () => {}, httpNotifications = false, notificationRecovery, responseRecovery, httpSessionRecovery,
  } = {}) {
    this.timeout = integer(timeout, 1, 2_147_483_647, "timeout");
    this.shutdownTimeout = integer(shutdownTimeout, 1, 60_000, "shutdownTimeout");
    this.capacity = integer(queueCapacity, 1, 1024, "queueCapacity");
    this.byteLimit = integer(maxInputBytes, 1024, 64 * 1024 * 1024, "maxInputBytes");
    integer(maxInFlight, 1, 16, "maxInFlight");
    integer(eventCapacity, 2 * maxInFlight + 4, 4096, "eventCapacity");
    integer(eventBytes, (maxInFlight + 3) * 16 * 1024, 64 * 1024 * 1024, "eventBytes");
    if (!["auto", "modern", "legacy"].includes(protocol)) throw new RangeError("Unsupported protocol mode");
    if (token !== undefined && typeof token !== "string") throw new TypeError("token must be a string");
    integer(coreEventCapacity, 4, 4096, "coreEventCapacity");
    integer(coreEventBytes, 49152, 64 * 1024 * 1024, "coreEventBytes");
    integer(catalogTtl, 0, Number.MAX_SAFE_INTEGER, "catalogTtl");
    integer(validatorCacheCapacity,0,4096,"validatorCacheCapacity");
    if(typeof httpNotifications!=="boolean")throw new TypeError("httpNotifications must be a boolean");
    notificationRecovery=notificationRecoveryPolicy(notificationRecovery);
    responseRecovery=responseRecoveryPolicy(responseRecovery);
    httpSessionRecovery=httpSessionRecoveryPolicy(httpSessionRecovery);
    this.httpSessionRecovery=Boolean(httpSessionRecovery);
    if (schemaRegistry !== undefined) schemaRegistryJson(schemaRegistry);
    this.credentials = new CredentialHost(message => this.post(message), { token, credentialProvider, credentialReference });
    this.config = {
      protocol, wasmURL: wasmURL === undefined ? undefined : String(new URL(wasmURL, globalThis.location.href)),
      options: { httpNotifications, notificationRecovery, responseRecovery, httpSessionRecovery, timeout, maxInFlight, queueCapacity, maxInputBytes, catalogTtl, coreEventCapacity, coreEventBytes, validatorCacheCapacity, schemaRegistry: schemaRegistry?.map(r => ({uri:r.uri, schemaJson:r.schemaJson})), elicitation: { form: Boolean(elicitation.form), url: Boolean(elicitation.url) }, eventCapacity, eventBytes },
    };
    this.onEvent = onEvent; this.onEventError = onEventError;
    this.operationEvents = new OperationObserver(onOperationEvent,onEventError);
    this.catalogChanges = new CatalogSource(); this.lastCatalogId = 0;
    this.state = "new"; this.nextId = 1;this.transportEpoch=0;
    this.pending = new Map(); this.bindings = new Set(); this.continuations = new WeakSet();
    this.count = 0; this.inputBytes = 0; this.cleanupCount = 0; this.cleanupBytes = 0; this.resumeCount = 0;
    this.worker = new Worker(workerURL, { type: "module", name: "mcpx" });
    this.io = new WorkerFetchHost((message, transfer) => this.post(message, transfer));
    this.message = event => this.receive(event.data);
    this.error = event => { event.preventDefault?.(); this.fail(failure("WorkerError", "mcpx worker failed; execution may be unknown")); };
    this.messageError = () => this.fail(failure("WorkerProtocolError", "Worker message could not be decoded"));
    this.worker.addEventListener("message", this.message);
    this.worker.addEventListener("error", this.error);
    this.worker.addEventListener("messageerror", this.messageError);
  }

  static async connect(endpoint, options = {}) {
    const remote = new WorkerRemote(options);
    try { await remote.connect(endpoint, options); return remote; }
    catch (error) { await remote.close().catch(() => {}); throw error; }
  }
  connect(endpoint, options) {
    if (this.state !== "new") return rejectOperation(failure("AlreadyConnected", "Worker connection has already started"));
    this.state = "connecting";
    this.credentials.endpoint = endpoint;
    return this.request("connect", { endpoint, ...this.config }, options, () => {
      if (this.state !== "connecting") throw failure("TransportClosed", "Worker is closing");
      delete this.config.options.schemaRegistry;
      this.state = "ready"; return this;
    });
  }
  assertReady() { if (this.state !== "ready") throw failure("TransportClosed", "Worker remote is not connected or is closing"); }
  get token() { return this.credentials.resolver.token; }
  set token(value) { this.credentials.resolver.token = value; }
  post(message, transfer = []) { if (!this.terminated) this.worker.postMessage(message, transfer); }

  request(method, payload, options = {}, transform = value => value) {
    try {
      if (["closed", "closing"].includes(this.state) && method !== "close") throw failure("TransportClosed", "Worker remote is closed or closing");
      const { signal, timeout = this.timeout } = options;
      integer(timeout, 1, 2_147_483_647, "timeout");
      if (signal && (typeof signal.addEventListener !== "function" || typeof signal.removeEventListener !== "function")) throw new TypeError("signal must be an AbortSignal");
      if (signal?.aborted) throw failure("Cancelled", "Operation cancelled before admission");
      if (this.nextId > Number.MAX_SAFE_INTEGER) throw failure("LimitExceeded", "Worker request IDs exhausted");
      const cleanup = ["release_binding", "release_continuation", "close", "stop_notifications", "notification_status", "resume_notifications"].includes(method);
      if(method==="resume_notifications"&&this.resumeCount)throw failure("QueueFull","A notification resume is already pending");
      const id = this.nextId;
      const wire = { v: VERSION, kind: "request", id, method, payload, deadline: monotonicNow() + timeout };
      const json = stringifySafe(wire);
      if (json.length > (cleanup ? 65536 : this.byteLimit)) throw failure("QueueFull", "Worker message byte budget is full");
      const bytes = new TextEncoder().encode(json).byteLength + 128;
      if (method !== "close" && (cleanup ? this.cleanupCount >= 160 || this.cleanupBytes + bytes > 65536 : this.count >= this.capacity || this.inputBytes + bytes > this.byteLimit)) throw failure("QueueFull", "Worker request queue is full");
      this.operationEvents.reserve(id);
      this.nextId++;
      const job = { id, method, bytes, cleanup, signal, transform };
      const promise = job.promise = new Promise((resolve, reject) => { job.resolve = resolve; job.reject = reject; });
      job.detachCancellation = attachCancellation(promise, id, () => this.interrupt(job, "Cancelled"));
      this.pending.set(id, job);
      if(method==="resume_notifications")this.resumeCount++;
      if (cleanup) { this.cleanupCount++; this.cleanupBytes += bytes; }
      else { this.count++; this.inputBytes += bytes; }
      job.abort = () => this.interrupt(job, "Cancelled");
      signal?.addEventListener("abort", job.abort, { once: true });
      job.timer = setTimeout(() => this.interrupt(job, "Timeout"), timeout);
      try { this.operationEvents.queued(id);
        if (!this.pending.has(id)) return promise;
        if (job.interrupted || this.terminated || monotonicNow() >= wire.deadline) {
          this.finish(job, failure(job.interrupted ?? (this.terminated ? "TransportClosed" : "Timeout"), "Operation stopped before worker dispatch"));
          return promise;
        }
        this.post(wire); job.posted = true; } catch { this.finish(job, failure("WorkerProtocolError", "Could not send worker message")); }
      return promise;
    } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); }
  }

  interrupt(job, code) {
    if (!this.pending.has(job.id) || job.interrupted) return;
    if (job.method === "close") { this.fail(failure("WorkerUnresponsive", "Worker cleanup deadline exceeded; execution may be unknown")); return; }
    job.interrupted = code;
    this.post({ v: VERSION, kind: "cancel", id: job.id, code });
    // A non-yielding validator cannot process messages. Terminate only after
    // allowing the worker a bounded interval to acknowledge and clean up.
    job.killTimer = setTimeout(() => this.fail(failure("WorkerUnresponsive", "Worker did not acknowledge interruption; execution may be unknown")), this.shutdownTimeout);
  }
  receive(message) {
    if (this.terminated) return;
    if (message?.v !== VERSION) { this.fail(failure("WorkerProtocolError", "Unsupported worker message version")); return; }
    if (message.kind === "catalog") {
      try { this.acceptCatalog(message); this.post({v:VERSION, kind:"catalog_ack", id:message.id}); }
      catch (error) { this.fail(error); }
      return;
    }
    if(message.kind==="transport_reset") {
      const job=this.pending.get(message.id);
      const automatic=message.automatic===true;
      if(typeof message.automatic!=="boolean"||!Number.isSafeInteger(message.epoch)||message.epoch!==this.transportEpoch+1
        ||(automatic? !this.httpSessionRecovery||message.id!==null : !Number.isSafeInteger(message.id)||message.id<=0||job?.method!=="reconnect"||job.reset)) {
        this.fail(failure("WorkerProtocolError","Invalid transport reset confirmation"));return;
      }
      if(!automatic)job.reset=true;
      this.transportEpoch=message.epoch;
      for(const binding of this.bindings)binding.stale=true;
      this.bindings.clear();
      this.continuations=new WeakSet();
      return;
    }
    if (message.kind === "credential") {
      try { this.credentials.receive(message); } catch { this.fail(failure("WorkerProtocolError", "Invalid worker credential request")); }
      return;
    }
    if (message.kind === "io") {
      try { this.io.receive(message); } catch { this.fail(failure("WorkerProtocolError", "Invalid worker I/O action")); }
      return;
    }
    if (message.kind === "events") {
      if (!Number.isSafeInteger(message.batch) || !Array.isArray(message.events) || message.events.length > 32 || !Number.isSafeInteger(message.dropped) || message.dropped < 0 || !Number.isSafeInteger(message.coreDropped ?? 0) || (message.coreDropped ?? 0) < 0) {
        this.fail(failure("WorkerProtocolError", "Invalid worker event batch")); return;
      }
      try {
        if (message.dropped) this.observe({ operation_id: 0, kind: "lagged", payload: { source: "worker-mailbox", dropped: message.dropped } });
        if (message.coreDropped) this.observe({ operation_id: 0, kind: "lagged", payload: { source: "core", dropped: message.coreDropped } });
        for (const event of message.events) {
          const job = this.pending.get(event?.queue_operation_id);
          if (job && ["completed", "tool_failed", "needs_input"].includes(event.kind)) job.context = copyErrorMetadata({ operation_id: event.operation_id, execution_certainty: event.kind === "needs_input" ? "needs_input" : "result_received" }, {});
          this.observe(event);
        }
      } finally { this.post({ v: VERSION, kind: "ack", batch: message.batch }); }
      return;
    }
    if (message.kind === "operation_started") {
      const job = this.pending.get(message.id);
      if (job && !job.ownerStarted) {
        job.ownerStarted = true;
        try { this.operationEvents.start(job.id); } catch (error) { this.fail(error); }
      }
      return;
    }
    if (message.kind === "fatal") { this.fail(decodeError(message.error)); return; }
    if (!["result", "error"].includes(message.kind)) { this.fail(failure("WorkerProtocolError", "Invalid worker reply")); return; }
    const job = this.pending.get(message.id);
    if (!job) return; // A reply after shutdown cannot resurrect a handle.
    if (message.kind === "result") {
      if(message.epoch!==undefined&&(!Number.isSafeInteger(message.epoch)||message.epoch<0)){this.fail(failure("WorkerProtocolError","Invalid result transport epoch"));return;}
      job.resultEpoch=message.epoch;job.context = copyErrorMetadata(message.context, {}); this.finish(job, null, message.value);
    }
    else if (message.kind === "error") this.finish(job, decodeError(message.error));
    else this.fail(failure("WorkerProtocolError", "Invalid worker reply"));
  }
  observe(event) { try { this.onEvent(event); } catch (error) { try { this.onEventError(error); } catch { /* Observer failures cannot own transport state. */ } } }
  acceptCatalog(change) {
    if (!Number.isSafeInteger(change?.id) || change.id < 1 || typeof change.published !== "boolean" || typeof change.invalidated !== "boolean" || !change.published && !change.invalidated) throw failure("WorkerProtocolError", "Invalid catalog change");
    if (change.id <= this.lastCatalogId) return;
    if (change.id !== this.lastCatalogId + 1) throw failure("WorkerProtocolError", "Missing catalog change");
    this.lastCatalogId = change.id; this.catalogChanges.change(change);
  }
  finish(job, error, value) {
    if (!this.pending.delete(job.id)) return;
    if(job.method==="resume_notifications")this.resumeCount--;
    clearTimeout(job.timer); clearTimeout(job.killTimer);
    job.signal?.removeEventListener("abort", job.abort);
    if (job.cleanup) { this.cleanupCount--; this.cleanupBytes -= job.bytes; }
    else { this.count--; this.inputBytes -= job.bytes; }
    const context = {
      operationId: job.context?.operation_id, queueOperationId: job.id,
      certainty: job.context?.execution_certainty ?? (job.posted && ["call", "execute", "execute_line", "resume"].includes(job.method) ? "unknown" : "not_sent"),
    };
    let result, failure;
    try { if (error) throw error; result = job.transform(value,job.resultEpoch); }
    catch (cause) { failure = materializeError(contextError(cause,context)); }
    this.operationEvents.finish(job.id, {coreId:failure?.operation_id ?? context.operationId,certainty:failure?.execution_certainty ?? context.certainty,error:failure,summary:job.context});
    if (failure) job.reject(failure); else job.resolve(result);
    releaseOperation(job);
  }

  fail(error) {
    if (this.terminated) return;
    const pending = [...this.pending.values()];
    this.terminate();
    this.failurePromise = this.io.close().then(() => {
      for (const job of pending) {
        // A worker-wide failure cannot transfer one operation's evidence to another.
        const cause = job.interrupted ? failure(job.interrupted,"Worker interruption required termination; execution may be unknown")
          : failure(typeof error?.code === "string" ? error.code : "WorkerError",typeof error?.message === "string" ? error.message : "Worker failed; execution may be unknown");
        this.finish(job,cause);
      }
    });
  }
  terminate() {
    if (this.terminated) return;
    this.terminated = true; this.state = "closed";
    this.catalogChanges.close();
    delete this.config.options.schemaRegistry;
    this.credentials.close();
    this.worker.removeEventListener("message", this.message);
    this.worker.removeEventListener("error", this.error);
    this.worker.removeEventListener("messageerror", this.messageError);
    this.worker.terminate();
    for (const binding of this.bindings) binding.disposed = true;
    this.bindings.clear();
  }

  outcome(raw,epoch=this.transportEpoch) {
    const result = parseSafe(raw);
    if (result?.kind === "needs_input") { Object.freeze(result.value);if(epoch===this.transportEpoch)this.continuations.add(result.value); }
    return result;
  }
  tool(name, options) { return this.bind(name, options, "bind"); }
  freshTool(name, options) { return this.bind(name, options, "fresh_bind"); }
  refresh(options) { try { this.assertReady(); return this.request("refresh", {}, options); } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); } }
  reconnect(options) {
    try {this.assertReady();return this.request("reconnect",{},options);}catch(error){return rejectOperation(contextError(error,{certainty:"not_sent"}));}
  }
  subscribeCatalog() { this.assertReady(); return this.catalogChanges.subscribe(); }
  startNotifications(options) { try { this.assertReady(); return this.request("start_notifications",{},options); } catch(error) { return rejectOperation(contextError(error,{certainty:"not_sent"})); } }
  stopNotifications(options) { try { this.assertReady(); return this.request("stop_notifications",{},options); } catch(error) { return rejectOperation(contextError(error,{certainty:"not_sent"})); } }
  notificationStatus(options) { try { this.assertReady(); return this.request("notification_status",{},options); } catch(error) { return rejectOperation(contextError(error,{certainty:"not_sent"})); } }
  resumeNotifications(resourceId,options) {
    try {
      this.assertReady();
      if(!Number.isSafeInteger(resourceId)||resourceId<=0)throw failure("InvalidHandle","Notification resource ID must be a positive exact host integer");
      return this.request("resume_notifications",{resourceId},options);
    }catch(error){return rejectOperation(contextError(error,{certainty:"not_sent"}));}
  }
  setSchemaRegistry(resources, options) {
    try {
      this.assertReady(); schemaRegistryJson(resources);
      return this.request("schema_registry", { resources }, options, () => {
        for (const binding of this.bindings) binding.stale = true;
        this.continuations = new WeakSet();
      });
    } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); }
  }
  schemaRegistryStats(options) { try { this.assertReady(); return this.request("schema_registry_stats", {}, options); } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); } }
  validatorCacheStats(options) { try { this.assertReady(); return this.request("cache_stats",{},options); } catch(error) {return rejectOperation(contextError(error, { certainty: "not_sent" }));} }
  clearValidatorCache(options) { try { this.assertReady(); return this.request("cache_clear",{},options); } catch(error) {return rejectOperation(contextError(error, { certainty: "not_sent" }));} }
  setValidatorCacheCapacity(capacity,options) { try { this.assertReady(); integer(capacity,0,4096,"validator cache capacity"); return this.request("cache_capacity",{capacity},options); } catch(error) {return rejectOperation(contextError(error, { certainty: "not_sent" }));} }
  commandSpec(tool, options) { try { this.assertReady(); return this.request("command_spec", { tool }, options); } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); } }
  completeLine(line, cursor, options) {
    try {
      this.assertReady();
      if (typeof line !== "string") throw new TypeError("Command line must be a string");
      const bytes = new TextEncoder().encode(line), size = bytes.byteLength;
      if (size > 65536) throw failure("LimitExceeded", "completion input exceeds 64 KiB");
      cursor ??= size;
      if (!Number.isSafeInteger(cursor) || cursor < 0 || cursor > size || cursor < size && (bytes[cursor] & 0xc0) === 0x80) throw failure("InvalidCursor", "completion cursor must be a UTF-8 byte boundary");
      return this.request("complete_line", { line, cursor }, options);
    } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); }
  }
  bind(name, options, method) {
    try {
      this.assertReady();
      return this.request(method, { name }, options, (id,epoch) => {
        this.assertReady();
        const binding = { id, disposed: false,stale:epoch!==this.transportEpoch };
        if(!binding.stale)this.bindings.add(binding);
        const call = (json, options, transform) => {
          try {
            this.assertReady();
            if (binding.disposed) throw failure("InvalidHandle", "Binding is disposed");
            if (binding.stale) throw failure("BindingStale", "Schema resources changed; bind the tool again");
            if (typeof json !== "string") throw new TypeError("Raw arguments must be a JSON string");
            return this.request("call", { id, json }, options, transform);
          } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); }
        };
        const validate = (json, options, transform) => {
          try {
            this.assertReady();
            if (binding.disposed) throw failure("InvalidHandle", "Binding is disposed");
            if (binding.stale) throw failure("BindingStale", "Schema resources changed; bind the tool again");
            if (typeof json !== "string") throw new TypeError("Raw arguments must be a JSON string");
            return this.request("validate", { id, json }, options, transform);
          } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); }
        };
        return {
          call: (args, options) => { try { return call(stringifySafe(args), options, (raw,epoch) => this.outcome(raw,epoch)); } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); } },
          callRaw: (json, options) => call(json, options, value => value),
          validate: (args, options) => { try { return validate(stringifySafe(args), options, parseSafe); } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); } },
          validateRaw: (json, options) => validate(json, options, value => value),
          dispose: () => {
            binding.disposed = true;
            if (this.state !== "ready") { this.bindings.delete(binding); return Promise.resolve(); }
            return this.request("release_binding", { id }, {}, () => { this.bindings.delete(binding); });
          },
        };
      });
    } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); }
  }
  tools(options) { try { this.assertReady(); return this.request("tools", {}, options, parseSafe); } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); } }
  execute(tool, argv, options) {
    try {
      this.assertReady();
      if (!Array.isArray(argv) || !argv.every(value => typeof value === "string")) throw new TypeError("argv must be an array of strings");
      return this.request("execute", { tool, argv: stringifySafe(argv) }, options, (raw,epoch) => this.outcome(raw,epoch));
    }
    catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); }
  }
  executeLine(line, options) {
    try {
      this.assertReady();
      if (typeof line !== "string") throw new TypeError("Command line must be a string");
      return this.request("execute_line", { line }, options, (raw,epoch) => this.outcome(raw,epoch));
    } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); }
  }
  resume(continuation, responses, options) {
    try {
      this.assertReady();
      if (!this.continuations.has(continuation)) throw failure("InvalidContinuation", "Use the original continuation from this worker remote");
      return this.resumeRaw(continuation.id, stringifySafe(responses), options, (raw,epoch) => this.outcome(raw,epoch));
    } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); }
  }
  resumeRaw(id, json, options, transform = value => value) {
    try {
      this.assertReady();
      if (!Number.isSafeInteger(id) || id < 1) throw failure("InvalidHandle", "Continuation ID must be a positive safe integer Number");
      if (typeof json !== "string") throw new TypeError("Raw responses must be a JSON string");
      return this.request("resume", { id, json }, options, transform);
    }
    catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); }
  }
  releaseContinuation(continuation) {
    try {
      this.assertReady();
      if (!this.continuations.has(continuation)) throw failure("InvalidContinuation", "Continuation belongs to another worker remote");
      return this.request("release_continuation", { id: continuation.id }, {}, () => { this.continuations.delete(continuation); });
    } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); }
  }
  diagnostics(options) { try { this.assertReady(); return this.request("stats", {}, options, value => ({ ...value, ioRequests: this.io.records.size, ioBufferedBytes: this.io.buffered })); } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); } }
  cancel() { for (const job of this.pending.values()) if (job.method !== "close") this.interrupt(job, "Cancelled"); }
  close() {
    if (this.failurePromise) return this.failurePromise;
    if (this.closePromise) return this.closePromise;
    if (this.terminated) return this.io.close();
    this.state = "closing";
    for (const job of this.pending.values()) clearTimeout(job.timer);
    this.closePromise = this.request("close", {}, { timeout: this.shutdownTimeout }).then(
      async loss => {
        if (loss?.dropped) this.observe({ operation_id: 0, kind: "lagged", payload: { source: "worker-mailbox", dropped: loss.dropped } });
        if (loss?.coreDropped) this.observe({ operation_id: 0, kind: "lagged", payload: { source: "core", dropped: loss.coreDropped } });
        await this.io.close(); this.terminate();
      }, async error => { await this.io.close(); this.terminate(); throw error; },
    );
    return this.closePromise;
  }
}
