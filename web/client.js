import { OperationObserver } from "./operation-events.js";
import { CatalogSource } from "./catalog.js";
import { HttpNotifications, notificationRecoveryPolicy } from "./notifications.js";
import { ToolResponse, responseRecoveryPolicy, resumableResponse } from "./responses.js";
import { copyErrorMetadata, contextError } from "./errors.js";
import { CredentialResolver, AdmissionLock, abortable } from "./credentials.js";
import { schemaRegistryJson } from "./schemas.js";
import { attachCancellation, releaseOperation, materializeError, rejectOperation } from "./operations.js";
import { HttpSessionRecovery, httpSessionRecoveryPolicy } from "./session-recovery.js";

function assertJson(value, seen = new Set()) {
  if (typeof value === "number" && (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value)))) {
    throw new RangeError("Use callRaw for numbers outside the JavaScript safe range");
  }
  if (["undefined", "function", "symbol", "bigint"].includes(typeof value)) throw new TypeError("Value is not JSON-compatible");
  if (value && typeof value === "object") {
    if (seen.has(value)) throw new TypeError("Cyclic arguments are not supported");
    if (!Array.isArray(value) && ![Object.prototype, null].includes(Object.getPrototypeOf(value))) throw new TypeError("Use plain JSON objects or callRaw");
    seen.add(value);
    const children = Array.isArray(value) ? value : Object.values(value).filter(child => child !== undefined);
    for (const child of children) assertJson(child, seen);
    seen.delete(value);
  }
}

export function stringifySafe(value) {
  assertJson(value);
  return JSON.stringify(value);
}

export function parseSafe(text) {
  return JSON.parse(text, (_key, value) => {
    if (typeof value === "number" && (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value)))) {
      const error = new RangeError("Result contains an unsafe number; use the raw JSON result");
      error.raw = text;
      throw error;
    }
    return value;
  });
}

function failure(code, message) { return Object.assign(new Error(message), { code }); }
function hostError(value) {
  if (value instanceof Error) return value;
  try {
    const error = typeof value === "string" ? JSON.parse(value) : value;
    if (typeof error?.code === "string" && typeof error.message === "string") {
      const result = failure(error.code, error.message);
      if (Array.isArray(error.issues)) result.issues = error.issues;
      return copyErrorMetadata(error, result);
    }
  } catch { /* The runtime may also throw an ordinary string. */ }
  return failure("HostError", String(value));
}
function integer(value, low, high, name) {
  if (!Number.isSafeInteger(value) || value < low || value > high) throw new RangeError(`${name} must be an integer in ${low}..${high}`);
  return value;
}

// One owner drives the synchronous runtime. Only fetch/read awaits run concurrently.
export class Remote {
  constructor(runtime, {
    fetch: fetchImpl = globalThis.fetch, token, onEvent = () => {}, onEventError = () => {},
    onOperationEvent, timeout = 60_000, elicitation = {}, maxInFlight = 16, queueCapacity = 64, maxInputBytes = 8 * 1024 * 1024,
    eventGate = () => {}, onCatalogChange, onTransportReset, httpNotifications = false, notificationRecovery, responseRecovery, httpSessionRecovery,
    credentialProvider, credentialReference, catalogTtl = 60_000, coreEventCapacity = 256, coreEventBytes = 4 * 1024 * 1024, schemaRegistry, validatorCacheCapacity = 128,
  } = {}) {
    this.runtime = runtime;
    this.fetch = fetchImpl.bind(globalThis);
    this.credentials = new CredentialResolver({ token, credentialProvider, credentialReference });
    this.admission = new AdmissionLock();
    this.scopeEpoch = Symbol();
    this.catalogEpoch = Symbol();
    this.catalogContext = Symbol();
    if(typeof httpNotifications!=="boolean")throw new TypeError("httpNotifications must be a boolean");
    this.httpNotifications=httpNotifications;
    this.notifications=new HttpNotifications(this,notificationRecoveryPolicy(notificationRecovery));
    this.responseRecovery=responseRecoveryPolicy(responseRecovery);
    this.catalogChanges = new CatalogSource();
    if (onCatalogChange !== undefined && typeof onCatalogChange !== "function") throw new TypeError("onCatalogChange must be a function");
    this.onCatalogChange = onCatalogChange;
    if(onTransportReset!==undefined&&typeof onTransportReset!=="function")throw new TypeError("onTransportReset must be a function");
    this.onTransportReset=onTransportReset;
    this.onEvent = onEvent;
    this.onEventError = onEventError;
    this.operationEvents = new OperationObserver(onOperationEvent, onEventError);
    if (typeof eventGate !== "function") throw new TypeError("eventGate must be a function");
    this.eventGate = eventGate;
    this.timeout = integer(timeout, 1, 2_147_483_647, "timeout");
    this.maxInFlight = integer(maxInFlight, 1, 16, "maxInFlight");
    this.queueCapacity = integer(queueCapacity, 1, 1024, "queueCapacity");
    this.maxInputBytes = integer(maxInputBytes, 1024, 64 * 1024 * 1024, "maxInputBytes");
    this.queue = [];
    this.active = new Set();
    this.inputBytes = 0;
    this.cleanupCount=0;this.cleanupBytes=0;this.resumeCount=0;
    this.nextOperation = 1;
    this.scheduled = false;
    this.closed = false;
    this.bindings = new Set();
    this.bindingRecords=new WeakMap();
    this.continuations = new WeakSet();
    this.continuationIds = new Map();
    this.clockOrigin = performance.now();
    this.sessionRecovery=new HttpSessionRecovery(this,httpSessionRecoveryPolicy(httpSessionRecovery));
    this.runtime.set_event_limits(integer(coreEventCapacity, 4, 4096, "coreEventCapacity"), integer(coreEventBytes, 49152, 64 * 1024 * 1024, "coreEventBytes"));
    this.runtime.set_elicitation(Boolean(elicitation.form), Boolean(elicitation.url));
    this.runtime.set_catalog_ttl(integer(catalogTtl, 0, Number.MAX_SAFE_INTEGER, "catalogTtl"));
    if (schemaRegistry !== undefined) this.runtime.set_schema_registry(schemaRegistryJson(schemaRegistry));
    this.runtime.set_validator_cache_capacity(integer(validatorCacheCapacity,0,4096,"validatorCacheCapacity"));
    // Subscriptions start at the state handed to this owner, without an initial event.
    this.runtime.catalog_change();
  }

  get token() { return this.credentials.token; }
  set token(value) { this.credentials.token = value; }

  get running() { return this.active.size > 0; }
  tick() { this.runtime.advance_time(Math.floor(performance.now() - this.clockOrigin)); }
  assertOpen() { if (this.closed) throw failure("TransportClosed", "Remote is closed"); }
  assertReady() {this.assertOpen();if(this.suspended)throw failure("TransportClosed","reconnect must complete before new operations");}
  subscribeCatalog() { this.assertOpen(); return this.catalogChanges.subscribe(); }
  startNotifications(options) {
    return this.enqueue({control:true,local:true,asyncLocal:true,notificationControl:"start",start:async job=>{
      if(this.notifications.record&&["opening","listening"].includes(this.notifications.status()?.phase)){this.httpNotifications=true;return this.notifications.status();}
      job.cancelLocal=()=>{this.httpNotifications=false;return this.notifications.stop();};
      await this.notifications.stop();
      this.ensure(job);this.httpNotifications=true;
      return this.notifications.ensure(job.credential,true);
    }},"",options);
  }
  stopNotifications(options) {
    return this.enqueue({cleanup:true,local:true,asyncLocal:true,skipPreflight:true,skipEventGate:true,skipAdmission:true,start:async job=>{
      for(const pending of [...this.queue,...this.active])if(["start","resume"].includes(pending.notificationControl)&&pending.id<job.id)this.cancelJob(pending,failure("Cancelled","Notification start/resume superseded by stop"));
      this.httpNotifications=false;const stopping=this.notifications.stop();job.cancelLocal=()=>stopping;
      return await stopping;
    }},"",options);
  }
  notificationStatus(options) { return this.enqueue({cleanup:true,local:true,skipPreflight:true,skipEventGate:true,skipAdmission:true,start:()=>this.notifications.status()},"",options); }
  resumeNotifications(resourceId,options) {
    try {
      if(!Number.isSafeInteger(resourceId)||resourceId<=0)throw failure("InvalidHandle","Notification resource ID must be a positive exact host integer");
      return this.enqueue({cleanup:true,local:true,asyncLocal:true,skipPreflight:true,skipEventGate:true,skipAdmission:true,notificationControl:"resume",start:async job=>{
        const marker={};job.cancelLocal=()=>this.notifications.cancelResume(marker);
        return await this.notifications.resume(resourceId,job.controller.signal,()=>this.ensure(job),marker);
      }},"",options);
    }catch(error){return rejectOperation(contextError(hostError(error),{certainty:"not_sent"}));}
  }
  async waitNotificationEvents(signal) { await abortable(Promise.resolve(this.eventGate(signal)),signal); }
  ensureNotifications(credential,force=false) {
    if(this.closed||!this.httpNotifications)return;
    try{this.notifications.ensure(credential,force);}catch(error){this.eventError(hostError(error));}
  }

  parseOutcome(raw) {
    const outcome = parseSafe(raw);
    if (outcome?.kind === "needs_input") {
      Object.freeze(outcome.value);
      this.continuations.add(outcome.value);
      const now = Math.floor(performance.now() - this.clockOrigin);
      for (const [id, expires] of this.continuationIds) {
        if (expires <= now) {
          this.runtime.release_continuation(id);
          this.continuationIds.delete(id);
        }
      }
      if (!this.continuationIds.has(outcome.value.id) && this.continuationIds.size >= 32) {
        this.runtime.release_continuation(outcome.value.id);
        throw failure("LimitExceeded", "Too many retained continuation handles");
      }
      this.continuationIds.set(outcome.value.id, outcome.value.expires_at_ms);
    }
    return outcome;
  }

  connect(options) {
    return this.enqueue({ control: true, connect: true, start: () => this.runtime.discover(), transform: () => this }, "", options);
  }
  reconnect(options) {
    return this.enqueue({control:true,connect:true,reconnect:true,start:()=>this.runtime.discover(),transform:()=>undefined},"",options);
  }
  async resetTransport(job) {
    this.ensure(job);
    this.reconnectJob=job;
    this.suspended=true;
    this.runtime.reset_transport();
    this.catalogContext=Symbol();this.scopeEpoch=Symbol();
    for(const binding of this.bindings)binding.stale=true;
    this.bindings.clear();this.continuations=new WeakSet();this.continuationIds.clear();
    try{this.onTransportReset?.(job.id,{automatic:Boolean(job.automatic)});}catch(error){this.eventError(error);}
    for(const pending of [...this.queue,...this.active])if(["start","resume"].includes(pending.notificationControl)&&(job.automatic||pending.id<job.id))this.cancelJob(pending,failure("Cancelled","Notification control superseded by reconnect"));
    await this.notifications.stop();this.ensure(job);
    await this.closeSession(job.controller.signal);this.ensure(job);
  }
  refresh(options) { return this.enqueue({ local: true, refresh: true, refreshAt: this.catalogEpoch, refreshContext: this.catalogContext, start: () => undefined }, "", options); }
  freshTool(name, options) { return this.enqueue({ local: true, start: () => this.tool(name) }, name, options); }
  setSchemaRegistry(resources, options) {
    try {
      const json = schemaRegistryJson(resources);
      return this.enqueue({ control: true, local: true, start: () => {
        this.runtime.set_schema_registry(json);
        this.catalogContext = Symbol();
        this.scopeEpoch = Symbol();
        for (const binding of this.bindings) binding.stale = true;
        this.bindings.clear();
        this.continuations = new WeakSet(); this.continuationIds.clear();
      } }, json, options);
    } catch (error) { return rejectOperation(contextError(hostError(error), { certainty: "not_sent" })); }
  }
  schemaRegistryStats(options) {
    return this.enqueue({ local: true, start: () => parseSafe(this.runtime.schema_registry_stats()) }, "", options);
  }
  validatorCacheStats(options) { return this.enqueue({local:true,start:()=>parseSafe(this.runtime.validator_cache_stats())},"",options); }
  setValidatorCacheCapacity(capacity,options) {
    try { integer(capacity,0,4096,"validator cache capacity"); return this.enqueue({local:true,start:()=>this.runtime.set_validator_cache_capacity(capacity)},"",options); }
    catch(error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); }
  }
  clearValidatorCache(options) { return this.enqueue({local:true,start:()=>this.runtime.clear_validator_cache()},"",options); }
  commandSpec(tool, options) {
    return this.enqueue({ local: true, start: () => parseSafe(this.runtime.command_spec(tool)) }, tool, options);
  }
  completeLine(line, cursor, options) {
    try {
      if (typeof line !== "string") throw new TypeError("Command line must be a string");
      const bytes = new TextEncoder().encode(line), size = bytes.byteLength;
      if (size > 65536) throw failure("LimitExceeded", "completion input exceeds 64 KiB");
      cursor ??= size;
      if (!Number.isSafeInteger(cursor) || cursor < 0 || cursor > size || cursor < size && (bytes[cursor] & 0xc0) === 0x80) throw failure("InvalidCursor", "completion cursor must be a UTF-8 byte boundary");
      return this.enqueue({ local: true, start: () => parseSafe(this.runtime.complete_line(line, cursor)) }, line, options);
    } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); }
  }

  tool(name) {
    this.assertReady();
    const binding = { id: this.runtime.bind(name), disposed: false, leases: 0, epoch: this.scopeEpoch };
    this.bindings.add(binding);
    const submit = (json, options, transform) => {
      if (this.closed) return rejectOperation(contextError(failure("TransportClosed", "Remote is closed"), { certainty: "not_sent" }));
      if (this.suspended) return rejectOperation(contextError(failure("TransportClosed", "reconnect must complete before new operations"), { certainty: "not_sent" }));
      if (binding.disposed) return rejectOperation(contextError(failure("InvalidHandle", "Binding is disposed"), { certainty: "not_sent" }));
      if (binding.stale) return rejectOperation(contextError(failure("BindingStale", "Binding belongs to an earlier authorization context"), { certainty: "not_sent" }));
      return this.enqueue({ binding, start: () => this.runtime.call_concurrent(binding.id, json), transform }, json, options);
    };
    const tool={
      call: (args, options) => {
        try { assertJson(args); return submit(JSON.stringify(args), options, raw => this.parseOutcome(raw)); }
        catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); }
      },
      callRaw: (json, options) => submit(json, options, raw => raw),
      validate: (args) => {
        this.assertReady();
        if (binding.stale) throw failure("BindingStale", "Binding belongs to an earlier authorization context");
        if (binding.disposed) throw failure("InvalidHandle", "Binding is disposed");
        assertJson(args);
        return parseSafe(this.runtime.validate(binding.id, JSON.stringify(args)));
      },
      validateRaw: (json) => {
        this.assertReady();
        if (binding.stale) throw failure("BindingStale", "Binding belongs to an earlier authorization context");
        if (binding.disposed) throw failure("InvalidHandle", "Binding is disposed");
        return this.runtime.validate(binding.id, json);
      },
      dispose: () => { binding.disposed = true; this.releaseBinding(binding); },
    };
    this.bindingRecords.set(tool,binding);
    return tool;
  }
  bindingCurrent(tool) {
    const binding=this.bindingRecords.get(tool);
    return Boolean(binding&&!binding.disposed&&!binding.stale&&binding.epoch===this.scopeEpoch);
  }

  // Kept for low-level callers that already own a runtime binding ID.
  callRaw(id, json, options) {
    return this.enqueue({ start: () => this.runtime.call_concurrent(id, json), transform: raw => raw }, json, options);
  }

  execute(tool, argv, options) {
    try {
      if (typeof tool !== "string") throw new TypeError("tool must be a string");
      if (!Array.isArray(argv) || !argv.every(value => typeof value === "string")) throw new TypeError("argv must be an array of strings");
      assertJson(argv);
      const json = JSON.stringify(argv);
      return this.enqueue({
        command: true,
        start: () => this.runtime.command_concurrent(tool, json),
        transform: raw => this.parseOutcome(raw),
      }, tool + json, options);
    } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); }
  }

  executeLine(line, options) {
    if (typeof line !== "string") return rejectOperation(contextError(new TypeError("Command line must be a string"), { certainty: "not_sent" }));
    return this.enqueue({ command: true, start: () => this.runtime.command_line_concurrent(line), transform: raw => this.parseOutcome(raw) }, line, options);
  }

  resume(continuation, responses, options) {
    try {
      if (!this.continuations.has(continuation)) throw failure("InvalidContinuation", "Continuation belongs to another remote or was reconstructed; use its original handle");
      assertJson(responses);
      const json = JSON.stringify(responses);
      return this.enqueue({ resume: true, guard: () => this.continuations.has(continuation), start: () => {
        const operation = this.runtime.resume_concurrent(continuation.id, json);
        this.continuations.delete(continuation);
        this.continuationIds.delete(continuation.id);
        return operation;
      }, transform: raw => this.parseOutcome(raw) }, json, options);
    } catch (error) { return rejectOperation(contextError(error, { certainty: "not_sent" })); }
  }

  resumeRaw(id, json, options) {
    return this.enqueue({ resume: true, start: () => {
      const operation = this.runtime.resume_concurrent(id, json);
      this.continuationIds.delete(id);
      return operation;
    }, transform: raw => raw }, json, options);
  }

  releaseContinuation(continuation) {
    this.assertOpen();
    if (!this.continuations.has(continuation)) throw failure("InvalidContinuation", "Continuation belongs to another remote");
    this.runtime.release_continuation(continuation.id);
    this.continuations.delete(continuation);
    this.continuationIds.delete(continuation.id);
  }

  releaseBinding(binding) {
    if (!binding.disposed || binding.leases > 0 || !this.bindings.has(binding)) return;
    this.runtime.release_binding(binding.id);
    this.bindings.delete(binding);
  }

  enqueue(spec, json, { signal, timeout = this.timeout } = {}) {
    try {
      this.assertOpen();
      if(this.suspended&&!this.reconnectJob&&!spec.reconnect&&!spec.cleanup)throw failure("TransportClosed","reconnect must complete before new operations");
      integer(timeout, 1, 2_147_483_647, "timeout");
      if (signal && (typeof signal.addEventListener !== "function" || typeof signal.removeEventListener !== "function")) throw new TypeError("signal must be an AbortSignal");
      if (signal?.aborted) throw failure("Cancelled", "Operation cancelled before admission");
      if (typeof json !== "string") throw new TypeError("Raw arguments must be a JSON string");
      if (this.nextOperation > Number.MAX_SAFE_INTEGER) throw failure("LimitExceeded", "Operation IDs exhausted");
      if(spec.notificationControl==="resume"&&this.resumeCount)throw failure("QueueFull","A notification resume is already pending");
      if (spec.cleanup ? this.cleanupCount>=2 : this.active.size + this.queue.length - this.cleanupCount >= this.queueCapacity || json.length > this.maxInputBytes) throw failure("QueueFull", "Remote input queue is full");
      const bytes = new TextEncoder().encode(json).byteLength + 128;
      if (spec.cleanup ? this.cleanupBytes+bytes>512 : bytes > this.maxInputBytes - this.inputBytes) throw failure("QueueFull", "Remote input byte budget is full");
      this.operationEvents.reserve(this.nextOperation);
      const job = { ...spec, id: this.nextOperation++, bytes, signal, deadline: performance.now() + timeout, controller: new AbortController(), accepted: null, done: false };
      const promise = job.promise = new Promise((resolve, reject) => { job.resolve = resolve; job.reject = reject; });
      // IDs are host queue IDs. Core operation IDs remain separately available in events.
      job.detachCancellation = attachCancellation(promise, job.id, () => this.cancelJob(job, failure("Cancelled", "Operation cancelled; execution may be unknown")));
      if(job.cleanup){this.cleanupCount++;this.cleanupBytes+=bytes;}else this.inputBytes += bytes;
      if(job.notificationControl==="resume"){this.resumeCount++;this.notifications.holdResume();}
      if (job.binding) job.binding.leases++;
      job.abort = () => this.cancelJob(job, failure("Cancelled", "Operation cancelled; execution may be unknown"));
      signal?.addEventListener("abort", job.abort, { once: true });
      job.timer = setTimeout(() => this.cancelJob(job, failure("Timeout", "Operation deadline exceeded; execution may be unknown")), timeout);
      this.queue.push(job);
      this.operationEvents.queued(job.id);
      this.schedule();
      return promise;
    } catch (error) { return rejectOperation(contextError(hostError(error), { certainty: "not_sent" })); }
  }

  schedule() {
    if (this.scheduled || this.closed) return;
    this.scheduled = true;
    queueMicrotask(() => {
      this.scheduled = false;
      while (!this.closed && this.queue.length) {
        const cleanup=this.queue.findIndex(job=>job.cleanup);
        const index=cleanup<0?0:cleanup;
        const job = this.queue[index];
        if (performance.now() >= job.deadline) {
          this.cancelJob(job, failure("Timeout", "Operation deadline exceeded before dispatch"));
          continue;
        }
        const normal=[...this.active].filter(active=>!active.cleanup);
        if(!job.cleanup&&this.sessionRecovery.pending)break;
        if (!job.cleanup&&(normal.length>=this.maxInFlight || job.control&&normal.length || normal.some(active=>active.control))) break;
        this.queue.splice(index,1);
        this.active.add(job);
        void this.work(job);
        if (job.control) break;
      }
    });
  }

  collect(job) {
    if(job.local&&!job.metadata)return;
    if (job.metadata || !job.started || job.accepted || job.coreId === undefined && !job.control) return;
    try {
      const raw = job.control ? (this.runtime.busy() ? undefined : this.runtime.outcome()) : this.runtime.completion(job.coreId);
      if (raw !== undefined && raw !== null) job.accepted = { raw };
    } catch (error) { job.accepted = { error: hostError(error) }; }
  }

  publish() {
    this.sessionRecovery.observe();
    try {
      const change = parseSafe(this.runtime.catalog_change());
      if (change.kind === "changed") {
        if (change.published) this.catalogEpoch = Symbol();
        if (change.invalidated) this.catalogContext = Symbol();
        this.catalogChanges.change(change);
        try { this.onCatalogChange?.(Object.freeze({published:change.published, invalidated:change.invalidated})); }
        catch (error) { this.eventError(error); }
      } else if (change.kind === "closed") this.catalogChanges.close();
    } catch (error) { this.eventError(error); }
    let batch;
    try { batch = parseSafe(this.runtime.events()); }
    catch (error) { this.eventError(error); return; }
    if (batch.dropped) {
      try { this.onEvent({ operation_id: 0, kind: "lagged", payload: { source: "core", dropped: batch.dropped } }); }
      catch (error) { this.eventError(error); }
    }
    for (const wire of batch.events) {
      const { payload_json: raw, ...event } = wire;
      try { event.payload = parseSafe(raw); }
      catch { event.raw_payload = raw; event.payload_error = "NumberOutOfRange"; }
      const job = [...this.active].find(active => active.coreId !== undefined && String(active.coreId) === String(event.operation_id));
      if (job && ["completed", "tool_failed", "needs_input"].includes(event.kind)) {
        job.execution = event.kind === "needs_input" ? "needs_input" : "result_received";
        job.resultSummary = event.payload;
      }
      const owner = job ?? (event.operation_id !== 0 ? [...this.active].find(active => active.metadata || active.control && active.started) : undefined);
      if (owner && ["failed","cancelled"].includes(event.kind) && typeof event.payload?.code === "string") event.payload = {...event.payload,queue_operation_id:owner.id};
      const linked = owner ? { ...event, queue_operation_id: owner.id } : event;
      try { this.onEvent(linked); } catch (error) { this.eventError(error); }
    }
  }
  eventError(error) { try { this.onEventError(error); } catch { /* Observers do not own transport state. */ } }

  checkDeadline(job) {
    if(job.internalRecovery&&performance.now()>=job.deadline){job.cancelCause??=failure("Timeout","HTTP session renewal deadline exceeded");job.controller.abort();return;}
    if (!job.accepted && performance.now() >= job.deadline) this.cancelJob(job, failure("Timeout", "Operation deadline exceeded; execution may be unknown"));
  }

  async waitEvents(job) {
    this.checkDeadline(job);
    const signal = job.controller.signal;
    if (signal.aborted) throw job.cancelCause;
    let abort;
    const cancelled = new Promise((_, reject) => {
      abort = () => reject(job.cancelCause);
      signal.addEventListener("abort", abort, { once: true });
    });
    try { await Promise.race([this.eventGate(signal), cancelled]); }
    finally { signal.removeEventListener("abort", abort); }
    this.checkDeadline(job);
    if (signal.aborted) throw job.cancelCause;
  }

  ensure(job) {
    if(job.internalRecovery&&this.closed)throw failure("TransportClosed","Remote closed during session renewal");
    this.checkDeadline(job);
    if (job.controller.signal.aborted) throw job.cancelCause;
    if(this.suspended&&!job.reconnect&&!job.cleanup)throw failure("TransportClosed","reconnect must complete before new operations");
  }
  async preflight(job) {
    this.ensure(job);
    this.sessionRecovery.wait(job);
    if(job.recoveredConnect)return;
    job.credential = await this.credentials.resolve(this.runtime.endpoint(), job.controller.signal);
    this.ensure(job);this.sessionRecovery.wait(job); this.tick();
    let status = this.runtime.catalog_status(job.credential.authContextId);
    if (typeof status === "string") status = parseSafe(status);
    const forceRefresh = job.refresh && (job.refreshAt === this.catalogEpoch || job.refreshContext !== this.catalogContext);
    if (status.requires_exclusive || status.needs_refresh || forceRefresh || job.connect) {
      // Cleanup controls remain independent of the metadata barrier. In
      // particular an old resume must not delay invalidating its identity.
      const running = [...this.active].filter(other => other !== job && !other.cleanup && (other.started || other.metadata || other.sent));
      await abortable(Promise.allSettled(running.map(other => other.promise)), job.controller.signal);
      this.ensure(job);this.sessionRecovery.wait(job); this.tick();
      status = this.runtime.catalog_status(job.credential.authContextId);
      if (typeof status === "string") status = parseSafe(status);
    }
    if (status.authorization_changed || job.connect) {
      await this.notifications.stop();
      this.ensure(job);
      await this.closeSession(job.controller.signal);
      this.ensure(job);
    }
    if (this.runtime.accept_authorization(job.credential.authContextId)) {
      this.catalogContext = Symbol();
      this.scopeEpoch = Symbol();
      for (const binding of this.bindings) binding.stale = true;
      this.bindings.clear();
      this.continuations = new WeakSet(); this.continuationIds.clear();
    }
    if (job.binding && (job.binding.stale || job.binding.epoch !== this.scopeEpoch)) throw failure("BindingStale", "Authorization changed; bind the tool again");
    if (job.resume && (status.authorization_changed || job.guard && !job.guard())) throw failure("InvalidContinuation", "Authorization changed; continuation was invalidated");
    if (!job.connect && (status.needs_refresh || forceRefresh)) {
      job.metadata = true;
      this.runtime.refresh_catalog();
      while (this.runtime.busy()) {
        this.ensure(job);
        const actions = parseSafe(this.runtime.actions()).filter(action => action.kind !== "cancel");
        if (!actions.length) throw failure("HostError", "Catalog refresh has no host action");
        for (const action of actions) { job.action = action; await this.io(job, action); }
      }
      this.ensure(job);
      job.metadata = false; job.action = undefined; job.sent = false;
    }
  }

  async closeSession(signal) {
    const session = this.session;
    if (!session) return;
    const controller = new AbortController();
    const abort = () => controller.abort();
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) controller.abort();
    const timer = setTimeout(abort, 500);
    try {
      const headers = new Headers({ "mcp-session-id": session.id, "mcp-protocol-version": "2025-11-25" });
      if (session.credential?.token) headers.set("authorization", `Bearer ${session.credential.token}`);
      const response = await this.fetch(this.runtime.endpoint(), { method: "DELETE", headers, signal: controller.signal, redirect: "error" });
      await response.body?.cancel();
      if (!response.ok && ![404,405].includes(response.status)) throw new Error();
      if (this.session === session) this.session = undefined;
    } catch { throw failure("SessionCleanupFailed", "Could not close the previous MCP session"); }
    finally { clearTimeout(timer); signal?.removeEventListener("abort", abort); }
  }

  async work(job) {
    let unlock,deferred=false;
    try {
      if(!job.ownerStarted){job.ownerStarted=true;this.operationEvents.start(job.id);}
      this.ensure(job);
      if(!job.skipEventGate)await this.waitEvents(job);
      if(!job.skipAdmission)unlock = await this.admission.acquire(job.controller.signal);
      if(!job.reconnect)this.sessionRecovery.wait(job);
      if(job.reconnect)await this.resetTransport(job);
      if(job.binding?.stale)throw failure("BindingStale","Binding belongs to an earlier transport or authorization context");
      if(job.resume&&job.guard&&!job.guard())throw failure("InvalidContinuation","Continuation was invalidated");
      if(!job.skipPreflight)await this.preflight(job);
      if(job.recoveredConnect){job.accepted={raw:"null"};return;}
      if (job.accepted) return;
      this.tick();
      job.started = true;
      if(!job.skipPreflight&&!job.notificationControl)this.ensureNotifications(job.credential);
      const value = job.start(job);
      if(job.asyncLocal){const result=await value;this.ensure(job);job.accepted={value:result};}
      else if (job.local) job.accepted = performance.now() >= job.deadline
        ? { error: failure("Timeout", "Operation deadline exceeded before returning local metadata") }
        : { value };
      else if (job.command) {
        const admission = typeof value === "string" ? parseSafe(value) : value;
        if (admission.kind === "help") job.accepted = performance.now() >= job.deadline
          ? { error: failure("Timeout", "Command deadline exceeded before returning help") }
          : { value: { kind: "help", value: admission.value } };
        else if (admission.kind === "operation") job.coreId = admission.value;
        else throw failure("HostError", "Invalid command admission");
      }
      else if (job.control && value !== undefined && value !== null) job.accepted = { value: { kind: "help", value } };
      else if (!job.control) job.coreId = value;
      this.publish();
      unlock?.(); unlock = undefined;
      while (!job.accepted) {
        const actions = parseSafe(this.runtime.actions());
        const requests = actions.filter(action => action.kind !== "cancel");
        if (!requests.length) { this.collect(job); if (!job.accepted) throw failure("HostError", "Active operation has no host action"); }
        for (const action of requests) {
          job.action = action;
          if (action.rpc_method === "tools/call") job.execution = "unknown";
          await this.io(job, action);
          this.collect(job);
          if (job.accepted) break;
        }
      }
      if(job.reconnect&&!job.accepted?.error){this.suspended=false;this.sessionRecovery.reconnected();}
      if(job.connect&&!job.accepted?.error)this.ensureNotifications(job.credential,true);
    } catch (error) {
      this.sessionRecovery.observe();
      if(this.sessionRecovery.defer(job)){
        deferred=true;
      } else
      if (!job.accepted) {
        if (job.metadata || job.control && job.started && !job.local) {
          this.runtime.fail("Host I/O failed; execution may be unknown");
          job.accepted = { error: hostError(error) };
        } else if (job.action) {
          try { this.runtime.fail_request(job.action.id, "Host I/O failed; execution may be unknown"); } catch { /* The decoder may have already accepted a terminal error. */ }
          this.collect(job);
        }
        if (!job.accepted) job.accepted = { error: hostError(error) };
      }
    } finally {
      if(deferred){
        if(job.metadata&&this.runtime.busy())this.runtime.fail("Expired metadata attempt ended");
        if(job.connect)this.sessionRecovery.origin=job;
        job.metadata=false;job.started=false;job.sent=false;job.action=undefined;job.credential=undefined;
        this.active.delete(job);this.queue.push(job);this.queue.sort((a,b)=>a.id-b.id);
        unlock?.();this.publish();this.schedule();return;
      }
      if(this.reconnectJob===job)this.reconnectJob=undefined;
      unlock?.();
      this.publish();
      await job.cancelCleanup;
      this.finish(job);
    }
  }

  async io(job, action) {
    if(this.responseRecovery&&resumableResponse(action)) {
      const response=job.response=new ToolResponse(this,job,action,this.responseRecovery);
      try{await response.run();}finally{delete job.response;}
      return;
    }
    this.checkDeadline(job);
    if (job.controller.signal.aborted) throw job.cancelCause;
    const headers = new Headers(action.headers);
    const token = job.credential?.token;
    if (token) headers.set("authorization", `Bearer ${token}`);
    job.sent = true;
    let response;
    try{response=await this.fetch(action.url, { method: "POST", headers, body: action.body, redirect: "error", signal: job.controller.signal });}
    catch(error){if(job.internalRecovery&&!job.controller.signal.aborted)throw failure("TransportError","Session discovery transport failed");throw error;}
    this.checkDeadline(job);
    if (job.controller.signal.aborted) { await response.body?.cancel(); throw job.cancelCause; }
    this.tick();
    let reader;
    try {
      const done = this.runtime.begin_response(action.id, response.status, JSON.stringify(Object.fromEntries(response.headers)));
      if (action.headers?.["mcp-protocol-version"] === "2025-11-25" && response.headers.has("mcp-session-id")) {
        this.session = { id: response.headers.get("mcp-session-id"), credential: job.credential };
      }
      this.collect(job);
      this.publish();
      if (done) return;
      reader = response.body?.getReader();
      if (!reader) throw failure("HostError", "Missing response body");
      while (true) {
        await this.waitEvents(job);
        let chunk;
        try{chunk=await reader.read();}
        catch(error){if(job.internalRecovery&&!job.controller.signal.aborted)throw failure("TransportError","Session discovery stream failed");throw error;}
        this.checkDeadline(job);
        if (job.controller.signal.aborted) throw job.cancelCause;
        this.tick();
        const complete = chunk.done ? (this.runtime.finish_response_for(action.id), true) : this.runtime.push_response_for(action.id, chunk.value);
        // Accept the result before callbacks can request cancellation.
        this.collect(job);
        this.publish();
        if (complete) break;
      }
    } finally {
      if (reader) {
        // A broken stream can reject both read and cancel. Renewal must retain
        // the original transport/protocol evidence when releasing the reader.
        try { await reader.cancel(); }
        catch(error){if(!job.internalRecovery)throw error;}
        finally { reader.releaseLock(); }
      } else {
        try{await response.body?.cancel();}catch(error){if(!job.internalRecovery)throw error;}
      }
    }
  }

  cancelJob(job, cause) {
    if (job.done || job.cancelCause) return;
    this.sessionRecovery.cancelConnect(job,cause);
    if (!this.active.has(job)) {
      const index = this.queue.indexOf(job);
      if (index < 0) return;
      this.queue.splice(index, 1);
      job.accepted = { error: cause };
      this.finish(job);
      return;
    }
    this.collect(job);
    if (job.accepted) {if(cause.code==="TransportClosed")void job.response?.stop();return;}
    job.cancelCause = cause;
    try {
      if (job.metadata || job.started && !job.local) {
        if (cause.code === "Timeout" && job.action) this.runtime.timeout_request(job.action.id);
        else if (job.control || job.metadata) this.runtime.cancel();
        else this.runtime.cancel_operation(job.coreId);
        this.collect(job);
      }
    } catch { /* A terminal result can win the cancellation race. */ }
    job.accepted = { error: cause };
    if (job.metadata || job.started && !job.local) this.runtime.actions(); // Cancellation directives are handled by this host below.
    job.controller.abort();
    job.cancelCleanup = Promise.all([job.response?job.response.stop().then(()=>this.cancelWire(job)):this.cancelWire(job),job.cancelLocal?.()]);
    this.publish();
  }

  async cancelWire(job) {
    const action = job.cancelAction??job.action;
    if (!job.sent || !action?.cancellation) return;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 200);
    try {
      const headers = new Headers(action.headers);
      if (job.credential?.token) headers.set("authorization", `Bearer ${job.credential.token}`);
      const response = await (job.cancelFetch??this.fetch)(action.url, { method: "POST", headers, redirect: "error", signal: controller.signal, body: action.cancellation });
      await response.body?.cancel();
    } catch { /* Cancellation is best effort and cannot roll back a tool. */ }
    finally { clearTimeout(timer); controller.abort(); }
  }

  finish(job) {
    if (job.done) return;
    job.done = true;
    clearTimeout(job.timer);
    job.signal?.removeEventListener("abort", job.abort);
    this.active.delete(job);
    if(job.cleanup){this.cleanupCount--;this.cleanupBytes-=job.bytes;}else this.inputBytes -= job.bytes;
    if(job.notificationControl==="resume"){this.resumeCount--;this.notifications.releaseResume();}
    if (job.binding) { job.binding.leases--; this.releaseBinding(job.binding); }
    const accepted = job.accepted;
    let value, error;
    try {
      if (accepted.error) throw accepted.error;
      value = Object.hasOwn(accepted,"value") ? accepted.value : job.transform(accepted.raw);
    } catch (cause) {
      error = materializeError(contextError(hostError(cause), { operationId:job.coreId,queueOperationId:job.id,certainty:job.execution ?? "not_sent" }));
    }
    this.operationEvents.finish(job.id, {coreId:error?.operation_id ?? job.coreId,certainty:error?.execution_certainty ?? job.execution ?? "not_sent",error,summary:job.resultSummary});
    if (error) job.reject(error); else job.resolve(value);
    releaseOperation(job);
    this.schedule();
  }

  cancel() {
    const cause = failure("Cancelled", "Operation cancelled; execution may be unknown");
    for (const job of [...this.queue, ...this.active]) this.cancelJob(job, cause);
  }

  cancelOperation(operationId, code = "Cancelled") {
    if (!["Cancelled", "Timeout"].includes(code)) throw new RangeError("Unsupported interruption code");
    const job = [...this.queue, ...this.active].find(job => job.id === operationId);
    if (job) this.cancelJob(job, failure(code, code === "Timeout" ? "Operation deadline exceeded; execution may be unknown" : "Operation cancelled; execution may be unknown"));
  }

  async close() {
    this.closed = true;
    this.httpNotifications=false;
    const recovering=this.sessionRecovery.stop();
    const active = [...this.active];
    for (const job of [...this.queue, ...active]) this.cancelJob(job, failure("TransportClosed", "Remote is closed; execution may be unknown"));
    await Promise.allSettled(active.map(job => job.promise));
    await recovering;
    for (const binding of [...this.bindings]) { binding.disposed = true; this.releaseBinding(binding); }
    for (const id of this.continuationIds.keys()) this.runtime.release_continuation(id);
    this.continuationIds.clear();
    try { await this.notifications.stop(); await this.closeSession(); }
    finally { this.credentials.close(); this.publish(); this.catalogChanges.close(); }
  }
}
