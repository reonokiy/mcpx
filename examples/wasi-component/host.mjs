import { contextError } from "../../web/errors.js";
// A small host for the typed WIT interface. MCP stays inside the component;
// the host forwards byte requests, supplies byte responses and observes events.
const failure = (code, message) => Object.assign(new Error(message), { code });
const sessions = new WeakMap(), driving = new WeakSet();

export async function closeComponent(remote) {
  if (driving.has(remote)) throw failure("Busy", "Wait for the component driver before closing");
  const session = sessions.get(remote);
  if (session) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 500);
    try {
      const headers = session.headers.filter(header => !["content-type", "mcp-method", "mcp-name", "mcp-session-id"].includes(header.name.toLowerCase())).map(header => [header.name, header.value]);
      headers.push(["mcp-session-id", session.id]);
      const response = await session.fetch(session.url, { method: "DELETE", headers, signal: controller.signal, redirect: "error" });
      await response.body?.cancel();
      if (!response.ok && ![404, 405].includes(response.status)) throw new Error();
      sessions.delete(remote);
    } catch { throw failure("SessionCleanupFailed", "Could not close the component's legacy session"); }
    finally { clearTimeout(timer); }
  }
  remote[Symbol.dispose]();
}

export async function driveComponent(remote, {
  fetch: fetchImpl = globalThis.fetch, signal, timeout = 60_000,
  onEvents = () => {}, onEventError = () => {}, onEvent,
} = {}) {
  if (onEvent !== undefined) throw new TypeError("Use onEvents(batch) to observe records and loss counts");
  if (!Number.isSafeInteger(timeout) || timeout < 1 || timeout > 2_147_483_647) throw new RangeError("Invalid timeout");
  if (typeof fetchImpl !== "function" || typeof onEvents !== "function" || typeof onEventError !== "function") throw new TypeError("Host callbacks must be functions");
  if (signal && (typeof signal.addEventListener !== "function" || typeof signal.removeEventListener !== "function")) throw new TypeError("signal must be an AbortSignal");
  if (driving.has(remote)) throw failure("Busy", "The component already has a driver");
  driving.add(remote);
  const active = new Map(), queued = [];
  let cause;
  const tick = () => remote.advanceTime(BigInt(Math.floor(performance.now())));
  const observe = () => {
    const batch = remote.events();
    if (batch.events.length || batch.dropped) {
      try { onEvents(batch); } catch (error) { try { onEventError(error); } catch { /* Observers do not own I/O. */ } }
    }
    for (const action of remote.actions()) {
      if (action.tag === "cancel") {
        const record = active.get(action.val);
        if (record) cancelRecord(record);
      } else queued.push(action);
    }
  };
  const abort = () => {
    cause ??= failure("Cancelled", "Component I/O cancelled; execution may be unknown");
    if (cause.code === "Timeout") {
      const ids = [...active.keys(), ...queued.filter(action => action.tag === "http").map(action => action.val.actionId)];
      for (const id of ids) { try { remote.timeoutRequest(id); } catch { /* A terminal may already have won. */ } }
    }
    remote.cancel();
    for (const record of active.values()) cancelRecord(record);
    observe();
  };
  const timer = setTimeout(() => { cause = failure("Timeout", "Component I/O deadline exceeded; execution may be unknown"); abort(); }, timeout);
  signal?.addEventListener("abort", abort, { once: true });
  const transport = async run => {
    try { return await run(); }
    catch { throw failure("HostError", "HTTP I/O failed; execution may be unknown"); }
  };
  const cancelRecord = record => {
    record.cancelled = true; record.controller.abort();
    if (!record.sent || !record.request.cancellation || record.cancelWire) return;
    record.cancelWire = (async () => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 200);
      try {
        const response = await fetchImpl(record.request.url, {
          method: "POST", headers: record.request.headers.map(header => [header.name, header.value]),
          body: record.request.cancellation, signal: controller.signal, redirect: "error",
        });
        await response.body?.cancel();
      } catch { /* The core-provided cancellation packet is best effort. */ }
      finally { clearTimeout(timer); controller.abort(); }
    })();
  };
  const perform = async action => {
    if (action.tag !== "http") throw failure("CapabilityUnsupported", "This example host supports HTTP actions only");
    const request = action.val;
    const record = { controller: new AbortController(), request };
    let response, reader;
    active.set(request.actionId, record);
    try {
      if (cause) throw cause;
      tick();
      record.sent = true;
      response = await transport(() => fetchImpl(request.url, {
        method: request.method.toUpperCase(),
        headers: request.headers.map(header => [header.name, header.value]),
        body: request.method === "get" ? undefined : request.body,
        signal: record.controller.signal, redirect: "error",
      }));
      if (cause) throw cause;
      tick();
      const completeHeaders = remote.beginResponse(request.actionId, response.status, [...response.headers].map(([name, value]) => ({ name, value })));
      if (request.headers.some(header => header.name === "mcp-protocol-version" && header.value === "2025-11-25") && response.headers.has("mcp-session-id")) {
        sessions.set(remote, { id: response.headers.get("mcp-session-id"), url: request.url, headers: request.headers, fetch: fetchImpl });
      }
      if (completeHeaders) return;
      reader = response.body?.getReader();
      if (!reader) throw failure("HostError", "Missing HTTP response stream");
      let received = 0, complete = false;
      while (!complete) {
        const chunk = await transport(() => reader.read());
        if (cause) throw cause;
        if (record.cancelled) throw failure("Cancelled", "Component operation cancelled; execution may be unknown");
        tick();
        if (chunk.done) { remote.finishResponseFor(request.actionId); break; }
        received += chunk.value.byteLength;
        if (received > 16 * 1024 * 1024) throw failure("LimitExceeded", "HTTP response exceeds 16 MiB");
        for (let offset = 0; offset < chunk.value.byteLength && !complete; offset += 65536) {
          complete = remote.pushResponseFor(request.actionId, chunk.value.subarray(offset, offset + 65536));
          observe();
          if (record.cancelled && !complete) throw failure("Cancelled", "Component operation cancelled; execution may be unknown");
        }
      }
    } catch (error) {
      // Typed core errors retain their code/pointers. Host transport exceptions
      // are sanitized; no URL, bearer or business body enters the default error.
      const result = cause ?? (record.cancelled ? failure("Cancelled", "Component operation cancelled; execution may be unknown")
        : typeof error?.code === "string" ? error : failure("HostError", "HTTP I/O failed; execution may be unknown"));
      try { remote.failRequest(request.actionId, result.message); } catch { /* The core may already own the terminal. */ }
      throw contextError(result, {
        operationId: request.operationId,
        certainty: result.executionCertainty?.replaceAll("-", "_") ?? (request.rpcMethod === "tools/call" ? "unknown" : "not_sent"),
      });
    } finally {
      if (reader) { try { await reader.cancel(); } catch { /* Abort may already have errored the stream. */ } reader.releaseLock(); }
      else { try { await response?.body?.cancel(); } catch { /* No reader remains. */ } }
      await record.cancelWire;
      active.delete(request.actionId);
      observe();
    }
  };
  try {
    if (signal?.aborted) abort();
    observe();
    while (!cause && (remote.busy() || queued.length)) {
      const actions = queued.splice(0);
      if (!actions.length) throw failure("HostError", "Active component has no host action");
      const results = await Promise.allSettled(actions.map(perform));
      const rejected = results.find(result => result.status === "rejected");
      if (rejected) throw rejected.reason;
      observe();
    }
    if (cause) throw cause;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", abort);
    driving.delete(remote);
  }
}
