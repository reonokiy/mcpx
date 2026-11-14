import { VERSION, integer } from "./worker-protocol.js";

// The UI realm owns cancellation even when the core's worker stops yielding.
export class WorkerFetchHost {
  constructor(post, fetchImpl = globalThis.fetch) {
    this.post = post; this.fetch = fetchImpl.bind(globalThis);
    this.records = new Map(); this.buffered = 0; this.closed = false;
    this.lastId = 0;
  }
  receive(message) {
    if (this.closed) return;
    if (!Number.isSafeInteger(message.id) || message.id < 1) throw new Error("Invalid I/O ID");
    if (message.op === "fetch") {
      if (message.id <= this.lastId) throw new Error("Reused I/O ID");
      this.lastId = message.id;
      if (this.records.size >= 36) { this.send("error", message.id); return; }
      const record = { id: message.id, controller: new AbortController() };
      this.records.set(record.id, record);
      record.opening = this.open(record, message);
    } else {
      const record = this.records.get(message.id);
      if (!record) { if (message.op === "cancel") this.send("closed", message.id); return; }
      if (message.op === "cancel") void this.cancel(record);
      else if (message.op === "pull") {
        if (record.pulling || !record.reader) throw new Error("Invalid I/O pull");
        record.pulling = this.pull(record).finally(() => { record.pulling = undefined; });
      } else throw new Error("Invalid I/O operation");
    }
  }
  send(op, id, fields = {}, transfer = []) { this.post({ v: VERSION, kind: "io", op, id, ...fields }, transfer); }
  releaseBuffer(record) {
    if (record.buffer) this.buffered -= record.buffer.byteLength;
    record.buffer = undefined; record.offset = 0;
  }
  async open(record, message) {
    try {
      const { method, headers, body, maxResponseBytes } = message.init;
      if (!["GET", "POST", "DELETE"].includes(method) || (body !== undefined && typeof body !== "string")) throw new Error();
      if (body && (body.length > 8 * 1024 * 1024 || new TextEncoder().encode(body).byteLength > 8 * 1024 * 1024)) throw new Error();
      record.limit = integer(maxResponseBytes, 1, 64 * 1024 * 1024, "response byte limit");
      record.streaming=message.init.notificationStream??false;
      if(typeof record.streaming!=="boolean")throw new Error();
      if(record.streaming) {
        const fields=new Headers(headers);
        if(!(method==="GET"&&fields.get("mcp-protocol-version")==="2025-11-25" || method==="POST"&&fields.get("mcp-protocol-version")==="2026-07-28"&&fields.get("mcp-method")==="subscriptions/listen"))throw new Error();
      }
      record.received = 0;
      const response = await this.fetch(message.url, { method, headers, body, signal: record.controller.signal, redirect: "error" });
      if (record.cancelled) { await response.body?.cancel(); return; }
      // Some browsers expose an empty stream even for a null-body status.
      // Reconstructing Response(204/205/304) with any stream is forbidden.
      if ([204, 205, 304].includes(response.status)) await response.body?.cancel();
      else record.reader = response.body?.getReader();
      this.send("headers", record.id, { status: response.status, headers: [...response.headers], hasBody: Boolean(record.reader) });
      if (!record.reader) { this.records.delete(record.id); this.send("end", record.id); }
    } catch {
      if (!record.cancelled) { this.records.delete(record.id); this.send("error", record.id); }
    }
  }
  async pull(record) {
    try {
      if (!record.buffer) {
        const chunk = await record.reader.read();
        if (record.cancelled) return;
        if (chunk.done) {
          this.records.delete(record.id); record.reader.releaseLock(); record.reader = undefined;
          this.send("end", record.id); return;
        }
        if(!record.streaming)record.received += chunk.value.byteLength;
        if (record.received > record.limit || this.buffered + chunk.value.byteLength > 8 * 1024 * 1024) throw new Error();
        record.buffer = chunk.value; record.offset = 0;
        this.buffered += chunk.value.byteLength;
      }
      if (record.cancelled) return;
      const end = Math.min(record.offset + 65536, record.buffer.byteLength);
      const bytes = record.buffer.slice(record.offset, end);
      record.offset = end;
      if (end === record.buffer.byteLength) this.releaseBuffer(record);
      this.send("chunk", record.id, { bytes }, [bytes.buffer]);
    } catch {
      if (!record.cancelled) {
        record.cancelled = true; record.controller.abort();
        try { await record.reader?.cancel(); } catch { /* Abort can also reject a pending read. */ }
        try { record.reader?.releaseLock(); } catch { /* A read may still be unwinding. */ }
        this.releaseBuffer(record); this.records.delete(record.id);
        this.send("error", record.id);
      }
    }
  }
  cancel(record) {
    if (record.cleanup) return record.cleanup;
    record.cancelled = true; record.controller.abort();
    record.cleanup = (async () => {
      await record.opening;
      try { await record.reader?.cancel(); } catch { /* The fetch may already be errored. */ }
      await record.pulling;
      try { record.reader?.releaseLock(); } catch { /* No lock remains after EOF. */ }
      this.releaseBuffer(record); this.records.delete(record.id);
      this.send("closed", record.id);
    })();
    return record.cleanup;
  }
  close() {
    if (this.closing) return this.closing;
    this.closed = true;
    this.closing = Promise.allSettled([...this.records.values()].map(record => this.cancel(record))).then(() => {});
    return this.closing;
  }
}
