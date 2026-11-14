import { VERSION, failure } from "./worker-protocol.js";

// Pull-based transport proxy: at most one chunk per reader is crossing the port.
export class WorkerFetch {
  constructor(post) { this.post = post; this.nextId = 1; this.records = new Map(); }
  fetch(url, init = {}) {
    if (this.records.size >= 36 || this.nextId > Number.MAX_SAFE_INTEGER) return Promise.reject(failure("LimitExceeded", "Worker I/O capacity exhausted"));
    if (init.signal?.aborted) return Promise.reject(failure("Cancelled", "Fetch was aborted"));
    const id = this.nextId++;
    const record = { id, signal: init.signal };
    record.headers = new Promise((resolve, reject) => { record.resolve = resolve; record.reject = reject; });
    record.closed = new Promise(resolve => { record.close = resolve; });
    record.abort = () => { void this.cancel(record); };
    this.records.set(id, record);
    init.signal?.addEventListener("abort", record.abort, { once: true });
    this.post({ v: VERSION, kind: "io", op: "fetch", id, url: String(url), init: {
      method: init.method ?? "GET", headers: [...new Headers(init.headers)], body: init.body,
      maxResponseBytes: init.maxResponseBytes ?? 8 * 1024 * 1024,
      notificationStream: init.notificationStream ?? false,
    } });
    return record.headers;
  }
  cancel(record) {
    if (!this.records.has(record.id)) return record.closed;
    if (!record.cancelled) {
      record.cancelled = true;
      this.post({ v: VERSION, kind: "io", op: "cancel", id: record.id });
    }
    return record.closed;
  }
  complete(record, error) {
    if (!this.records.delete(record.id)) return;
    record.signal?.removeEventListener("abort", record.abort);
    if (!record.resolved) record.reject(error ?? failure("HostError", "Missing host response"));
    else if (error) { try { record.controller?.error(error); } catch { /* A consumer may have cancelled the stream. */ } }
    else { try { record.controller?.close(); } catch { /* The consumer already closed its stream. */ } }
    if (error) record.pullReject?.(error); else record.pullResolve?.();
    record.close();
  }
  receive(message) {
    const record = this.records.get(message.id);
    if (!record) return;
    switch (message.op) {
      case "headers": {
        if (record.cancelled) return;
        if (record.resolved) throw failure("WorkerProtocolError", "Duplicate host response headers");
        const stream = message.hasBody ? new ReadableStream({
          start: controller => { record.controller = controller; },
          pull: () => new Promise((resolve, reject) => {
            record.pullResolve = resolve; record.pullReject = reject;
            this.post({ v: VERSION, kind: "io", op: "pull", id: record.id });
          }),
          cancel: () => this.cancel(record),
        }, { highWaterMark: 0 }) : null;
        record.resolved = true;
        record.resolve(new Response(stream, { status: message.status, headers: message.headers }));
        break;
      }
      case "chunk": {
        if (record.cancelled) return;
        if (!(message.bytes instanceof Uint8Array) || message.bytes.byteLength > 65536 || !record.pullResolve) throw failure("WorkerProtocolError", "Invalid host I/O chunk");
        record.controller.enqueue(message.bytes);
        const resolve = record.pullResolve;
        record.pullResolve = record.pullReject = undefined;
        resolve(); break;
      }
      case "end": this.complete(record); break;
      case "closed": this.complete(record, failure("Cancelled", "Host I/O was aborted")); break;
      case "error": this.complete(record, failure("HostError", "Host HTTP I/O failed; execution may be unknown")); break;
      default: throw failure("WorkerProtocolError", "Invalid host I/O message");
    }
  }
}
