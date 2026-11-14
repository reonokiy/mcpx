import { VERSION, failure, integer } from "./worker-protocol.js";

const terminal = event => ["catalog_updated", "completed", "tool_failed", "failed", "cancelled", "needs_input"].includes(event.kind);
const LOSS_BYTES = 512;
const RECORD_BYTES = 16 * 1024;

// Pending and posted-but-unacknowledged records share the same two budgets.
export class WorkerEvents {
  constructor(post, { maxInFlight = 16, eventCapacity = 256, eventBytes = 4 * 1024 * 1024 } = {}) {
    integer(maxInFlight, 1, 16, "maxInFlight");
    this.reserve = maxInFlight + 2;
    this.capacity = integer(eventCapacity, 2 * maxInFlight + 4, 4096, "eventCapacity");
    this.byteLimit = integer(eventBytes, (this.reserve + 1) * RECORD_BYTES, 64 * 1024 * 1024, "eventBytes");
    this.post = post;
    this.pending = [];
    this.sent = new Map();
    this.waiters = new Set();
    this.count = 0;
    this.bytes = 0;
    this.peakCount = 0;
    this.peakBytes = 0;
    this.dropped = 0;
    this.totalDropped = 0;
    this.coreDropped = 0;
    this.totalCoreDropped = 0;
    this.batch = 1;
    this.scheduled = false;
    this.closed = false;
    this.waitCount = 0;
    this.peakWaiters = 0;
  }
  ready() { return this.count < this.capacity - this.reserve && this.bytes < this.byteLimit - this.reserve * RECORD_BYTES; }
  wait(signal) {
    if (this.closed || signal.aborted) return Promise.reject(failure("TransportClosed", "Event channel is closed or operation was interrupted"));
    if (this.dropped || this.coreDropped) this.schedule();
    if (this.ready()) return;
    this.waitCount++;
    return new Promise((resolve, reject) => {
      const waiter = { resolve: () => { cleanup(); resolve(); } };
      const abort = () => { cleanup(); reject(failure("Cancelled", "Event wait interrupted")); };
      const cleanup = () => { this.waiters.delete(waiter); signal.removeEventListener("abort", abort); };
      this.waiters.add(waiter);
      this.peakWaiters = Math.max(this.peakWaiters, this.waiters.size);
      signal.addEventListener("abort", abort, { once: true });
    });
  }
  push(event) {
    if (this.closed) return;
    if (event.kind === "lagged" && event.payload?.source === "core") {
      const count = integer(event.payload.dropped, 1, Number.MAX_SAFE_INTEGER, "core dropped count");
      if (count > Number.MAX_SAFE_INTEGER - this.coreDropped || count > Number.MAX_SAFE_INTEGER - this.totalCoreDropped) throw failure("ConsumerLagged", "Core event loss counter exhausted");
      this.coreDropped += count; this.totalCoreDropped += count; this.schedule(); return;
    }
    const bytes = new TextEncoder().encode(JSON.stringify(event)).byteLength;
    const protectedRecord = terminal(event);
    const recordLimit = protectedRecord ? this.capacity : this.capacity - this.reserve;
    const byteLimit = protectedRecord ? this.byteLimit : this.byteLimit - this.reserve * RECORD_BYTES;
    if (bytes > RECORD_BYTES || this.count + 1 > recordLimit || this.bytes + bytes > byteLimit) {
      if (protectedRecord) throw failure("ConsumerLagged", "Worker event terminal reservation exhausted");
      if (this.totalDropped === Number.MAX_SAFE_INTEGER) throw failure("ConsumerLagged", "Worker event loss counter exhausted");
      this.dropped++; this.totalDropped++; this.schedule();
      return;
    }
    this.pending.push({ event, bytes });
    this.count++; this.bytes += bytes;
    this.peakCount = Math.max(this.peakCount, this.count);
    this.peakBytes = Math.max(this.peakBytes, this.bytes);
    this.schedule();
  }
  schedule() {
    if (this.closed) return;
    if (!this.scheduled) {
      this.scheduled = true;
      queueMicrotask(() => { this.scheduled = false; this.flush(); });
    }
  }
  flush() {
    while (this.pending.length) {
      const records = this.pending.splice(0, 32);
      if (this.batch > Number.MAX_SAFE_INTEGER) throw failure("LimitExceeded", "Event batch IDs exhausted");
      const batch = this.batch++;
      this.sent.set(batch, { count: records.length, bytes: records.reduce((sum, record) => sum + record.bytes, 0) });
      this.post({ v: VERSION, kind: "events", batch, events: records.map(record => record.event), dropped: this.dropped, coreDropped: this.coreDropped });
      this.dropped = 0; this.coreDropped = 0;
    }
    // A loss-only report owns one counted credit and at most one may be in
    // flight. More loss accumulates until ACK; a stopped consumer cannot grow it.
    if ((this.dropped || this.coreDropped) && this.lossBatch === undefined && this.count < this.capacity && this.bytes + LOSS_BYTES <= this.byteLimit) {
      if (this.batch > Number.MAX_SAFE_INTEGER) throw failure("LimitExceeded", "Event batch IDs exhausted");
      const batch = this.batch++;
      this.lossBatch = batch;
      this.count++; this.bytes += LOSS_BYTES;
      this.peakCount = Math.max(this.peakCount, this.count); this.peakBytes = Math.max(this.peakBytes, this.bytes);
      this.sent.set(batch, { count: 1, bytes: LOSS_BYTES, lossOnly: true });
      this.post({ v: VERSION, kind: "events", batch, events: [], dropped: this.dropped, coreDropped: this.coreDropped });
      this.dropped = 0; this.coreDropped = 0;
    }
  }
  acknowledge(batch) {
    const records = this.sent.get(batch);
    if (!records) return; // Duplicate/late acknowledgements do not free more credit.
    this.sent.delete(batch);
    if (records.lossOnly) this.lossBatch = undefined;
    this.count -= records.count; this.bytes -= records.bytes;
    if (this.dropped || this.coreDropped) this.schedule();
    if (this.ready()) for (const waiter of [...this.waiters]) waiter.resolve();
  }
  snapshot() {
    return { eventRecords: this.count, eventBytes: this.bytes, peakEventRecords: this.peakCount, peakEventBytes: this.peakBytes, droppedEvents: this.totalDropped, coreDroppedEvents: this.totalCoreDropped, eventWaits: this.waitCount, peakEventWaiters: this.peakWaiters };
  }
  close() {
    this.flush();
    this.closed = true;
    for (const waiter of [...this.waiters]) waiter.resolve();
    const loss = { dropped: this.dropped, coreDropped: this.coreDropped };
    this.dropped = 0; this.coreDropped = 0;
    return loss;
  }
}
