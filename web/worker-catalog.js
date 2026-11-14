import { VERSION, failure } from "./worker-protocol.js";

// One posted record plus one coalesced pending state, independent of event ACKs.
export class WorkerCatalog {
  constructor(post) { this.post = post; this.next = 1; this.closed = false; }
  change(value) {
    if (this.closed) return;
    this.pending = {published:Boolean(this.pending?.published || value.published), invalidated:Boolean(this.pending?.invalidated || value.invalidated)};
    this.flush();
  }
  take() {
    if (!this.pending) return undefined;
    if (this.next > Number.MAX_SAFE_INTEGER) throw failure("LimitExceeded", "Catalog message IDs exhausted");
    const record = {id:this.next++, ...this.pending};
    this.pending = undefined;
    return record;
  }
  flush() {
    if (this.inFlight || this.closed) return;
    const record = this.take();
    if (record) { this.inFlight = record.id; this.post({v:VERSION, kind:"catalog", ...record}); }
  }
  acknowledge(id) {
    if (id !== this.inFlight || this.closed) return;
    this.inFlight = undefined; this.flush();
  }
  close() {
    const final = this.take();
    this.closed = true; this.inFlight = undefined;
    return final;
  }
}
