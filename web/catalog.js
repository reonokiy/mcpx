const failure = (code, message) => Object.assign(new Error(message), {code});
const done = () => ({done:true, value:undefined});

// Identity markers and bounded consumer slots; never retain a Remote or schemas.
export class CatalogSource {
  constructor() {
    this.publication = Symbol(); this.context = Symbol();
    this.watchers = new Set(); this.closed = false;
  }
  subscribe() {
    if (this.closed) throw failure("TransportClosed", "Catalog source is closed");
    if (this.watchers.size === 64) throw failure("LimitExceeded", "Catalog subscriber limit reached");
    const record = {publication:this.publication, context:this.context, active:true};
    this.watchers.add(record);
    return new CatalogSubscription(this, record);
  }
  read(record) {
    if (!record.active) return done();
    const published = record.publication !== this.publication;
    const invalidated = record.context !== this.context;
    if (published || invalidated) {
      record.publication = this.publication; record.context = this.context;
      return {done:false, value:Object.freeze({published, invalidated})};
    }
    return this.closed ? done() : undefined;
  }
  settle(record) {
    if (!record.waiter) return;
    const result = this.read(record);
    if (result !== undefined) {
      const waiter = record.waiter;
      waiter.cleanup(); waiter.resolve(result);
    }
  }
  change({published, invalidated}) {
    if (this.closed) return;
    if (published) this.publication = Symbol();
    if (invalidated) this.context = Symbol();
    for (const record of this.watchers) this.settle(record);
  }
  close() {
    this.closed = true;
    for (const record of this.watchers) this.settle(record);
    this.watchers.clear();
  }
}

class CatalogSubscription {
  constructor(source, record) { this.source = source; this.record = record; }
  [Symbol.asyncIterator]() { return this; }
  tryNext() {
    if (this.record.waiter) throw failure("Busy", "A catalog read is already pending");
    return this.source.read(this.record);
  }
  next({signal} = {}) {
    try {
      const record = this.record;
      if (record.waiter) throw failure("Busy", "A catalog read is already pending");
      if (signal && (typeof signal.addEventListener !== "function" || typeof signal.removeEventListener !== "function")) throw new TypeError("signal must be an AbortSignal");
      if (signal?.aborted) throw failure("Cancelled", "Catalog read cancelled");
      const ready = this.source.read(record);
      if (ready !== undefined) return Promise.resolve(ready);
      return new Promise((resolve, reject) => {
        const cleanup = () => { record.waiter = undefined; signal?.removeEventListener("abort", abort); };
        const abort = () => { cleanup(); reject(failure("Cancelled", "Catalog read cancelled")); };
        record.waiter = {resolve, cleanup};
        signal?.addEventListener("abort", abort, {once:true});
      });
    } catch (error) { return Promise.reject(error); }
  }
  close() {
    this.record.active = false;
    this.source.watchers.delete(this.record);
    this.source.settle(this.record);
  }
  return() { this.close(); return Promise.resolve(done()); }
}
