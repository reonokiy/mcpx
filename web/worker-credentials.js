import { CredentialResolver, credential } from "./credentials.js";
import { VERSION, failure } from "./worker-protocol.js";

// Providers stay in the embedding realm; only one bounded resolution crosses
// the port. Tokens are never used as catalog identity or included in errors.
export class CredentialHost {
  constructor(post, options) { this.post = post; this.resolver = new CredentialResolver(options); }
  receive(message) {
    if (message.op === "cancel") {
      if (this.active?.id === message.id) this.active.controller.abort();
      return;
    }
    if (message.op !== "resolve" || !Number.isSafeInteger(message.id) || message.id <= (this.lastId ?? 0) || this.active || this.closed) throw failure("WorkerProtocolError", "Invalid credential request");
    this.lastId = message.id;
    if (message.endpoint !== this.endpoint) throw failure("WorkerProtocolError", "Credential endpoint changed");
    const record = { id: message.id, controller: new AbortController() };
    this.active = record;
    void this.resolver.resolve(this.endpoint, record.controller.signal).then(
      value => this.reply(record, { credential: value }),
      () => this.reply(record, { error: true }),
    );
  }
  reply(record, result) {
    if (this.active !== record) return;
    this.active = undefined;
    if (!this.closed) this.post({ v: VERSION, kind: "credential", op: "result", id: record.id, ...result });
  }
  close() {
    this.closed = true;
    this.active?.controller.abort(); this.active = undefined;
    this.resolver.close(); this.endpoint = this.post = undefined;
  }
}

export class CredentialPort {
  constructor(post) { this.post = post; this.nextId = 1; }
  resolve({ endpoint, signal }) {
    if (this.active || this.nextId > Number.MAX_SAFE_INTEGER) return Promise.reject(failure("CredentialUnavailable", "Credential channel is busy"));
    if (signal.aborted) return Promise.reject(failure("Cancelled", "Credential resolution interrupted"));
    const record = { id: this.nextId++, signal };
    this.active = record;
    record.promise = new Promise((resolve, reject) => { record.resolve = resolve; record.reject = reject; });
    record.abort = () => this.post({ v: VERSION, kind: "credential", op: "cancel", id: record.id });
    signal.addEventListener("abort", record.abort, { once: true });
    this.post({ v: VERSION, kind: "credential", op: "resolve", id: record.id, endpoint });
    return record.promise;
  }
  receive(message) {
    const record = this.active;
    if (!record || message.id !== record.id) return;
    if (message.op !== "result") throw failure("WorkerProtocolError", "Invalid credential reply");
    this.active = undefined;
    record.signal.removeEventListener("abort", record.abort);
    try {
      if (message.error || record.signal.aborted) throw failure("CredentialUnavailable", "Credential resolution failed or was interrupted");
      record.resolve(credential(message.credential));
    } catch (error) { record.reject(error); }
  }
}
