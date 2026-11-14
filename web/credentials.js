import { failure } from "./worker-protocol.js";

export function abortable(promise, signal) {
  if (signal.aborted) return Promise.reject(failure("Cancelled", "Operation interrupted"));
  let abort;
  const interrupted = new Promise((_, reject) => {
    abort = () => reject(failure("Cancelled", "Operation interrupted"));
    signal.addEventListener("abort", abort, { once: true });
  });
  return Promise.race([promise, interrupted]).finally(() => signal.removeEventListener("abort", abort));
}
export function credential(value) {
  if (!value || typeof value.authContextId !== "string" || !value.authContextId.length || new TextEncoder().encode(value.authContextId).byteLength > 256
    || (value.token !== undefined && (typeof value.token !== "string" || value.token.length > 65536 || !/^[\x21-\x7e]+$/.test(value.token)))) {
    throw failure("CredentialUnavailable", "Provider must return a bounded non-secret authContextId and header-safe bearer");
  }
  const result = { token: value.token, authContextId: value.authContextId };
  if (new TextEncoder().encode(JSON.stringify(result)).byteLength > 65536) throw failure("CredentialUnavailable", "Credential message exceeds 64 KiB");
  return Object.freeze(result);
}
export class CredentialResolver {
  constructor({ token, credentialProvider, credentialReference } = {}) {
    if (credentialProvider !== undefined && typeof credentialProvider !== "function") throw new TypeError("credentialProvider must be a function");
    if (credentialProvider && token !== undefined) throw new TypeError("Choose token or credentialProvider");
    if (credentialReference !== undefined && (typeof credentialReference !== "string" || !credentialReference.length || new TextEncoder().encode(credentialReference).byteLength > 1024 || !credentialProvider)) throw new TypeError("credentialReference requires a provider and 1..1024 bytes");
    this.token = token; this.provider = credentialProvider; this.reference = credentialReference;
    this.lastToken = Symbol(); this.epoch = 0;
  }
  async resolve(endpoint, signal) {
    if (this.closed || signal.aborted) throw failure("CredentialUnavailable", "Credential resolver is closed or interrupted");
    if (!this.provider) {
      const changed = this.token !== this.lastToken;
      if (changed && this.epoch === Number.MAX_SAFE_INTEGER) throw failure("LimitExceeded", "Credential identity exhausted");
      const epoch = this.epoch + (changed ? 1 : 0);
      const value = credential({ token: this.token, authContextId: `static-${epoch}` });
      // Invalid credentials and exhausted rotations must not commit a new identity.
      if (changed) { this.epoch = epoch; this.lastToken = this.token; }
      return value;
    }
    // A provider that ignores abort retains its one slot until it settles.
    // Repeated cancellations cannot create an unbounded number of callbacks.
    if (this.running) throw failure("CredentialUnavailable", "Previous credential resolution has not settled");
    const pending = Promise.resolve().then(() => {
      if (this.closed || signal.aborted) throw failure("CredentialUnavailable", "Credential resolution interrupted before provider dispatch");
      return this.provider({ endpoint, reference: this.reference, signal });
    });
    this.running = pending;
    const clear = () => { if (this.running === pending) this.running = undefined; };
    pending.then(clear, clear);
    try { return credential(await abortable(pending, signal)); }
    catch { throw failure(signal.aborted ? "Cancelled" : "CredentialUnavailable", "Credential resolution failed or was interrupted"); }
  }
  close() {
    this.closed = true;
    this.token = this.lastToken = this.provider = this.reference = this.running = undefined;
  }
}

export class AdmissionLock {
  constructor() { this.waiters = []; this.locked = false; }
  acquire(signal) {
    if (signal.aborted) return Promise.reject(failure("Cancelled", "Admission interrupted"));
    if (!this.locked) { this.locked = true; return Promise.resolve(() => this.release()); }
    return new Promise((resolve, reject) => {
      const waiter = { resolve, signal };
      waiter.abort = () => { this.waiters = this.waiters.filter(value => value !== waiter); reject(failure("Cancelled", "Admission interrupted")); };
      signal.addEventListener("abort", waiter.abort, { once: true });
      this.waiters.push(waiter);
    });
  }
  release() {
    const waiter = this.waiters.shift();
    if (!waiter) { this.locked = false; return; }
    waiter.signal.removeEventListener("abort", waiter.abort);
    waiter.resolve(() => this.release());
  }
}
