import { materializeError } from "./operations.js";
import { contextError, copyErrorMetadata } from "./errors.js";
const failure = message => contextError(Object.assign(new Error(message), { code:"LimitExceeded" }), {certainty:"not_sent"});
function short(value, limit) {
  let result = "", bytes = 0;
  for (const char of value) {
    const code = char.codePointAt(0), length = code < 128 ? 1 : code < 2048 ? 2 : code < 65536 ? 3 : 4;
    if (bytes + length > limit) break;
    result += char; bytes += length;
  }
  return result;
}
function diagnostic(error) {
  const originalCode = typeof error.code === "string" ? error.code : "HostError";
  const code = short(originalCode,128) === originalCode ? originalCode : "EventTooLarge";
  const message = short(typeof error.message === "string" ? error.message : "Operation failed",512);
  const value = copyErrorMetadata(error,{code,message});
  if (Array.isArray(error.issues)) value.issues = error.issues.slice(0,4)
    .filter(issue=>typeof issue?.instance_path === "string"&&typeof issue.schema_path === "string"&&short(issue.instance_path,64)===issue.instance_path&&short(issue.schema_path,64)===issue.schema_path)
    .map(issue=>({instance_path:issue.instance_path,schema_path:issue.schema_path}));
  if (code !== error.code || message !== error.message || (value.issues?.length ?? 0) !== (error.issues?.length ?? 0)) value.truncated = true;
  return value;
}
// Separate sequence/ID domain from protocol onEvent. Synchronous observers do
// not buffer events; host queues own the bounded number of active reservations.
export class OperationObserver {
  constructor(callback, onError) {
    if (callback !== undefined && typeof callback !== "function") throw new TypeError("onOperationEvent must be a function");
    this.callback = callback; this.onError = onError; this.sequence = 0; this.active = new Set();
  }
  reserve(id) {
    if (!this.callback) return;
    if (this.sequence + this.active.size + 2 > Number.MAX_SAFE_INTEGER) throw failure("Queue event sequence exhausted");
    this.active.add(id);
  }
  queued(id) { if (this.active.has(id)) this.emit(id,"queued",{execution_certainty:"not_sent"}); }
  start(id) {
    if (!this.active.has(id)) return;
    if (this.sequence + this.active.size + 1 > Number.MAX_SAFE_INTEGER) throw failure("Queue event sequence exhausted");
    this.emit(id,"started",{execution_certainty:"not_sent"});
  }
  finish(id, {coreId,certainty="not_sent",error,summary} = {}) {
    if (!this.active.delete(id)) return;
    const kind = error ? error.code === "Cancelled" ? "cancelled" : "failed" : certainty === "needs_input" ? "needs_input" : "completed";
    const payload = error ? diagnostic(error) : {execution_certainty:certainty};
    if (!error && typeof summary?.is_error === "boolean") payload.is_error = summary.is_error;
    if (!error && ["valid","invalid","indeterminate","not_declared","not_applicable"].includes(summary?.output_validation)) payload.output_validation = summary.output_validation;
    this.emit(id,kind,payload,coreId);
  }
  emit(id,kind,payload,coreId) {
    const event = {queue_operation_id:id,sequence:++this.sequence,kind,payload};
    if (typeof coreId === "bigint" && coreId > 0n && coreId <= 9_007_199_254_740_991n) coreId = Number(coreId);
    if (Number.isSafeInteger(coreId) && coreId > 0) event.core_operation_id = coreId;
    try { this.callback(event); } catch (error) { try { this.onError(materializeError(error)); } catch { /* Observers never own queue execution. */ } }
  }
}
