import { copyErrorMetadata } from "./errors.js";
export const VERSION = 1;
export const monotonicNow = () => performance.timeOrigin + performance.now();
export const failure = (code, message) => Object.assign(new Error(message), { code });
export function integer(value, min, max, name) {
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new RangeError(`${name} must be an integer in ${min}..${max}`);
  return value;
}
export function encodeError(error) {
  if (typeof error === "string") {
    try { error = JSON.parse(error); } catch { error = failure("HostError", "Worker operation failed"); }
  }
  const result = {
    name: ["RangeError", "TypeError"].includes(error?.name) ? error.name : "Error",
    code: typeof error?.code === "string" ? error.code : "HostError",
    message: typeof error?.message === "string" ? error.message : "Worker operation failed",
  };
  if (typeof error?.raw === "string") result.raw = error.raw;
  if (Array.isArray(error?.issues)) result.issues = error.issues.map(issue => ({ instance_path: issue.instance_path, schema_path: issue.schema_path }));
  return copyErrorMetadata(error, result);
}
export function decodeError(value) {
  if (!value || typeof value.message !== "string" || typeof value.code !== "string") return failure("WorkerProtocolError", "Invalid worker error response");
  const Constructor = value.name === "RangeError" ? RangeError : value.name === "TypeError" ? TypeError : Error;
  const error = Object.assign(new Constructor(value.message), { code: value.code });
  if (typeof value.raw === "string") error.raw = value.raw;
  if (Array.isArray(value.issues)) error.issues = value.issues;
  return copyErrorMetadata(value, error);
}
