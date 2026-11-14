// Only scalar, validated metadata crosses host boundaries. No owner/job references.
const certainties = new Set(["not_sent", "unknown", "result_received", "needs_input"]);
const advice = new Set(["none", "correct_input", "refresh_binding", "refresh_authentication", "explicit_policy", "inspect_execution", "inspect_result", "resume_input"]);
const exactId = value => Number.isSafeInteger(value) && value > 0;
const contextId = value => typeof value === "bigint" && value > 0n && value <= 9_007_199_254_740_991n ? Number(value) : exactId(value) ? value : undefined;
export function copyErrorMetadata(source, target) {
  for (const key of ["operation_id", "queue_operation_id"]) if (exactId(source?.[key])) target[key] = source[key];
  if (certainties.has(source?.execution_certainty)) target.execution_certainty = source.execution_certainty;
  if (advice.has(source?.retry_advice)) target.retry_advice = source.retry_advice;
  return target;
}
function retryAdvice(code, certainty) {
  if (certainty === "unknown") return "inspect_execution";
  if (certainty === "result_received") return "inspect_result";
  if (certainty === "needs_input") return "resume_input";
  if (["BindingStale", "BindingExpired"].includes(code)) return "refresh_binding";
  if (["AuthenticationRequired", "AuthorizationFailed", "CredentialMissing", "Unauthorized", "AuthenticationFailed", "AuthFailed", "CredentialUnavailable"].includes(code)) return "refresh_authentication";
  if (["ValidationFailed", "InvalidJson", "InvalidInput", "InvalidArguments", "NumberOutOfRange", "SerializationFailed", "UnknownArgument", "MissingArgument", "InvalidCommand", "TokenizeError", "CommandParseError", "CommandError"].includes(code)) return "correct_input";
  if (["Timeout", "TransportError", "HostError", "QueueFull", "Busy"].includes(code)) return "explicit_policy";
  return "none";
}
export function contextError(value, { operationId, queueOperationId, certainty } = {}) {
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      if (typeof parsed?.code === "string" && typeof parsed.message === "string") value = parsed;
    } catch { /* Ordinary thrown strings remain generic host errors. */ }
  }
  // Cancellation causes can be shared by several jobs: never mutate the cause.
  const Constructor = value instanceof RangeError ? RangeError : value instanceof TypeError ? TypeError : Error;
  const error = new Constructor(typeof value?.message === "string" ? value.message : String(value));
  error.code = typeof value?.code === "string" ? value.code : value instanceof RangeError ? "NumberOutOfRange" : value instanceof TypeError ? "InvalidInput" : "HostError";
  if (typeof value?.raw === "string") error.raw = value.raw;
  if (Array.isArray(value?.issues)) error.issues = value.issues;
  copyErrorMetadata(value, error);
  operationId = contextId(operationId); queueOperationId = contextId(queueOperationId);
  if (operationId !== undefined) error.operation_id = operationId;
  if (queueOperationId !== undefined) error.queue_operation_id = queueOperationId;
  // Accepted terminal evidence outranks an earlier transport snapshot.
  if (["result_received", "needs_input"].includes(certainty) || !error.execution_certainty) {
    error.execution_certainty = certainties.has(certainty) ? certainty : "unknown";
    error.retry_advice = retryAdvice(error.code, error.execution_certainty);
  } else if (!error.retry_advice) error.retry_advice = retryAdvice(error.code, error.execution_certainty);
  return error;
}
