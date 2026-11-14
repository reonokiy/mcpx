// This factory has no owner/job lexical scope. A retained settled promise keeps
// only an empty cancellation cell, rather than its queue, inputs or credentials.
export function attachCancellation(promise, operationId, action) {
  Object.defineProperties(promise, {
    operationId: { value: operationId },
    cancel: { value: () => action?.() },
  });
  return () => { action = undefined; };
}

export function releaseOperation(job) {
  job.detachCancellation();
  // Keep only harmless identity/terminal facts, including when an embedding
  // application observed a queue record before completion.
  for (const key of Object.keys(job)) if (key !== "id" && key !== "done") delete job[key];
  job.done = true;
}

export function materializeError(error) {
  // V8's lazy stack can retain CallSites/async promises and their receivers.
  // Format the default string stack before delivery, without changing error
  // identity, type, code, raw JSON or pointers. Custom getters may throw.
  try { void error?.stack; } catch { /* Diagnostics must not replace the error. */ }
  return error;
}

export function rejectOperation(error) { return Promise.reject(materializeError(error)); }
