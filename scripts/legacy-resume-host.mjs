// Explicit low-level host. A high-level Remote must not own this runtime too.
const parse = value => typeof value === "string" ? JSON.parse(value) : value;
const check = (value, message) => { if (!value) throw new Error(message); };
const code = error => typeof error === "string" ? JSON.parse(error).code : error.code;
function fails(fn, expected) {
  try { fn(); } catch(error) { check(code(error) === expected, `Expected ${expected}, received ${code(error)}`); return; }
  throw new Error(`Expected ${expected}`);
}
export async function runLegacyResume(runtime, endpoint, legacy) {
  // Keep the credential that opened the original stream for every resumed GET
  // and capability response. Resuming is independent of provider resolution.
  const frozen = "Bearer resume-frozen";
  function headers(fields, get=false) {
    const headers = new Headers();
    for (const [name, value] of Object.entries(fields)) {
      if (name === "last-event-id") {
        if (get) headers.set(name, String.fromCharCode(...new TextEncoder().encode(value)));
      } else headers.set(name, value);
    }
    headers.set("authorization", frozen);
    return headers;
  }
  async function drive() {
    while (runtime.busy()) {
      const actions = parse(runtime.actions()); check(actions.length, "Missing metadata/call action");
      for (const action of actions) {
        const response = await fetch(action.url, {method:action.method ?? "POST", headers:headers(action.headers), body:action.body, redirect:"error"});
        if (runtime.begin_response(action.id, response.status, JSON.stringify(Object.fromEntries(response.headers)))) { await response.body?.cancel(); continue; }
        const reader = response.body.getReader();
        try {
          for (;;) {
            const part = await reader.read();
            if (part.done) { runtime.finish_response_for(action.id); break; }
            if (runtime.push_response_for(action.id, part.value)) break;
          }
        } finally { await reader.cancel(); reader.releaseLock(); }
      }
    }
    runtime.events();
  }
  async function read(plan, expected) {
    const response = await fetch(plan.url, {method:plan.method, headers:headers(plan.headers, true), body:plan.body == null ? undefined : JSON.stringify(plan.body), redirect:"error"});
    runtime.begin_notifications(plan.resource_id, response.status, response.headers.get("content-type"));
    const reader = response.body.getReader();
    try {
      for (;;) {
        const part = await reader.read(); if (part.done) break;
        for (let offset=0; offset<part.value.length; offset+=65536) runtime.push_notifications(plan.resource_id, part.value.subarray(offset, offset+65536));
        for (const reply of parse(runtime.notification_replies(plan.resource_id))) {
          const response = await fetch(plan.url, {method:"POST", headers:headers({...plan.headers,"content-type":"application/json",accept:"application/json, text/event-stream"}), body:JSON.stringify(reply), redirect:"error"});
          check(response.status === 202, "Capability reply failed"); await response.body?.cancel();
        }
      }
    } finally { await reader.cancel().catch(()=>{}); reader.releaseLock(); }
    fails(() => runtime.finish_notifications(plan.resource_id), expected);
  }
  runtime.accept_authorization("resume-scope"); runtime.discover(); await drive();
  runtime.advance_time(100);
  const first = parse(runtime.start_notifications());
  await read(first, legacy ? "ProtocolError" : "SubscriptionDisconnected");
  if (!legacy) {
    fails(() => runtime.notification_resume(first.resource_id), "ResumeUnavailable");
    fails(() => runtime.resume_notifications(first.resource_id), "ResumeUnavailable");
    return;
  }
  check(parse(runtime.notification_resume(first.resource_id)).retry_after_ms === 25, "Retry delay lost");
  fails(() => runtime.resume_notifications(first.resource_id), "RetryLater");
  runtime.refresh_catalog(); await drive();
  const binding = runtime.bind("echo"), call = runtime.call_concurrent(binding, '{"message":"independent"}');
  await drive(); check(parse(runtime.completion(call)).value.raw.structuredContent.message === "independent", "Independent call lost");
  runtime.release_binding(binding);
  runtime.advance_time(124); check(parse(runtime.notification_resume(first.resource_id)).retry_after_ms === 1, "Retry interval shortened");
  await new Promise(resolve => setTimeout(resolve, 30));
  runtime.advance_time(125);
  const second = parse(runtime.resume_notifications(first.resource_id));
  check(second.method === "GET" && second.body == null && second.rpc_id == null, "Resume replayed a POST");
  check(second.resource_id !== first.resource_id && second.headers["last-event-id"] === "游标/é", "Wrong checkpoint");
  fails(() => runtime.push_notifications(first.resource_id, new TextEncoder().encode("data: {}\n\n")), "InvalidHandle");
  fails(() => runtime.abort_notifications(first.resource_id, false), "InvalidHandle");
  await read(second, "SubscriptionDisconnected");
  check(parse(runtime.notification_resume(second.resource_id)).retry_after_ms === 0, "Zero retry ignored");
  const third = parse(runtime.resume_notifications(second.resource_id));
  check(third.headers["last-event-id"] === "cursor-two", "New checkpoint lost");
  await read(third, "SubscriptionDisconnected");
  fails(() => runtime.resume_notifications(third.resource_id), "ResumeUnavailable");
}
