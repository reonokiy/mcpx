// Exercises the actual Wasm/jco ABI, including values that u32 conversion used
// to wrap onto a live binding/request/operation. No network is needed here.
export function runHostIds(runtime) {
  const check = (condition, message) => { if (!condition) throw new Error(message); };
  const fails = (run, code) => {
    try { run(); } catch (error) {
      if (typeof error === "string") error = JSON.parse(error);
      check(error.code === code, `Expected ${code}, got ${error.code}`);
      return;
    }
    throw new Error(`Expected ${code}`);
  };
  check(JSON.parse(runtime.tools()).some(tool => tool.name === "echo"), "Host adapter lost catalog access");
  for (const capacity of [-1, 0.5, NaN, Infinity, 4097, 2 ** 32, "2", false, null, undefined]) {
    fails(() => runtime.set_validator_cache_capacity(capacity), "ConfigurationError");
  }
  runtime.set_validator_cache_capacity(2);
  runtime.clear_validator_cache();
  check(JSON.parse(runtime.validator_cache_stats()).entries === 0, "Cache clear failed");
  for (const cursor of [-1, 0.5, NaN, Infinity, 2 ** 32, "0", false, null, undefined]) {
    fails(() => runtime.complete_line("", cursor), "InvalidCursor");
  }
  const json = JSON.stringify({ message: "host-id-boundaries" });
  const binding = runtime.bind("echo");
  runtime.clear_validator_cache();
  check(JSON.parse(runtime.validator_cache_stats()).entries === 0, "Live binding prevented cache eviction");
  const methods = [
    id => runtime.validate(id, json), id => runtime.call(id, json),
    id => runtime.call_concurrent(id, json), id => runtime.release_binding(id),
    id => runtime.resume(id, "{}"), id => runtime.resume_concurrent(id, "{}"),
    id => runtime.release_continuation(id), id => runtime.completion(id),
    id => runtime.cancel_operation(id), id => runtime.fail_request(id, "invalid"),
    id => runtime.timeout_request(id), id => runtime.begin_response(id, 200, "{}"),
    id => runtime.exhaust_response_resumption(id),
    id => runtime.push_response_for(id, new Uint8Array()), id => runtime.finish_response_for(id),
  ];
  for (const id of [0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, "1", true, null, undefined]) {
    for (const method of methods) fails(() => method(id), "InvalidHandle");
  }
  const forgedBinding = Number(binding) + 2 ** 32;
  fails(() => runtime.validate(forgedBinding, json), "InvalidHandle");
  fails(() => runtime.call_concurrent(forgedBinding, json), "InvalidHandle");
  runtime.release_binding(forgedBinding);
  check(JSON.parse(runtime.validate(binding, json)).message === "host-id-boundaries", "Forged binding released the live handle");

  const operation = runtime.call_concurrent(binding, json);
  const [action] = JSON.parse(runtime.actions());
  fails(()=>runtime.reset_transport(),"Busy");
  check(action && !Number.isNaN(Number(operation)), "Call admission failed");
  check(new Set([Number(binding), Number(operation), action.id, action.rpc_id]).size === 4 && action.operation_id == operation, "Core ID domains overlap or action metadata lost its operation");
  for (const foreign of [binding, operation, action.rpc_id]) fails(() => runtime.begin_response(Number(foreign), 200, "{}"), "UnexpectedResponse");
  for (const foreign of [binding, action.id, action.rpc_id]) fails(() => runtime.cancel_operation(Number(foreign)), "InvalidOperation");
  runtime.release_binding(Number(operation));
  check(JSON.parse(runtime.validate(binding, json)).message === "host-id-boundaries", "Operation ID aliased a binding");
  const forgedOperation = Number(operation) + 2 ** 32;
  const forgedAction = action.id + 2 ** 32;
  fails(() => runtime.cancel_operation(forgedOperation), "InvalidOperation");
  check(runtime.completion(forgedOperation) == null, "Forged operation consumed a completion");
  fails(() => runtime.fail_request(forgedAction, "forged"), "UnexpectedResponse");
  fails(() => runtime.timeout_request(forgedAction), "UnexpectedResponse");
  fails(() => runtime.exhaust_response_resumption(forgedAction), "InvalidHandle");
  fails(() => runtime.begin_response(forgedAction, 200, "{}"), "UnexpectedResponse");
  for (const status of [0, 600, 65536 + 200, -1, 200.5, NaN, Infinity, "200", true, null]) fails(() => runtime.begin_response(action.id, status, "{}"), "InvalidResponse");
  for (const headers of ["null", "[]", "broken", '{"x-number":1}']) fails(() => runtime.begin_response(action.id, 200, headers), "InvalidJson");
  fails(() => runtime.begin_response(action.id, 200, '{"Content-Type":"application/json","content-type":"text/event-stream"}'), "InvalidResponse");
  runtime.begin_response(action.id, 200, '{"content-type":"application/json"}');
  fails(() => runtime.push_response_for(forgedAction, new Uint8Array()), "InvalidHandle");
  fails(() => runtime.finish_response_for(forgedAction), "InvalidHandle");
  const rpc = JSON.parse(action.body);
  const complete = result => runtime.push_response_for(action.id, new TextEncoder().encode(JSON.stringify({ jsonrpc: "2.0", id: rpc.id, result })));
  if (!complete({ content: [], structuredContent: { message: "host-id-boundaries" } })) runtime.finish_response_for(action.id);
  const receipt=JSON.parse(runtime.http_session_status());
  check(receipt.expired_at_ms===null&&receipt.last_tool_result===String(operation),"Session status lost the exact received operation ID");
  check(runtime.http_session_status()===JSON.stringify(receipt),"Session status consumed evidence");
  const outcome = JSON.parse(runtime.completion(operation));
  check(runtime.http_session_status()===JSON.stringify(receipt),"Completion consumption changed receipt evidence");
  check(outcome.value.raw.structuredContent.message === "host-id-boundaries", "Original operation lost its result");

  if (action.headers["mcp-protocol-version"] === "2026-07-28") {
    const pendingOperation = runtime.call_concurrent(binding, json);
    const [pendingAction] = JSON.parse(runtime.actions());
    runtime.begin_response(pendingAction.id, 200, '{"content-type":"application/json"}');
    const done = runtime.push_response_for(pendingAction.id, new TextEncoder().encode(JSON.stringify({
      jsonrpc: "2.0", id: JSON.parse(pendingAction.body).id,
      result: { resultType: "input_required", requestState: "opaque host state" },
    })));
    if (!done) runtime.finish_response_for(pendingAction.id);
    const handle = JSON.parse(runtime.completion(pendingOperation)).value;
    const forgedContinuation = handle.id + 2 ** 32;
    runtime.release_continuation(forgedContinuation);
    fails(() => runtime.resume_concurrent(forgedContinuation, "{}"), "InvalidContinuation");
    const resumed = runtime.resume_concurrent(handle.id, "{}");
    runtime.actions();
    runtime.cancel_operation(resumed);
    fails(() => runtime.completion(resumed), "Cancelled");
    check(runtime.completion(resumed) == null, "Cancelled completion was retained");
  }
  runtime.actions(); runtime.events();
  runtime.release_binding(binding);
  check(!runtime.busy(), "Boundary checks left an active operation");

  // Feed a raw Schema so the uint64 enum never passes through JS Number.
  runtime.refresh_catalog();
  const [listing] = JSON.parse(runtime.actions());
  runtime.begin_response(listing.id, 200, '{"content-type":"application/json"}');
  const schemaReply = `{"jsonrpc":"2.0","id":${listing.rpc_id},"result":{"tools":[{"name":"number_enum","inputSchema":{"type":"object","properties":{"amount":{"type":"number","enum":[18446744073709551615,0.25]}}}}]}}`;
  if (!runtime.push_response_for(listing.id, new TextEncoder().encode(schemaReply))) runtime.finish_response_for(listing.id);
  const spec = JSON.parse(runtime.command_spec("number_enum"));
  check(spec.parameters[0].values.includes("18446744073709551615"), "CommandSpec rounded a uint64 enum");
  const line = "number_enum --amount 18";
  const candidates = JSON.parse(runtime.complete_line(line, line.length));
  check(candidates.candidates[0]?.value === "18446744073709551615", "Completion rounded a uint64 candidate");
  let admitted = runtime.command_concurrent("number_enum", '["--amount","18446744073709551615"]');
  if (typeof admitted === "string") admitted = JSON.parse(admitted);
  const [numeric] = JSON.parse(runtime.actions());
  check(numeric.body.includes('"amount":18446744073709551615'), "number flag rounded an exact integer before transport");
  runtime.begin_response(numeric.id, 200, '{"content-type":"application/json"}');
  if (!runtime.push_response_for(numeric.id, new TextEncoder().encode(JSON.stringify({ jsonrpc: "2.0", id: numeric.rpc_id, result: { content: [] } })))) runtime.finish_response_for(numeric.id);
  const oldBinding=runtime.bind("number_enum");
  const finalReceipt=runtime.http_session_status();
  check(JSON.parse(finalReceipt).last_tool_result===String(admitted.value),"Named tool receipt was not recorded");
  runtime.reset_transport();
  check(runtime.http_session_status()===finalReceipt,"Transport reset changed accepted receipt evidence");
  check(JSON.parse(runtime.completion(admitted.value)).kind === "completed", "Transport reset lost a delivered completion");
  fails(()=>runtime.validate(oldBinding,'{"amount":0.25}'),"InvalidHandle");
  runtime.events();
  const registryStats = runtime.schema_registry_stats();
  for (const invalid of ["broken", "{}", '[{"uri":"https://schemas.example/a","schema_json":3}]', '[{"uri":"https://schemas.example/a","schema_json":"{}","unknown":true}]']) {
    fails(() => runtime.set_schema_registry(invalid), "ConfigurationError");
  }
  check(runtime.schema_registry_stats() === registryStats, "Invalid registry records changed the active resource snapshot");
  fails(() => runtime.set_schema_registry(JSON.stringify(Array.from({length:129}, (_,i) => ({uri:`https://schemas.example/${i}`,schema_json:"{}"})))), "LimitExceeded");
}
