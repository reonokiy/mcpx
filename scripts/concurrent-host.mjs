// A low-level host test: every fetch must start before the server releases any response.
export async function runConcurrent(runtime) {
  runtime.advance_time(Math.floor(performance.now()));
  const binding = runtime.bind("echo");
  const command = value => {
    const admission = typeof value === "string" ? JSON.parse(value) : value;
    if (admission.kind !== "operation") throw new Error("Expected a command operation");
    return admission.value;
  };
  const operations = Array.from({ length: 16 }, (_, index) => index % 3 === 0
    ? runtime.call_concurrent(binding, JSON.stringify({ message: `parallel-${index}` }))
    : index % 3 === 1 ? command(runtime.command_concurrent("echo", JSON.stringify(["--message", `parallel-${index}`])))
      : command(runtime.command_line_concurrent(`echo --message 'parallel-${index}'`)));
  const actions = JSON.parse(runtime.actions());
  if (actions.length !== 16) throw new Error("Expected sixteen independent host actions");
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 8000);
  const tasks = actions.map(async action => {
    const response = await fetch(action.url, { method: "POST", headers: action.headers, body: action.body, signal: controller.signal, redirect: "error" });
    runtime.advance_time(Math.floor(performance.now()));
    if (runtime.begin_response(action.id, response.status, JSON.stringify(Object.fromEntries(response.headers)))) { await response.body?.cancel(); return; }
    const reader = response.body.getReader();
    try {
      while (true) {
        const chunk = await reader.read();
        runtime.advance_time(Math.floor(performance.now()));
        if (chunk.done) { runtime.finish_response_for(action.id); break; }
        if (runtime.push_response_for(action.id, chunk.value)) break;
      }
    } finally { await reader.cancel(); }
  });
  try {
    await Promise.all(tasks);
    const results = operations.map(operation => JSON.parse(runtime.completion(operation)).value.raw.structuredContent.message);
    const events = JSON.parse(runtime.events()).events;
    if (events.filter(event => event.kind === "completed").length !== 16) throw new Error("Lost terminal records");
    if (new Set(events.filter(event => event.kind === "completed").map(event => event.operation_id)).size !== 16) throw new Error("Repeated operation IDs");
    return results;
  } catch (error) {
    controller.abort();
    await Promise.allSettled(tasks);
    runtime.fail("Concurrent host I/O failed");
    throw error;
  } finally {
    clearTimeout(timeout);
    controller.abort();
    runtime.release_binding(binding);
  }
}
