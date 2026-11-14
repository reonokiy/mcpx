import { WorkerRemote } from "../../web/worker.js";

const $ = id => document.getElementById(id);
let remote, operation, completionTask;
const lines = [];
function write(value) {
  const text = typeof value === "string" ? value : JSON.stringify(value, null, 2);
  lines.push(text.length > 65536 ? text.slice(0, 65536) + "\n[display truncated]" : text);
  while (lines.length > 100 || lines.reduce((sum, line) => sum + line.length, 0) > 1024 * 1024) lines.shift();
  $("output").textContent = lines.join("\n");
}
function controls() {
  const ready = remote?.state === "ready";
  $("connect").disabled = Boolean(remote);
  $("close").disabled = !remote;
  $("command").disabled = !ready;
  $("run").disabled = !ready || Boolean(operation || completionTask);
  $("complete").disabled = !ready || Boolean(operation || completionTask);
  $("cancel").disabled = !(operation || completionTask);
}
$("connect").addEventListener("click", async () => {
  if (remote) return;
  const current = new WorkerRemote({ onEvent: event => {
    if (event.kind === "progress") write({ progress: event.payload?.params?.progress });
    if (event.kind === "lagged") write({ droppedNotifications: event.payload.dropped });
  } });
  remote = current;
  $("status").textContent = "Connecting";
  const connecting = current.connect($("endpoint").value);
  operation = connecting; controls();
  try {
    await connecting;
    if (remote === current) $("status").textContent = "Connected";
  } catch (error) {
    write({ code: error.code, message: error.message });
    await current.close().catch(() => {});
    if (remote === current) { remote = undefined; $("status").textContent = "Disconnected"; }
  } finally { if (operation === connecting) operation = undefined; controls(); }
});
$("command-form").addEventListener("submit", async event => {
  event.preventDefault();
  if (!remote || operation || completionTask) return;
  write(`> ${$("command").value}`);
  const pending = remote.executeLine($("command").value);
  operation = pending; controls();
  try {
    const result = await pending;
    write(result.kind === "help" ? result.value : result);
  } catch (error) { write(error.raw ?? { code: error.code, message: error.message, issues: error.issues }); }
  finally { if (operation === pending) operation = undefined; controls(); }
});
$("cancel").addEventListener("click", () => (operation ?? completionTask)?.cancel());
async function complete() {
  if (!remote || operation || completionTask) return;
  const owner = remote, input = $("command"), line = input.value, cursor = input.selectionStart;
  const bytes = new TextEncoder().encode(line);
  const pending = owner.completeLine(line, new TextEncoder().encode(line.slice(0, cursor)).byteLength);
  completionTask = pending; controls();
  $("completions").replaceChildren();
  try {
    const result = await pending;
    if (remote !== owner || input.value !== line || input.selectionStart !== cursor) return;
    const apply = candidate => {
      if (remote !== owner || input.value !== line) return;
      const decoder = new TextDecoder(), before = decoder.decode(bytes.slice(0, result.start));
      input.value = before + candidate.replacement + decoder.decode(bytes.slice(result.end));
      const position = before.length + candidate.replacement.length;
      input.focus(); input.setSelectionRange(position, position); $("completions").replaceChildren();
    };
    if (result.candidates.length === 1) apply(result.candidates[0]);
    else {
      const hint = document.createElement("p");
      hint.textContent = result.candidates.length ? `Choose a suggestion${result.truncated ? " (more results available; type a longer prefix)" : ""}:` : "No suggestions for this input.";
      $("completions").append(hint);
      for (const candidate of result.candidates) {
        const button = document.createElement("button"); button.type = "button";
        button.textContent = candidate.value.length > 512 ? candidate.value.slice(0, 512) + "…" : candidate.value || "(empty value)";
        button.title = candidate.description; button.addEventListener("click", () => apply(candidate));
        $("completions").append(button);
      }
    }
  } catch (error) { if (error.code !== "Cancelled") write({ code: error.code, message: error.message }); }
  finally { if (completionTask === pending) completionTask = undefined; controls(); }
}
$("complete").addEventListener("click", complete);
$("command").addEventListener("keydown", event => {
  if (event.key === "Tab" && !event.shiftKey && remote?.state === "ready" && !operation) { event.preventDefault(); void complete(); }
});
$("command").addEventListener("input", () => { completionTask?.cancel(); $("completions").replaceChildren(); });
$("close").addEventListener("click", async () => {
  const current = remote;
  if (!current) return;
  $("status").textContent = "Closing";
  const closing = current.close(); controls();
  $("completions").replaceChildren();
  try { await closing; } catch (error) { write({ code: error.code, message: error.message }); }
  if (remote === current) { remote = undefined; $("status").textContent = "Disconnected"; }
  controls();
});
