import { engine } from "../../dist/component/mcpx_component.js";
import { driveComponent, closeComponent } from "./host.mjs";

const [endpoint, tool, args = "{}"] = process.argv.slice(2);
if (!endpoint || !tool) {
  console.error("Usage: node examples/wasi-component/main.mjs ENDPOINT TOOL '[JSON arguments]'");
  process.exitCode = 2;
} else {
  let remote;
  try {
    remote = engine.createAuto(endpoint);
    remote.discover();
    await driveComponent(remote);
    const binding = remote.bind(tool);
    const operation = remote.callConcurrent(binding, args);
    await driveComponent(remote);
    const outcome = remote.completion(operation);
    // The result body stays JSON text, preserving uint64 and extension fields.
    if (outcome.tag === "completed") {
      console.log(outcome.val.rawJson);
      process.exitCode = outcome.val.isError || outcome.val.outputValidation === "invalid" ? 1 : 0;
    } else {
      console.log(JSON.stringify({ kind: "needs_input", id: String(outcome.val.id), expiresAtMs: String(outcome.val.expiresAtMs) }));
      process.exitCode = 7;
    }
    remote.releaseBinding(binding);
  } catch (error) {
    console.error(`${error.code ?? "HostError"}: ${error.message ?? "Component operation failed"}`);
    process.exitCode = 1;
  } finally {
    if (remote) {
      try { await closeComponent(remote); }
      catch (error) { console.error(`${error.code}: ${error.message}`); process.exitCode = 1; remote[Symbol.dispose](); }
    }
  }
}
