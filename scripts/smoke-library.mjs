import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createMockServer } from "./mock-server.mjs";

const { server, requests } = createMockServer();
await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
try {
  const endpoint = `http://127.0.0.1:${server.address().port}/mcp`;
  const { stdout, stderr } = await promisify(execFile)("target/debug/examples/library", [endpoint, "typed library example"]);
  assert.equal(JSON.parse(stdout).structuredContent.message, "typed library example");
  assert.match(stderr, /completed/);
  assert.match(stderr, /decoded 21 bytes/);
  assert.equal(requests.filter(request => request.method === "tools/call").length, 1);
  console.log("PASS Rust library example: runtime binding, checked call, async event stream and close");
  const { stdout: queued, stderr: lifecycle } = await promisify(execFile)("target/debug/examples/queued", [endpoint]);
  const operationBatches=lifecycle.trim().split("\n").map(JSON.parse);
  assert.ok(operationBatches.every(record=>record.type==="operations"&&record.batch.dropped===0));
  const operationEvents=operationBatches.flatMap(record=>record.batch.events);
  assert.equal(operationEvents.filter(event=>event.kind==="queued").length,3);
  assert.equal(operationEvents.filter(event=>event.kind==="completed").length,3);
  assert.deepEqual(JSON.parse(queued).map(result => result.structuredContent.message), ["queued-0", "queued-1"]);
  assert.equal(requests.filter(request => request.method === "tools/call").length, 3);
  console.log("PASS queued Rust example: Send handles, typed arguments, concurrent callers and owner shutdown");
} finally {
  await new Promise(resolve => server.close(resolve));
}
