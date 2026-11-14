import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";
import { createMockServer, echo } from "./mock-server.mjs";
import { schemaResources } from "./schema-fixture.mjs";

const exec = promisify(execFile);
const { server, requests } = createMockServer();
await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
const endpoint = `http://127.0.0.1:${server.address().port}/mcp`;
const { server: legacyServer } = createMockServer(undefined, { legacy: true });
await new Promise(resolve => legacyServer.listen(0, "127.0.0.1", resolve));
const legacyEndpoint = `http://127.0.0.1:${legacyServer.address().port}/mcp`;
const directory = await mkdtemp(join(tmpdir(), "mcpx-cli-"));
try {
  const { stdout: help } = await exec("target/debug/mcpx", ["--help"]);
  assert.match(help, /Bind and call remote MCP tools/);
  const { stdout: completionHelp } = await exec("target/debug/mcpx", ["--config", "/nonexistent/mcpx.toml", "complete", "--help"]);
  assert.match(completionHelp, /--cursor/);
  for (const version of ["2026-07-28", "2025-11-25"]) {
    const base = ["--url", endpoint, "--protocol", version, "--output", "json"];
    const { stdout: tools } = await exec("target/debug/mcpx", [...base, "tools"]);
    assert.equal(JSON.parse(tools)[0].name, "echo");
    const { stdout: result } = await exec("target/debug/mcpx", [...base, "echo", "--message", "native-cli"]);
    assert.equal(JSON.parse(result).value.raw.structuredContent.message, "native-cli");
    const { stdout: toolHelp } = await exec("target/debug/mcpx", [...base, "echo", "--help"]);
    assert.match(toolHelp, /--message/);
    await assert.rejects(exec("target/debug/mcpx", [...base, "echo", "--args", '{"message":10}']), error => {
      const detail = JSON.parse(error.stderr);
      return error.code === 2 && detail.execution_certainty === "not_sent" && detail.retry_advice === "correct_input" && detail.operation_id === undefined;
    });
  }
  assert.equal(requests.filter(request => request.method === "tools/call").length, 2);
  const { stdout: genericUrl } = await exec("target/debug/mcpx", ["--url", endpoint, "--output", "json", "call", "echo", "--message", "generic-url-call"]);
  assert.equal(JSON.parse(genericUrl).value.raw.structuredContent.message, "generic-url-call");
  const callsBeforeCompletion = requests.filter(request => request.method === "tools/call").length;
  for (const line of ["ec", "echo --en", "echo --enabled t", "echo --mode 雪"]) {
    const { stdout } = await exec("target/debug/mcpx", ["--url", endpoint, "--output", "json", "complete", "--line", line]);
    const candidate = JSON.parse(stdout).candidates[0];
    assert.equal(candidate.value, ({ "ec":"echo", "echo --en":"--enabled", "echo --enabled t":"true", "echo --mode 雪":"雪 花" })[line]);
  }
  const beforeInvalidCompletion = requests.length;
  await assert.rejects(exec("target/debug/mcpx", ["--url", endpoint, "complete", "--line", "雪", "--cursor", "1"]), error => error.code === 2 && error.stderr.includes("InvalidCursor"));
  assert.equal(requests.length, beforeInvalidCompletion);
  assert.equal(requests.filter(request => request.method === "tools/call").length, callsBeforeCompletion);
  console.log("PASS dynamic CLI completion: live tool/flag/enum candidates, UTF-8 offsets and zero business requests");
  const collision = createMockServer(undefined, { tools: ["complete", "tools", "call"].map(name => ({ ...echo, name })) });
  await new Promise(resolve => collision.server.listen(0, "127.0.0.1", resolve));
  try {
    for (const name of ["complete", "tools", "call"]) {
      const { stdout } = await exec("target/debug/mcpx", ["--url", `http://127.0.0.1:${collision.server.address().port}/mcp`, "--output", "json", "call", name, "--message", name]);
      assert.equal(JSON.parse(stdout).value.raw.structuredContent.message, name);
    }
    assert.deepEqual(collision.requests.filter(r => r.method === "tools/call").map(r => r.params.name), ["complete", "tools", "call"]);
  } finally { await new Promise(resolve => collision.server.close(resolve)); }
  console.log("PASS native CLI: help, catalog, modern/legacy SSE, schema rejection and exit codes");
  const registryFile = join(directory,"schemas.json");
  await writeFile(registryFile, JSON.stringify(schemaResources().map(r => ({uri:r.uri,schema_json:r.schemaJson}))));
  for (const selectedEndpoint of [endpoint, legacyEndpoint]) {
    await assert.rejects(exec("target/debug/mcpx", ["--url",selectedEndpoint,"ref_echo","--args",'{"message":"registry-absent"}']), error => error.code === 3 && error.stderr.includes("SchemaReferenceUnavailable"));
    const { stdout } = await exec("target/debug/mcpx", ["--url",selectedEndpoint,"--schema-registry",registryFile,"--output","json","ref_echo","--args",'{"message":"registry-cli"}']);
    assert.equal(JSON.parse(stdout).value.output_validation,"valid");
    await assert.rejects(exec("target/debug/mcpx", ["--url",selectedEndpoint,"--schema-registry",registryFile,"unsupported_vocab","--args","{}"]), error => error.code === 3 && error.stderr.includes("SchemaUnsupported"));
  }
  console.log("PASS CLI schema registry: explicit file, modern/legacy input/output refs and unsupported vocabulary rejection");
  const config = join(directory, "mcpx.toml");
  const fixture = resolve("tests/fixtures/stdio_server.py");
  for (const legacy of [false,true]) {
    const configPath = join(directory, `schema-stdio-${legacy}.toml`);
    await writeFile(configPath, `[servers.ref]\ntransport="stdio"\nprogram="python3"\nargs=${JSON.stringify([fixture,"--schema-refs",...(legacy?["--legacy"]:[])])}\n`);
    await assert.rejects(exec("target/debug/mcpx", ["--config",configPath,"ref","echo","--args",'{"message":"registry-absent-stdio"}']), error => error.code === 3 && error.stderr.includes("SchemaReferenceUnavailable"));
    const { stdout } = await exec("target/debug/mcpx", ["--config",configPath,"--schema-registry",registryFile,"--output","json","ref","echo","--args",'{"message":"registry-stdio"}']);
    assert.equal(JSON.parse(stdout).value.output_validation,"valid");
  }
  const requestLog = join(directory, "slow-requests.jsonl");
  await writeFile(config, `schema_version = 1\n[servers.remote]\ntransport = "http"\nurl = ${JSON.stringify(endpoint)}\n[servers.old]\ntransport = "http"\nurl = ${JSON.stringify(legacyEndpoint)}\n[servers.local]\ntransport = "stdio"\nprogram = "python3"\nargs = [${JSON.stringify(fixture)}, "--legacy"]\n[servers.local.env]\nMCPX_FIXTURE_VALUE = "NOT_IN_SERVER_LIST"\n[servers.slow]\ntransport = "stdio"\nprogram = "python3"\nargs = [${JSON.stringify(fixture)}, "--delay", "10", "--requests-log", ${JSON.stringify(requestLog)}]\n`);
  const base = ["--config", config, "--output", "json"];
  for (const alias of ["remote", "old", "local"]) {
    const { stdout } = await exec("target/debug/mcpx", [...base, "complete", alias, "--line", "echo --m"]);
    assert.equal(JSON.parse(stdout).candidates[0].value, "--message");
  }
  const { stdout: aliases } = await exec("target/debug/mcpx", [...base, "servers", "list"]);
  assert.equal(JSON.parse(aliases).length, 4);
  assert.ok(!aliases.includes("NOT_IN_SERVER_LIST"));
  for (const alias of ["remote", "old", "local"]) {
    const { stdout } = await exec("target/debug/mcpx", [...base, alias, "echo", "--message", alias]);
    assert.equal(JSON.parse(stdout).value.raw.structuredContent.message, alias);
  }
  const file = join(directory, "args.json");
  await writeFile(file, JSON.stringify({ message: "file-argument" }));
  const { stdout: fileResult } = await exec("target/debug/mcpx", [...base, "call", "local", "echo", "--args-file", file]);
  assert.equal(JSON.parse(fileResult).value.raw.structuredContent.message, "file-argument");
  const { stdout: text } = await exec("target/debug/mcpx", ["--config", config, "remote", "echo", "--message", "plain-text"]);
  assert.equal(text.trim(), "plain-text");
  const { stdout: ndjson } = await exec("target/debug/mcpx", ["--config", config, "--output", "ndjson-events", "remote", "echo", "--message", "events"]);
  const records = ndjson.trim().split('\n').map(JSON.parse);
  assert.ok(records.some(record => record.type === "event" && record.event.kind === "progress"));
  assert.equal(records.at(-1).type, "result");
  assert.equal(records.at(-1).outcome.value.raw.structuredContent.message, "events");
  console.log("PASS named config: offline list, HTTP/stdio auto protocol, generic call, args-file, text and NDJSON");

  const child = spawn("target/debug/mcpx", ["--config", config, "--output", "ndjson-events", "slow", "echo", "--message", "cancel"], { stdio: ["ignore", "pipe", "pipe"] });
  let stderr = "";
  child.stdout.resume();
  child.stderr.on("data", bytes => { stderr += bytes; });
  const completion = new Promise((resolve, reject) => { child.once("error", reject); child.once("exit", (code, signal) => resolve({ code, signal })); });
  let pid;
  try {
    for (let attempt = 0; attempt < 300; attempt++) {
      try {
        const requests = (await readFile(requestLog, "utf8")).trim().split('\n').map(JSON.parse);
        pid = requests.find(request => request.method === "tools/call")?.pid;
        if (pid) break;
      } catch {}
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    assert.ok(pid, "slow operation must have been sent before cancelling");
    child.kill("SIGINT");
    const result = await completion;
    assert.equal(result.code, 130);
    assert.match(stderr, /Cancelled/);
    const detail = JSON.parse(stderr);
    assert.equal(detail.execution_certainty,"unknown");assert.equal(detail.retry_advice,"inspect_execution");assert.ok(Number.isSafeInteger(detail.operation_id));
    assert.throws(() => process.kill(pid, 0), error => error.code === "ESRCH");
  } finally { if (child.exitCode === null) child.kill("SIGKILL"); }
  console.log("PASS native Ctrl-C: exit 130, in-flight process terminated and reaped");
} finally {
  await new Promise(resolve => server.close(resolve));
  await new Promise(resolve => legacyServer.close(resolve));
  await rm(directory, { recursive: true, force: true });
}
