import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createMockServer } from "./mock-server.mjs";
import { engine } from "../dist/component/mcpx_component.js";
import { driveComponent, closeComponent } from "../examples/wasi-component/host.mjs";
import { componentRuntime } from "../web/component.js";
import { schemaResources } from "./schema-fixture.mjs";

const fixtures = [];
try {
  const metadata = componentRuntime({ tools: () => [{ name: "future", description: undefined, inputSchemaJson: '{"type":"object"}', outputSchemaJson: undefined, extensionsJson: '{"opaque":18446744073709551615}' }] }).tools();
  assert.match(metadata, /"opaque":18446744073709551615/);
  assert.throws(() => engine.create("invalid", false), error => error.code === "InvalidEndpoint" && Array.isArray(error.issues) && error.executionCertainty === "not-sent");
  for (const legacy of [false, true]) {
    const fixture = createMockServer(undefined, { legacy, sessions: legacy, concurrencyBarrier: 16 });
    fixtures.push(fixture);
    await new Promise(resolve => fixture.server.listen(0, "127.0.0.1", resolve));
    const endpoint = `http://127.0.0.1:${fixture.server.address().port}/mcp`;
    for (const automatic of [false, true]) {
      const remote = automatic ? engine.createAuto(endpoint) : engine.create(endpoint, legacy);
      const events = [];
      const drive = options => driveComponent(remote, { onEvents: batch => { assert.equal(batch.dropped, 0n); events.push(...batch.events); }, ...options });
      try {
        remote.acceptAuthorization("anonymous-test");
        remote.setSchemaRegistry(schemaResources());
        remote.setElicitation(true, false);
        remote.discover(); await drive();
        assert.equal(remote.catalogStatus("anonymous-test").needsRefresh, false);
        const tools = remote.tools();
        const echo = tools.find(tool => tool.name === "echo");
        assert.equal(JSON.parse(echo.inputSchemaJson).properties.message.type, "string");
        assert.equal(typeof echo.extensionsJson, "string");
        assert.ok(events.some(event => event.kind.tag === "catalog-updated"));
        assert.equal(remote.schemaRegistryStats().resources, 3);
        const referenced = remote.bind("ref_echo");
        assert.throws(() => remote.callConcurrent(referenced, '{"message":3}'), error => error.code === "ValidationFailed");
        const registeredCall = remote.callConcurrent(referenced, '{"message":"registry-wit"}');
        assert.throws(() => remote.setSchemaRegistry([]), error => error.code === "Busy");
        await drive();
        assert.equal(remote.completion(registeredCall).val.outputValidation, "valid");
        assert.throws(() => remote.setSchemaRegistry([{uri:"https://schemas.mcpx.test/invalid",schemaJson:'{"$vocabulary":{"https://example.com/no":true}}'}]), error => error.code === "SchemaUnsupported");
        assert.equal(remote.schemaRegistryStats().resources, 3);
        remote.releaseBinding(referenced);
        assert.equal(remote.completeLine("ec", 2).candidates[0].value, "echo");
        assert.equal(remote.completeLine("echo --enabled t", 16).candidates[0].value, "true");
        assert.ok(remote.commandSpec("echo").parameters.some(p => p.name === "mode" && p.values.includes("two words")));
        assert.throws(() => remote.completeLine("雪", 1), error => error.code === "InvalidCursor");
        const binding = remote.bind("echo");
        assert.equal(typeof binding, "bigint");
        assert.throws(() => remote.callConcurrent(binding, '{"message":1}'), error => error.code === "ValidationFailed" && error.issues.some(issue => issue.instancePath === "/message"));
        assert.equal(remote.commandLineConcurrent("echo --help").tag, "help");
        assert.throws(() => remote.commandLineConcurrent("echo --message '"), error => error.code === "TokenizeError");
        const operations = Array.from({ length: 16 }, (_, index) => index % 3 === 0
          ? remote.callConcurrent(binding, JSON.stringify({ message: `parallel-${index}` }))
          : index % 3 === 1 ? remote.commandConcurrent("echo", ["--message", `parallel-${index}`]).val
            : remote.commandLineConcurrent(`echo --message 'parallel-${index}'`).val);
        remote.setValidatorCacheCapacity(0);
        remote.clearValidatorCache();
        assert.equal(remote.validatorCacheStats().entries, 0);
        assert.equal(remote.validatorCacheStats().capacity, 0);
        assert.equal(typeof remote.validatorCacheStats().compilations, "bigint");
        assert.throws(() => remote.setValidatorCacheCapacity(4097), error => error.code === "ConfigurationError");
        await drive();
        for (const [index, operation] of operations.entries()) {
          const outcome = remote.completion(operation);
          assert.equal(outcome.tag, "completed");
          assert.equal(outcome.val.operationId, operation);
          assert.equal(outcome.val.outputValidation, "valid");
          assert.equal(JSON.parse(outcome.val.rawJson).structuredContent.message, `parallel-${index}`);
          assert.equal(remote.completion(operation), undefined);
        }
        assert.equal(new Set(events.filter(event => event.kind.tag === "completed").map(event => event.operationId)).size, 17);
        assert.ok(events.every(event => typeof event.operationId === "bigint" && typeof event.sequence === "bigint"));
        const raw = remote.callConcurrent(binding, '{"message":"raw-number"}');
        await drive();
        assert.match(remote.completion(raw).val.rawJson, /"n":18446744073709551615/);
        if (!legacy) {
          const operation = remote.callConcurrent(binding, '{"message":"interactive"}');
          await drive();
          const pending = remote.completion(operation);
          assert.equal(pending.tag, "needs-input");
          assert.equal(typeof pending.val.id, "bigint");
          assert.match(pending.val.rawJson, /fixture opaque/);
          assert.throws(() => remote.resumeConcurrent(pending.val.id, '{"confirm":{"action":"accept","content":{"yes":1}}}'), error => error.code === "ValidationFailed");
          const resumed = remote.resumeConcurrent(pending.val.id, '{"confirm":{"action":"accept","content":{"yes":true}}}');
          await drive();
          assert.equal(JSON.parse(remote.completion(resumed).val.rawJson).structuredContent.message, "interactive");
        }
        const cancelled = remote.callConcurrent(binding, '{"message":"slow-typed-component-cancel"}');
        await assert.rejects(drive({ onEvents: batch => {
          assert.equal(batch.dropped, 0n); events.push(...batch.events);
          for (const event of batch.events) if (event.operationId === cancelled && event.kind.tag === "progress") remote.cancelOperation(cancelled);
        } }), error => error.code === "Cancelled");
        assert.throws(() => remote.completion(cancelled), error => error.code === "Cancelled" && error.operationId === cancelled && error.executionCertainty === "unknown" && error.retryAdvice === "inspect-execution");
        const timedOut = remote.callConcurrent(binding, '{"message":"slow-typed-component-timeout"}');
        await assert.rejects(drive({ timeout: 100 }), error => error.code === "Timeout");
        assert.throws(() => remote.completion(timedOut), error => error.code === "Timeout");
        assert.ok(events.some(event => event.operationId === timedOut && event.kind.tag === "failed" && event.failure?.code === "Timeout"));
        const after = remote.callConcurrent(binding, '{"message":"typed-after-cancel"}');
        await drive();
        assert.equal(JSON.parse(remote.completion(after).val.rawJson).structuredContent.message, "typed-after-cancel");
        const transportFailed = remote.callConcurrent(binding, '{"message":"typed-unsent-host-error"}');
        await assert.rejects(drive({ fetch: () => { throw Object.assign(new Error("secret-host-token"), { code: "ECONNRESET" }); } }), error => error.code === "HostError" && !error.message.includes("secret-host-token") && error.execution_certainty === "unknown" && error.operation_id === Number(transportFailed));
        assert.throws(() => remote.completion(transportFailed), error => error.code === "HostError" && !error.message.includes("secret-host-token"));
        remote.setEventLimits(4, 49152);
        const flood = remote.callConcurrent(binding, '{"message":"burst-worker"}');
        let dropped = 0n, progress = 0;
        await drive({ onEvents: batch => { dropped += batch.dropped; progress += batch.events.filter(event => event.kind.tag === "progress").length; } });
        assert.ok(dropped > 0n);
        assert.equal(dropped + BigInt(progress), 100n);
        assert.equal(remote.completion(flood).tag, "completed");
        assert.equal(remote.eventStats().reservedTerminals, 0);
        assert.equal(remote.eventStats().bytes, 0n);
        remote.releaseBinding(binding);
        assert.equal(remote.busy(), false);
      } finally { await closeComponent(remote); }
    }
    if (legacy) assert.equal(fixture.transportRequests.filter(request => request.method === "DELETE").length, 2);
    const cancellations = fixture.requests.filter(request => request.method === "notifications/cancelled");
    assert.equal(cancellations.length, legacy ? 4 : 0);
    for (const cancel of cancellations) assert.ok(fixture.requests.some(request => request.id === cancel.params.requestId && request.method === "tools/call"));
    const child = await promisify(execFile)(process.execPath, ["examples/wasi-component/main.mjs", endpoint, "echo", '{"message":"typed-example-child"}'], { timeout: 10_000 });
    assert.equal(JSON.parse(child.stdout).structuredContent.message, "typed-example-child");
    assert.equal(child.stderr, "");
    console.log(`PASS typed WIT ${legacy ? "legacy" : "modern"}: explicit/auto, errors/pointers, binary HTTP, 16-way barrier, raw uint64, outcomes, cancel/deadline, session cleanup and executable example`);
  }
} finally {
  for (const fixture of fixtures) await new Promise(resolve => fixture.server.close(resolve));
}
