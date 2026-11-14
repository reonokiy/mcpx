export const COMMON_SCHEMA = "https://schemas.mcpx.test/common.json";
export const RESULT_SCHEMA = "https://schemas.mcpx.test/result.json";
export const INPUT_SCHEMA = "https://schemas.mcpx.test/input.json";
export const COSTLY_PATTERN = String.raw`^(a|a)*\1b`;
export const COSTLY_VALUE = `${"a".repeat(40)}!`;
export const referencedTool = {
  name: "ref_echo", inputSchema: { $id: INPUT_SCHEMA, type: "object", properties: { message: { $ref: `${COMMON_SCHEMA}#/$defs/message` } }, required: ["message"], additionalProperties: false },
  outputSchema: { $ref: RESULT_SCHEMA },
};
export const unsupportedTool = {
  name: "unsupported_vocab", inputSchema: { type: "object", $vocabulary: { "https://json-schema.org/draft/2020-12/vocab/not-real": true } },
};
export function schemaResources(mode) {
  return [
    { uri: INPUT_SCHEMA, schemaJson: JSON.stringify(referencedTool.inputSchema) },
    { uri: COMMON_SCHEMA, schemaJson: JSON.stringify({ $id: COMMON_SCHEMA, $defs: {
      message: { type: "string", ...(mode === "changed" ? { enum: ["schema-updated"] } : { minLength: 1 }) },
      confirm: { type: "object", properties: { yes: { type: "boolean" } }, required: ["yes"] },
    } }) },
    { uri: RESULT_SCHEMA, schemaJson: JSON.stringify({ $id: RESULT_SCHEMA, type: "object", properties: { message: { $ref: "common.json#/$defs/message" } }, required: [mode === "bad-output" ? "missing" : "message"] }) },
  ];
}

export function regexResources(messageSchema) {
  return schemaResources().map(resource => {
    if (resource.uri !== COMMON_SCHEMA) return resource;
    const schema = JSON.parse(resource.schemaJson);
    schema.$defs.message = messageSchema;
    return { ...resource, schemaJson:JSON.stringify(schema) };
  });
}
export function outputRegexResources() {
  return schemaResources().map(resource => {
    if (resource.uri !== RESULT_SCHEMA) return resource;
    const schema = JSON.parse(resource.schemaJson);
    schema.properties.message = {pattern:COSTLY_PATTERN};
    return {...resource, schemaJson:JSON.stringify(schema)};
  });
}

async function exerciseRegexPolicy(remote) {
  const costly = {pattern:COSTLY_PATTERN};
  for (const schema of [costly, {not:costly}, {if:costly,then:false,else:true}]) {
    await remote.setSchemaRegistry(regexResources(schema));
    const stats = await remote.schemaRegistryStats();
    check(stats.patterns === 1 && stats.pattern_bytes === COSTLY_PATTERN.length, "Pattern accounting was lost across host ABI");
    const guarded = await remote.freshTool("ref_echo");
    const error = await rejected(guarded.call({message:COSTLY_VALUE}), "ValidationLimitExceeded");
    check(error.execution_certainty === "not_sent" && error.issues.length === 1, "Regex exhaustion lost safe input diagnostics");
    await guarded.dispose();
  }
  await remote.setSchemaRegistry(regexResources(costly));
  const retained = await remote.freshTool("ref_echo");
  for (const [pattern, code] of [["x".repeat(4097),"LimitExceeded"], ["^(?:a|b){200000}$","SchemaUnsupported"], ["((?=a)){1000000000}","SchemaUnsupported"]]) {
    await rejected(remote.setSchemaRegistry(regexResources({pattern})), code);
    check((await remote.schemaRegistryStats()).pattern_bytes === COSTLY_PATTERN.length, "Rejected regex changed the prepared registry");
    check((await retained.call({message:"aab"})).value.output_validation === "valid", "Rejected regex invalidated a usable binding");
  }
  await retained.dispose();
  await remote.setSchemaRegistry(regexResources({anyOf:[costly,{not:costly}]}));
  const tautology = await remote.freshTool("ref_echo");
  check((await tautology.call({message:COSTLY_VALUE})).value.output_validation === "valid", "Provable regex tautology changed validity");
  await tautology.dispose();
  await remote.setSchemaRegistry(outputRegexResources());
  const output = await remote.freshTool("ref_echo");
  const outcome = await output.call({message:COSTLY_VALUE});
  check(outcome.value.output_validation === "indeterminate" && outcome.value.raw.structuredContent.message === COSTLY_VALUE, "Unfinished output validation lost its status or raw result");
  await output.dispose();
  await remote.setSchemaRegistry(schemaResources());
}

function check(condition, message) { if (!condition) throw new Error(message); }
async function rejected(promise, code) {
  try { await promise; } catch (error) {
    if (typeof error === "string") error = JSON.parse(error);
    check(error.code === code, `Registry expected ${code}, got ${error.code}: ${error.message}`); return error;
  }
  throw new Error(`Registry expected ${code}`);
}
// Identical scenario for raw Wasm, jco and Worker owners. No tools/call needed.
export async function exerciseValidatorCache(remote) {
  await remote.setValidatorCacheCapacity(2);
  await remote.clearValidatorCache();
  const initial = await remote.validatorCacheStats();
  check(initial.capacity === 2 && initial.entries === 0, "Cache configuration was lost");
  const first = await remote.freshTool("echo");
  const cold = await remote.validatorCacheStats();
  check(cold.entries === 2 && cold.compilations === initial.compilations + 2, "Input/output were not cached separately");
  await first.dispose();
  const live = await remote.freshTool("echo");
  const warm = await remote.validatorCacheStats();
  check(warm.hits === cold.hits + 2 && warm.compilations === cold.compilations, "Rebinding recompiled retained validators");
  await remote.clearValidatorCache();
  check((await remote.validatorCacheStats()).entries === 0, "Clear retained cached validators");
  await rejected(live.call({message:3}), "ValidationFailed");
  await live.dispose();
  await remote.setValidatorCacheCapacity(0);
  for (let i=0; i<2; i++) await (await remote.freshTool("echo")).dispose();
  const disabled = await remote.validatorCacheStats();
  check(disabled.entries === 0 && disabled.compilations === cold.compilations + 4, "Disabled cache retained or skipped compilation");
  await remote.setValidatorCacheCapacity(2);
}
export async function exerciseSchemaRegistry(remote, { legacy = false, rotateIdentity } = {}) {
  await exerciseRegexPolicy(remote);
  check((await remote.schemaRegistryStats()).resources === 3, "Constructor lost registered schemas");
  await rejected(remote.freshTool("unsupported_vocab"), "SchemaUnsupported");
  const binding = await remote.freshTool("ref_echo");
  await rejected(binding.call({ message: 3 }), "ValidationFailed");
  const outcome = await binding.call({ message: "registry-ready" });
  check(outcome.value.raw.structuredContent.message === "registry-ready" && outcome.value.output_validation === "valid", "Registered input/output refs failed");
  await rejected(remote.setSchemaRegistry([{ uri:COMMON_SCHEMA, schemaJson:'{"$vocabulary":{"https://example.com/required":true}}' }]), "SchemaUnsupported");
  check((await remote.schemaRegistryStats()).resources === 3, "Invalid bundle changed registry");
  check((await binding.call({ message: "registry-after-invalid" })).value.output_validation === "valid", "Failed replacement invalidated old bindings");
  let pending;
  if (!legacy) {
    pending = await binding.call({ message: "interactive-ref" });
    check(pending.kind === "needs_input", "Registered form Schema did not produce a continuation");
    await rejected(remote.resume(pending.value, { confirm:{action:"accept",content:{yes:1}} }), "ValidationFailed");
    check((await remote.resume(pending.value, { confirm:{action:"accept",content:{yes:true}} })).kind === "completed", "Registered form Schema could not resume");
    pending = await binding.call({ message: "interactive-ref" });
  }
  await remote.setSchemaRegistry(schemaResources("changed"));
  check((await remote.validatorCacheStats()).entries === 0, "Registry replacement retained old validators");
  await rejected(binding.call({ message:"registry-old" }), "BindingStale");
  if (pending) await rejected(remote.resume(pending.value, {confirm:{action:"accept",content:{yes:true}}}), "InvalidContinuation");
  await binding.dispose();
  const fresh = await remote.freshTool("ref_echo");
  await rejected(fresh.call({ message:"registry-unissued" }), "ValidationFailed");
  check((await fresh.call({ message:"schema-updated" })).value.output_validation === "valid", "Replacement did not compile new resource snapshot");
  await remote.setSchemaRegistry(schemaResources("bad-output"));
  const invalidOutput = await (await remote.freshTool("ref_echo")).call({ message:"registry-bad-output" });
  check(invalidOutput.value.output_validation === "invalid", "Registered output Schema was ignored");
  await remote.setSchemaRegistry([]);
  check((await remote.schemaRegistryStats()).resources === 0, "Empty replacement retained resources");
  await rejected(remote.freshTool("ref_echo"), "SchemaReferenceUnavailable");
  if (rotateIdentity) {
    await remote.setSchemaRegistry(schemaResources());
    const old = await remote.freshTool("ref_echo");
    rotateIdentity();
    check((await remote.schemaRegistryStats()).resources === 0, "Schema resources crossed authorization identities");
    check((await remote.validatorCacheStats()).entries === 0, "Validators crossed authorization identities");
    await rejected(old.call({message:"registry-old-principal"}), "BindingStale");
    await rejected(remote.freshTool("ref_echo"), "SchemaReferenceUnavailable");
    await remote.setSchemaRegistry(schemaResources());
    check((await (await remote.freshTool("ref_echo")).call({message:"registry-new-principal"})).value.output_validation === "valid", "New identity could not register its own resources");
  }
}
