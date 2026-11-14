const failure = (code, message) => Object.assign(new Error(message), { code });

// Only fixed host records are encoded here. Schema text stays opaque until Rust parses it.
export function schemaRegistryJson(resources) {
  if (!Array.isArray(resources)) throw new TypeError("schemaRegistry must be an array of uri/schemaJson records");
  if (resources.length > 128) throw failure("LimitExceeded", "schema registry exceeds 128 resources");
  const encoder = new TextEncoder();
  let bytes = 0;
  const result = resources.map(resource => {
    if (!resource || typeof resource.uri !== "string" || typeof resource.schemaJson !== "string" || Object.keys(resource).some(key => !["uri", "schemaJson"].includes(key))) throw failure("ConfigurationError", "schema resources require uri/schemaJson string fields");
    const uriBytes = encoder.encode(resource.uri).byteLength, schemaBytes = encoder.encode(resource.schemaJson).byteLength;
    if (uriBytes > 2048 || schemaBytes > 1024 * 1024) throw failure("LimitExceeded", "schema resource exceeds URI or document byte limit");
    bytes += uriBytes + schemaBytes + 128;
    if (bytes > 8 * 1024 * 1024) throw failure("LimitExceeded", "schema resources exceed 8 MiB source budget");
    return { uri: resource.uri, schema_json: resource.schemaJson };
  });
  const json = JSON.stringify(result);
  if (encoder.encode(json).byteLength > 16 * 1024 * 1024) throw failure("LimitExceeded", "schema bundle exceeds 16 MiB");
  return json;
}
