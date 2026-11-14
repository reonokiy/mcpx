import { createServer } from "node:http";
import { COMMON_SCHEMA, referencedTool, unsupportedTool } from "./schema-fixture.mjs";

export const echo = {
  name: "echo", description: "Echo a schema-checked message",
  inputSchema: { type: "object", properties: { message: { type: "string" }, enabled: { type: "boolean" }, mode: { type: "string", enum: ["fast", "two words", "雪 花"] } }, required: ["message"], additionalProperties: false },
  outputSchema: { type: "object", properties: { message: { type: "string" } }, required: ["message"] },
};

export function createMockServer(staticHandler = (_req, res) => { res.writeHead(404).end(); }, { legacy = false, concurrencyBarrier = 0, serverFactory = createServer, catalogTtl, sessions = false, tools = [echo, referencedTool, unsupportedTool] } = {}) {
  const requests = [];
  const transportRequests = [];
  const closedRequests = [];
  let parallelWaiters = [];
  const server = serverFactory(async (req, res) => {
    if (new URL(req.url, "http://localhost").pathname !== "/mcp") return staticHandler(req, res);
    res.setHeader("access-control-allow-origin", "*");
    res.setHeader("access-control-allow-headers", "authorization, content-type, mcp-protocol-version, mcp-session-id, mcp-method, mcp-name");
    res.setHeader("access-control-expose-headers", "mcp-session-id");
    res.setHeader("access-control-allow-methods", "POST, GET, DELETE, OPTIONS");
    if (req.method === "OPTIONS") return res.writeHead(204).end();
    transportRequests.push({ method: req.method, auth: req.headers.authorization, session: req.headers["mcp-session-id"] });
    if (sessions && req.method === "DELETE") return res.writeHead(204).end();
    if (req.method !== "POST") return res.writeHead(405).end();
    try {
      let body = "";
      for await (const chunk of req) {
        body += chunk;
        if (body.length > 1_048_576) throw new Error("Request too large");
      }
      const rpc = JSON.parse(body);
      requests.push(rpc);
      if (rpc.method === "tools/call") res.once("close", () => closedRequests.push(rpc));
      const modern = rpc.params?._meta?.["io.modelcontextprotocol/protocolVersion"];
      if (modern && (req.headers["mcp-method"] !== rpc.method || (rpc.method === "tools/call" && req.headers["mcp-name"] !== rpc.params.name))) {
        throw new Error("Missing modern routing headers");
      }
      if (legacy && rpc.method === "server/discover") {
        return res.writeHead(400, { "content-type": "application/json" }).end(JSON.stringify({ jsonrpc: "2.0", id: rpc.id, error: { code: -32601, message: "initialize first" } }));
      }
      let result;
      switch (rpc.method) {
        case "server/discover":
          if (modern !== "2026-07-28") throw new Error("Missing modern metadata");
          result = { supportedVersions: ["2026-07-28"], capabilities: { tools: {} } };
          break;
        case "initialize":
          if (sessions) res.setHeader("mcp-session-id", `session-${requests.length}`);
          result = { protocolVersion: "2025-11-25", capabilities: { tools: {} } }; break;
        case "notifications/initialized": return res.writeHead(202).end();
        case "notifications/cancelled": return res.writeHead(202).end();
        case "tools/list": result = { tools, ...(catalogTtl === undefined ? {} : { ttlMs: catalogTtl }) }; break;
        case "tools/call":
          if (concurrencyBarrier > 0 && rpc.params.arguments.message?.startsWith("parallel-")) {
            const waiting = new Promise(resolve => parallelWaiters.push(resolve));
            if (parallelWaiters.length === concurrencyBarrier) {
              const batch = parallelWaiters; parallelWaiters = [];
              for (const resolve of batch) resolve();
            }
            await waiting;
          }
          if (modern && ["interactive", "interactive-repeat", "interactive-ref"].includes(rpc.params.arguments.message)) {
            if (!rpc.params.inputResponses || rpc.params.arguments.message === "interactive-repeat" && rpc.params.requestState === "fixture opaque\u0000state") {
              if (!rpc.params._meta["io.modelcontextprotocol/clientCapabilities"]?.elicitation?.form) throw new Error("Form capability missing");
              result = { resultType: "input_required", requestState: rpc.params.inputResponses ? "fixture second state" : "fixture opaque\u0000state", inputRequests: { confirm: { method: "elicitation/create", params: { message: "Continue?", requestedSchema: rpc.params.arguments.message === "interactive-ref" ? { $ref: `${COMMON_SCHEMA}#/$defs/confirm` } : { type: "object", properties: { yes: { type: "boolean" } }, required: ["yes"] } } } } };
              break;
            }
            const expectedState = rpc.params.arguments.message === "interactive-repeat" ? "fixture second state" : "fixture opaque\u0000state";
            if (rpc.params.requestState !== expectedState || rpc.params.inputResponses.confirm?.content?.yes !== true) throw new Error("Invalid resumed request");
          }
          result = { content: [{ type: "text", text: rpc.params.arguments.message }], structuredContent: rpc.params.arguments };
          if (rpc.params.arguments.message === "raw-number") result.structuredContent = { ...rpc.params.arguments, n: "__mcpx_large_number__" };
          break;
        default: return res.writeHead(400).end();
      }
      const response = { jsonrpc: "2.0", id: rpc.id, result };
      if (rpc.method === "tools/call") {
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.write(`data: ${JSON.stringify({ jsonrpc: "2.0", method: "notifications/progress", params: { progress: 1, progressToken: rpc.params._meta?.progressToken } })}\n\n`);
        if (rpc.params.arguments.message === "unsafe-event-number") {
          res.write(`data: ${JSON.stringify({ jsonrpc: "2.0", method: "notifications/message", params: { data: "__mcpx_large_number__" } }).replace('"__mcpx_large_number__"', '18446744073709551615')}\n\n`);
        }
        if (rpc.params.arguments.message === "burst-worker") {
          for (let progress = 2; progress <= 100; progress++) res.write(`data: ${JSON.stringify({ jsonrpc: "2.0", method: "notifications/progress", params: { progress, progressToken: rpc.params._meta?.progressToken } })}\n\n`);
        }
        if (rpc.params.arguments.message?.startsWith("slow-")) {
          await new Promise(resolve => {
            const delay = ["slow-worker-frozen", "slow-worker-collateral"].includes(rpc.params.arguments.message) ? 10_000 : 1000;
            const timer = setTimeout(resolve, delay);
            res.once("close", () => { clearTimeout(timer); resolve(); });
          });
          if (res.destroyed) return;
        }
        const wire = `data: ${JSON.stringify(response).replace('"__mcpx_large_number__"', '18446744073709551615')}\n\n`;
        for (let i = 0; i < wire.length; i += 7) res.write(wire.slice(i, i + 7));
        res.end();
      } else res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(response));
    } catch { res.writeHead(400).end(); }
  });
  return { server, requests, closedRequests, transportRequests };
}

if (process.argv[1] === new URL(import.meta.url).pathname) {
  const { server } = createMockServer();
  server.listen(0, "127.0.0.1", () => console.log(`http://127.0.0.1:${server.address().port}/mcp`));
}
