import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createMockServer, echo } from "./mock-server.mjs";
import { schemaResources } from "./schema-fixture.mjs";

const wasmtime=process.env.WASMTIME ?? "target/tools/wasmtime/wasmtime";
const cli="target/wasm32-wasip2/debug/mcpx.wasm";
const library="target/wasm32-wasip2/debug/examples/library.wasm";
const directory=await mkdtemp(join(tmpdir(),"mcpx-wasi-cli-"));
const fixtures=[];
let children=0;
async function execute(module,args=[], {host=[],input=""}={}) {
  children++;
  return new Promise((resolve,reject)=>{
    const child=spawn(wasmtime,["run","-S","http","-S","cli-exit-with-code",...host,module,...args],{stdio:["pipe","pipe","pipe"]});
    let stdout="",stderr="";
    const timer=setTimeout(()=>{child.kill("SIGKILL");reject(new Error("WASI subprocess did not exit"));},20000);
    child.on("error",error=>{clearTimeout(timer);reject(error);});
    child.stdout.on("data",chunk=>{stdout+=chunk;if(stdout.length>4*1024*1024)child.kill("SIGKILL");});
    child.stderr.on("data",chunk=>{stderr+=chunk;if(stderr.length>1048576)child.kill("SIGKILL");});
    child.stdin.on("error",()=>{});child.stdin.end(input);
    child.on("close",(code,signal)=>{clearTimeout(timer);resolve({code,signal,stdout,stderr});});
  });
}
function ok(result) {assert.equal(result.signal,null,result.stderr);assert.equal(result.code,0,result.stderr);assert.equal(result.stderr,"");return result.stdout;}
function failed(result,code,name) {assert.equal(result.signal,null,result.stderr);assert.equal(result.code,code,result.stderr);assert.equal(result.stdout,"");assert.equal(JSON.parse(result.stderr).code,name);return JSON.parse(result.stderr);}
function message(outcome) {return outcome.value.raw.structuredContent.message;}
async function listen(fixture) {fixtures.push(fixture);await new Promise(r=>fixture.server.listen(0,"127.0.0.1",r));return `http://127.0.0.1:${fixture.server.address().port}/mcp`;}
try {
  assert.match(ok(await execute(cli,["--help"])),/WASI HTTP/);
  assert.match(ok(await execute(cli,["complete","--help"])),/--line/);
  const missingRoute=await execute(cli,["echo","--message","missing-url"]);
  assert.equal(missingRoute.code,2);
  assert.equal(missingRoute.stdout,"");
  assert.match(missingRoute.stderr,/Usage: mcpx/);
  assert.doesNotMatch(missingRoute.stderr,/"execution_certainty"/);
  failed(await execute(cli,["call","missing","echo"]),3,"ConfigurationError");
  await writeFile(join(directory,"args.json"),'{"message":"wasi-file"}');
  await writeFile(join(directory,"large.json"),JSON.stringify({message:"x".repeat(192*1024)}));
  await writeFile(join(directory,"schemas.json"),JSON.stringify(schemaResources().map(r=>({uri:r.uri,schema_json:r.schemaJson}))));
  const host=["--dir",`${resolve(directory)}::/data`];
  for (const legacy of [false,true]) {
    const fixture=createMockServer(undefined,{legacy,sessions:legacy});
    const endpoint=await listen(fixture);
    for (const protocol of [legacy?"2025-11-25":"2026-07-28","auto"]) {
      const before=fixture.transportRequests.filter(r=>r.method==="DELETE").length;
      const global=["--url",endpoint,"--protocol",protocol,"--output","json"];
      const call=await execute(cli,[...global,"echo","--message",`wasi-${protocol}`]);
      assert.equal(message(JSON.parse(ok(call))),`wasi-${protocol}`);
      const count=fixture.requests.filter(r=>r.method==="tools/call").length;
      assert.ok(JSON.parse(ok(await execute(cli,[...global,"tools"]))).some(t=>t.name==="echo"));
      assert.match(ok(await execute(cli,[...global,"echo","--help"])),/--message/);
      assert.ok(JSON.parse(ok(await execute(cli,[...global,"complete","--line","echo --mode tw"]))).candidates.some(c=>c.value==="two words"));
      const invalid = failed(await execute(cli,[...global,"echo","--args",'{"message":5}']),2,"ValidationFailed");
      assert.equal(invalid.execution_certainty,"not_sent");assert.equal(invalid.retry_advice,"correct_input");assert.equal(invalid.operation_id,undefined);
      assert.equal(fixture.requests.filter(r=>r.method==="tools/call").length,count,"Metadata/input failure sent a business call");
      const deletes=fixture.transportRequests.filter(r=>r.method==="DELETE").length-before;
      assert.equal(deletes,legacy?5:0,"WASI did not delete each legacy session");
    }
    const global=["--url",endpoint,"--output","json"];
    assert.equal(JSON.parse(ok(await execute(cli,[...global,"complete","--line","--args-file"]))).candidates.length,0,"Completion interpreted a line token as a file option");
    assert.equal(message(JSON.parse(ok(await execute(cli,[...global,"echo","--args-file","/data/args.json"],{host})))),"wasi-file");
    assert.equal(message(JSON.parse(ok(await execute(cli,[...global,"echo","--args-file","/data/large.json"],{host})))).length,192*1024,"Chunked outgoing body/response was truncated");
    failed(await execute(cli,[...global,"echo","--args-file","/data/args.json"]),3,"ConfigurationError");
    assert.equal(message(JSON.parse(ok(await execute(cli,[...global,"echo","--args-file","-"],{input:'{"message":"wasi-stdin"}'})))),"wasi-stdin");
    const registered=await execute(cli,[...global,"--schema-registry","/data/schemas.json","ref_echo","--args",'{"message":"wasi-registry"}'],{host});
    assert.equal(JSON.parse(ok(registered)).value.output_validation,"valid");
    failed(await execute(cli,[...global,"ref_echo","--args",'{"message":"missing-resource"}']),3,"SchemaReferenceUnavailable");
    const raw=ok(await execute(cli,[...global,"echo","--message","raw-number"]));
    assert.match(raw,/18446744073709551615/);
    const ndjson=ok(await execute(cli,["--url",endpoint,"--output","ndjson-events","echo","--message","wasi-events"])).trim().split("\n").map(JSON.parse);
    assert.ok(ndjson.some(r=>r.type==="event"&&r.event.kind==="progress"));
    assert.equal(ndjson.at(-1).type,"result");
    const envHost=["--env","MCPX_WASI_TEST_TOKEN=fixture-only-token"];
    ok(await execute(cli,[...global,"--token-env","MCPX_WASI_TEST_TOKEN","echo","--message","wasi-credential"],{host:envHost}));
    assert.ok(fixture.transportRequests.some(r=>r.auth==="Bearer fixture-only-token"));
    failed(await execute(cli,[...global,"--token-env","MCPX_WASI_TEST_TOKEN","echo","--message","never-env"]),4,"CredentialUnavailable");
    const example=JSON.parse(ok(await execute(library,[endpoint,...(legacy?["legacy"]:[])])));
    assert.equal(message(example.typed),"wasi-typed");assert.equal(message(example.serde),"wasi-serde");
    assert.equal(example.decoded.message,"wasi-serde");
    assert.equal(message(example.string),"wasi string");assert.equal(message(example.argv),"wasi argv");
    if (!legacy) assert.equal(message(example.resumed),"interactive");
    console.log(`PASS Wasmtime WASI ${legacy?"legacy":"modern"}: explicit/Auto, discovery, JSON/SSE, validation, help/completion, files/stdin/preopens, env, Schema refs, uint64, events, session cleanup and Rust library`);
  }
  let cleanupAttempts=0;
  const cleanupFixture=createMockServer(undefined,{legacy:true,sessions:true,serverFactory:handler=>createServer((req,res)=>{
    if(req.method==="DELETE"&&++cleanupAttempts===1) return res.writeHead(503).end();
    return handler(req,res);
  })});
  const cleanupEndpoint=await listen(cleanupFixture);
  ok(await execute(library,[cleanupEndpoint,"legacy"]));
  assert.equal(cleanupAttempts,2,"Failed guest session cleanup could not be explicitly retried");
  for (const status of [401,403,307]) {
    const seen=[];
    const fixture={server:createServer(async(req,res)=>{
      let text="";for await(const chunk of req)text+=chunk;
      seen.push({path:req.url,method:JSON.parse(text).method});
      res.writeHead(status,{location:"/redirect-target"}).end();
    })};
    const endpoint=await listen(fixture);
    const error = failed(await execute(cli,["--url",endpoint,"tools"]),status===307?5:4,status===307?"HttpError":"AuthFailed");
    assert.equal(error.execution_certainty,"not_sent");assert.ok(Number.isSafeInteger(error.operation_id));
    if(status!==307)assert.equal(error.retry_advice,"refresh_authentication");
    assert.deepEqual(seen,[{path:"/mcp",method:"server/discover"}],"Authentication/redirect failure retried or changed endpoint");
  }
  for(const kind of ["needs_input","tool_failure","invalid_output"]) {
    const fixture={server:createServer(async(req,res)=>{
      let text="";for await(const chunk of req)text+=chunk;
      const rpc=JSON.parse(text);
      const result=rpc.method==="server/discover"?{supportedVersions:["2026-07-28"],capabilities:{tools:{}}}
        :rpc.method==="tools/list"?{tools:[echo]}
          :kind==="needs_input"?{resultType:"input_required",requestState:"fixture pending state"}
            :kind==="tool_failure"?{isError:true,content:[{type:"text",text:"fixture tool failure"}]}
              :{content:[],structuredContent:{message:3}};
      res.writeHead(200,{"content-type":"application/json"}).end(JSON.stringify({jsonrpc:"2.0",id:rpc.id,result}));
    })};
    const endpoint=await listen(fixture);
    const result=await execute(cli,["--url",endpoint,"--output","json","echo","--message","wasi-outcome"]);
    assert.equal(result.signal,null);assert.equal(result.code,kind==="needs_input"?7:1,result.stderr);assert.equal(result.stderr,"");
    const outcome=JSON.parse(result.stdout);
    assert.equal(outcome.kind,kind==="needs_input"?"needs_input":"completed");
    if(kind==="invalid_output")assert.equal(outcome.value.output_validation,"invalid");
    if(kind==="tool_failure")assert.equal(outcome.value.is_error,true);
  }
  // This server never sends a response body. A guest timer must interrupt the
  // poll wait itself, then close the stream and send only the core cancel frame.
  for (const legacy of [false,true]) for(const headers of [false,true]) {
    let stalled=0,closed=0,cancels=0;
    const fixture={server:createServer(async(req,res)=>{
      let text="";for await(const chunk of req)text+=chunk;
      const rpc=JSON.parse(text);
      if (req.method==="DELETE") return res.writeHead(204).end();
      if (rpc.method==="notifications/cancelled") {cancels++;return res.writeHead(202).end();}
      if (rpc.method==="notifications/initialized") return res.writeHead(202).end();
      const result=rpc.method==="initialize"?{protocolVersion:"2025-11-25",capabilities:{tools:{}}}
        :rpc.method==="server/discover"?{supportedVersions:["2026-07-28"],capabilities:{tools:{}}}
          :{tools:[echo]};
      if (rpc.method==="tools/call") {stalled++;res.on("close",()=>closed++);if(headers) {res.writeHead(200,{"content-type":"text/event-stream"});res.flushHeaders();}return;}
      res.writeHead(200,{"content-type":"application/json"}).end(JSON.stringify({jsonrpc:"2.0",id:rpc.id,result}));
    })};
    const endpoint=await listen(fixture);
    const result=await execute(cli,["--url",endpoint,"--protocol",legacy?"2025-11-25":"2026-07-28","--timeout","1","echo","--message","wasi-stall"]);
    const error = failed(result,6,"Timeout");
    assert.equal(error.execution_certainty,"unknown");assert.equal(error.retry_advice,"inspect_execution");assert.ok(Number.isSafeInteger(error.operation_id));
    assert.equal(stalled,1);assert.equal(closed,1);assert.equal(cancels,legacy?1:0);
  }
  console.log(`PASS WASI poll deadlines: stalled streams closed, legacy cancellation only, no replay; ${children} guest processes exited`);
} finally {
  for(const fixture of fixtures) {fixture.server.closeAllConnections();await new Promise(r=>fixture.server.close(r));}
  await rm(directory,{recursive:true,force:true});
}
