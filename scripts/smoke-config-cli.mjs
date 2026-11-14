import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp,mkdir,writeFile,rm } from "node:fs/promises";
import { join,resolve } from "node:path";
import { tmpdir } from "node:os";
import { createMockServer,echo } from "./mock-server.mjs";
import { referencedTool,unsupportedTool,schemaResources,regexResources,outputRegexResources,COSTLY_PATTERN,COSTLY_VALUE } from "./schema-fixture.mjs";

const selected=process.argv[2]??"both";
assert.ok(["native","wasi","both"].includes(selected));
const kinds=selected==="both"?["native","wasi"]:[selected];
const root=await mkdtemp(join(tmpdir(),"mcpx-named-config-"));
const managementNames=["tools","complete","call","servers","completions","__complete"];
const tools=[echo,referencedTool,unsupportedTool,...managementNames.map(name=>({...echo,name}))];
const modern=createMockServer(undefined,{tools});
const legacy=createMockServer(undefined,{tools,legacy:true,sessions:true});
const credentialName="MCPX_CONFIG_TEST_CREDENTIAL",overrideName="MCPX_CONFIG_TEST_OVERRIDE";
const secret="fixture-only-config-token",override="fixture-only-override-token";
let processes=0;
function launch(program,args,options) {
  processes++;
  return new Promise((resolve,reject)=>{
    const child=spawn(program,args,{...options,stdio:["pipe","pipe","pipe"]});
    let stdout="",stderr="";
    const timer=setTimeout(()=>{child.kill("SIGKILL");reject(new Error("Configuration CLI process did not exit"));},20000);
    child.on("error",error=>{clearTimeout(timer);reject(error);});
    child.stdout.on("data",chunk=>{stdout+=chunk;if(stdout.length>4*1024*1024)child.kill("SIGKILL");});
    child.stderr.on("data",chunk=>{stderr+=chunk;if(stderr.length>1048576)child.kill("SIGKILL");});
    child.stdin.on("error",()=>{});child.stdin.end();
    child.on("close",(code,signal)=>{clearTimeout(timer);resolve({code,signal,stdout,stderr});});
  });
}
function ok(result) {assert.equal(result.signal,null,result.stderr);assert.equal(result.code,0,result.stderr);assert.equal(result.stderr,"");return result.stdout;}
function fail(result,code,name) {assert.equal(result.signal,null,result.stderr);assert.equal(result.code,code,result.stderr);assert.equal(result.stdout,"");const error=JSON.parse(result.stderr);assert.equal(error.code,name);assert.ok(!result.stderr.includes(secret)&&!result.stderr.includes(override));return error;}
function message(text) {return JSON.parse(text).value.raw.structuredContent.message;}
try {
  for(const fixture of [modern,legacy])await new Promise(r=>fixture.server.listen(0,"127.0.0.1",r));
  const endpoint=`http://127.0.0.1:${modern.server.address().port}/mcp`;
  const oldEndpoint=`http://127.0.0.1:${legacy.server.address().port}/mcp`;
  const config=`schema_version=1
[servers.demo]
transport="http"
url=${JSON.stringify(endpoint)}
[servers.old]
transport="http"
url=${JSON.stringify(oldEndpoint)}
protocol="2025-11-25"
[servers.forced]
transport="http"
url=${JSON.stringify(endpoint)}
protocol="2025-11-25"
[servers.secure]
transport="http"
url=${JSON.stringify(oldEndpoint)}
protocol="2025-11-25"
credential="env:${credentialName}"
[servers.external]
transport="http"
url=${JSON.stringify(endpoint)}
credential="account:embedding-reference"
[servers.emptyref]
transport="http"
url=${JSON.stringify(endpoint)}
credential=""
[servers.unused]
transport="http"
url="http://127.0.0.1:9/mcp"
[servers.local]
transport="stdio"
program="THIS-PROGRAM-MUST-NOT-RUN"
args=["do-not-print-argument"]
working_directory="."
env={ACCESS_TOKEN="do-not-print-environment"}
`;
  await writeFile(join(root,"mcpx.toml"),config);
  await writeFile(join(root,"args.json"),'{"message":"named-argument-file"}');
  await writeFile(join(root,"bad-args.json"),'{"message":');
  await writeFile(join(root,"schemas.json"),JSON.stringify(schemaResources().map(r=>({uri:r.uri,schema_json:r.schemaJson}))));
  await writeFile(join(root,"regex-schemas.json"),JSON.stringify(regexResources({pattern:COSTLY_PATTERN}).map(r=>({uri:r.uri,schema_json:r.schemaJson}))));
  await writeFile(join(root,"regex-output-schemas.json"),JSON.stringify(outputRegexResources().map(r=>({uri:r.uri,schema_json:r.schemaJson}))));
  const invalid=[
    ["version.toml","schema_version=2","ConfigurationError"],
    ["field.toml",'[servers.demo]\ntransport="http"\nurl="secret-url"\nprotcol="auto"',"ConfigurationError"],
    ["alias.toml",'[servers.call]\ntransport="http"\nurl="http://localhost"',"InvalidAlias"],
    ["utf8.toml",Buffer.from([0xff]),"ConfigurationError"],
  ];
  for(const [name,text]of invalid)await writeFile(join(root,name),text);
  await writeFile(join(root,"endpoint.toml"),'[servers.bad]\ntransport="http"\nurl="not-an-endpoint"');
  for(const kind of kinds) {
    const path=name=>kind==="native"?join(root,name):`/data/${name}`;
    const execute=(args,{env={},grant=true,cwd=false}={})=>{
      const cleanEnv=Object.fromEntries(Object.entries(process.env).filter(([k])=>![credentialName,overrideName].includes(k)));
      if(kind==="native")return launch(resolve("target/debug/mcpx"),args,{env:{...cleanEnv,...env},cwd:typeof cwd === "string" ? cwd : cwd?root:process.cwd()});
      const flags=["run","-S","http","-S","cli-exit-with-code"];
      if(grant)flags.push("--dir",`${root}::/data`);
      if(cwd)flags.push("-S","cwd=/data");
      for(const [name,value]of Object.entries(env))flags.push("--env",`${name}=${value}`);
      return launch(process.env.WASMTIME??"target/tools/wasmtime/wasmtime",[...flags,"target/wasm32-wasip2/debug/mcpx.wasm",...args],{env:cleanEnv});
    };
    const base=["--config",path("mcpx.toml"),"--output","json"];
    const requests=()=>modern.requests.length+legacy.requests.length;
    let before=requests();
    const listing=ok(await execute([...base,"servers","list"]));
    assert.equal(JSON.parse(listing).length,8);
    for(const value of ["do-not-print-argument","do-not-print-environment",credentialName,"embedding-reference"])assert.ok(!listing.includes(value));
    assert.match(ok(await execute(["--config","/not-readable/config.toml","--help"])),/mcpx/);
    assert.match(ok(await execute(["--config","/not-readable/config.toml","complete","--help"])),/--cursor/);
    for(const shell of ["bash","zsh","fish"]) {
      assert.match(ok(await execute(["--config","/not-readable/config.toml","completions",shell],{grant:false})),/__complete/);
    }
    const shellLine=`mcpx --config ${JSON.stringify(path("mcpx.toml"))}`;
    assert.equal(ok(await execute(["__complete","fish","--line",`${shellLine} de`])),"demo\n");
    assert.equal(ok(await execute(["__complete","fish","--line",`${shellLine} demo echo --mode $(not-executed)`])),"");
    fail(await execute([...base,"--token-env",credentialName,"tools","missing"]),3,"RemoteNotFound");
    for(const [name,,code]of invalid)fail(await execute(["--config",path(name),"servers","list"]),3,code);
    for(const cursor of ["4294967296","18446744073709551615","1"]) {
      fail(await execute(["--config","/not-readable/config.toml","complete","missing","--line","雪","--cursor",cursor]),2,"InvalidCursor");
    }
    assert.equal(requests(),before,"Offline config/list/help/lookup sent I/O");
    for(const alias of ["demo","old"]) {
      const businessBefore=modern.requests.concat(legacy.requests).filter(r=>r.method==="tools/call").length;
      assert.equal(ok(await execute(["__complete","fish","--line",`${shellLine} ${alias} echo --mode tw`])),"two words\n");
      assert.equal(modern.requests.concat(legacy.requests).filter(r=>r.method==="tools/call").length,businessBefore,"Shell completion called a tool");
    }
    if(kind==="wasi") {
      before=requests();
      fail(await execute([...base,"servers","list"],{grant:false}),3,"ConfigurationError");
      fail(await execute([...base,"--token-env",credentialName,"--schema-registry","/not-readable/schemas","call","local","echo","--args-file","/not-readable/input"]),3,"CapabilityUnavailable");
      assert.equal(requests(),before);
    }
    for(const alias of ["demo","old"]) {
      const count=(alias==="demo"?modern:legacy).requests.filter(r=>r.method==="tools/call").length;
      assert.ok(JSON.parse(ok(await execute([...base,"tools",alias]))).some(t=>t.name==="echo"));
      assert.match(ok(await execute([...base,alias,"echo","--help"])),/--message/);
      for (const flag of ["-h", "--help"]) {
        const help=ok(await execute([...base,alias,flag]));
        assert.match(help,/Commands:/);
        assert.match(help,/echo/);
        assert.ok(help.includes(`Usage: mcpx ${alias}`));
      }
      assert.equal(JSON.parse(ok(await execute([...base,"complete",alias,"--line","echo --mode tw"]))).candidates[0].value,"two words");
      assert.equal(JSON.parse(ok(await execute([...base,"complete",alias,"--line","--args-file"]))).candidates.length,0);
      fail(await execute([...base,alias,"echo","--args",'{"message":3}']),2,"ValidationFailed");
      assert.equal((alias==="demo"?modern:legacy).requests.filter(r=>r.method==="tools/call").length,count);
      assert.equal(message(ok(await execute([...base,alias,"echo","--message",`named-${alias}`]))),`named-${alias}`);
      assert.equal(message(ok(await execute([...base,"call",alias,"echo","--args-file",path("args.json")]))),"named-argument-file");
    }
    for(const name of managementNames)assert.equal(message(ok(await execute([...base,"call","demo",name,"--message",name]))),name);
    for(const overrideProtocol of [null,"2026-07-28","auto"]) {
      const before=modern.requests.length;
      const flags=overrideProtocol?["--protocol",overrideProtocol]:[];
      ok(await execute([...base,...flags,"forced","echo","--message","configured-protocol"]));
      assert.equal(modern.requests[before].method,overrideProtocol?"server/discover":"initialize");
    }
    const transportStart=legacy.transportRequests.length;
    const env={[credentialName]:secret};
    assert.equal(message(ok(await execute([...base,"secure","echo","--message","named-credential"],{env}))),"named-credential");
    const credentialRequests=legacy.transportRequests.slice(transportStart);
    assert.ok(credentialRequests.some(r=>r.method==="DELETE"));
    assert.ok(credentialRequests.every(r=>r.auth===`Bearer ${secret}`),"Configured credential/session snapshot was lost");
    before=requests();
    fail(await execute([...base,"secure","echo","--message","missing-credential"]),4,"CredentialUnavailable");
    fail(await execute([...base,"secure","echo","--args-file",path("bad-args.json")]),2,"InvalidJson");
    fail(await execute([...base,"external","echo","--message","unsupported-reference"]),4,"CredentialUnavailable");
    fail(await execute([...base,"--token-env",overrideName,"emptyref","echo","--message","empty-reference"],{env:{[overrideName]:override}}),3,"ConfigurationError");
    assert.equal(requests(),before,"Missing/unsupported credentials sent I/O");
    assert.equal(message(ok(await execute([...base,"--token-env",overrideName,"external","echo","--message","explicit-override"],{env:{[overrideName]:override}}))),"explicit-override");
    assert.equal(modern.transportRequests.at(-1).auth,`Bearer ${override}`);
    const refs=JSON.parse(ok(await execute([...base,"--schema-registry",path("schemas.json"),"demo","ref_echo","--args",'{"message":"named-refs"}'])));
    assert.equal(refs.value.output_validation,"valid");
    const callsBeforeLimit = modern.requests.filter(r=>r.method==="tools/call").length;
    const limited = fail(await execute([...base,"--schema-registry",path("regex-schemas.json"),"demo","ref_echo","--args",JSON.stringify({message:COSTLY_VALUE})]),2,"ValidationLimitExceeded");
    assert.equal(limited.execution_certainty,"not_sent");
    assert.equal(modern.requests.filter(r=>r.method==="tools/call").length,callsBeforeLimit,"Regex exhaustion dispatched a business call");
    const unfinished = await execute([...base,"--schema-registry",path("regex-output-schemas.json"),"demo","ref_echo","--args",JSON.stringify({message:COSTLY_VALUE})]);
    assert.equal(unfinished.code,1,unfinished.stderr);
    assert.equal(unfinished.stderr,"");
    const rawUnfinished = JSON.parse(unfinished.stdout).value;
    assert.equal(rawUnfinished.output_validation,"indeterminate");
    assert.equal(rawUnfinished.raw.structuredContent.message,COSTLY_VALUE);
    assert.equal(message(ok(await execute(["--output","json","call","demo","echo","--message","default-config"],{cwd:true}))),"default-config");
    assert.equal(message(ok(await execute(["--output","json","call","demo","echo","--args-file","args.json"],{cwd:true}))),"named-argument-file");
    assert.equal(JSON.parse(ok(await execute(["--output","json","--schema-registry","schemas.json","call","demo","ref_echo","--args",'{"message":"relative-schema"}'],{cwd:true}))).value.output_validation,"valid");
    if (kind === "native") {
      const nested=join(root,"nested","child");
      await mkdir(nested,{recursive:true});
      assert.equal(message(ok(await execute(["--output","json","demo","echo","--message","ancestor-config"],{cwd:nested}))),"ancestor-config");
      assert.match(ok(await execute(["servers","list"],{cwd:nested})),/demo/);
      await writeFile(join(root,"nested","mcpx.toml"),'[servers.nearest]\ntransport="http"\nurl="http://127.0.0.1:9/mcp"');
      const nearest=ok(await execute(["servers","list"],{cwd:nested}));
      assert.match(nearest,/nearest/);
      assert.doesNotMatch(nearest,/demo/);
      assert.match(ok(await execute(["--config",join(root,"mcpx.toml"),"servers","list"],{cwd:nested})),/demo/);
      await writeFile(join(root,"nested","mcpx.toml"),'invalid toml');
      fail(await execute(["servers","list"],{cwd:nested}),3,"ConfigurationError");
    }
    for (const args of [["--not-an-option"]]) {
      const result=await execute(args);
      assert.equal(result.code,2);
      assert.match(result.stderr,/Usage: mcpx/);
      assert.match(result.stderr,/Commands:/);
      assert.doesNotMatch(result.stderr,/"execution_certainty"/);
    }
    for (const command of ["servers","auth"]) {
      assert.match(ok(await execute([command])),/Commands:/);
    }
    assert.match(ok(await execute([])),/Commands:/);
    before=requests();
    const invalidTools=await execute(["--url",endpoint,"--output","json","tools","--args-file",path("args.json")]);
    assert.equal(invalidTools.code,2);
    assert.match(invalidTools.stderr,/Usage: mcpx/);
    fail(await execute(["--config",path("endpoint.toml"),"bad","echo","--message","bad-url"]),3,"InvalidEndpoint");
    assert.equal(requests(),before);
    console.log(`PASS ${kind} named configuration: offline list/help, shared strict schema, routes/collisions, completion, protocol overrides, files/default cwd, credentials/grants, session cleanup and no rejected business dispatch`);
  }
  console.log(`PASS configuration CLI matrix: ${processes} processes exited`);
} finally {
  for(const fixture of [modern,legacy]){fixture.server.closeAllConnections();await new Promise(r=>fixture.server.close(r));}
  await rm(root,{recursive:true,force:true});
}
