import assert from "node:assert/strict";
import {spawn,execFile} from "node:child_process";
import {createServer,request} from "node:http";
import {createServer as createSecureServer} from "node:https";
import {mkdtemp,readFile,rm} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {promisify} from "node:util";
import {wasiTlsFixture} from "./wasi-tls-fixture.mjs";
const wasmtime=process.env.WASMTIME??"target/tools/wasmtime/wasmtime";
const guest="target/wasm32-wasip2/debug/examples/sdk_interop.wasm";
const httpsMode=process.argv[2]==="--https";
const filter=process.argv[httpsMode?3:2];
const httpsHost=process.env.MCPX_WASI_HTTPS_HOST??"target/tools/wasi-https-host/debug/mcpx-wasi-https-host";
let tls;
let guests=0,servers=0;
async function execute(endpoint,protocol,scenario,{ca=tls?.ca,origin=new URL(endpoint).origin}={}) {
  guests++;
  const child=spawn(httpsMode?httpsHost:wasmtime,httpsMode?["--ca-pem",ca,"--allow-origin",origin,guest,endpoint,protocol,scenario]:["run","-S","http",guest,endpoint,protocol,scenario],{stdio:["ignore","pipe","pipe"]});
  let stdout="",stderr="";
  const timeout=httpsMode?60000:20000;
  const timer=setTimeout(()=>child.kill("SIGKILL"),timeout);
  child.stdout.on("data",chunk=>stdout+=chunk);child.stderr.on("data",chunk=>stderr+=chunk);
  const {code,signal}=await new Promise((resolve,reject)=>{child.once("error",reject);child.once("close",(code,signal)=>resolve({code,signal}));}).finally(()=>clearTimeout(timer));
  assert.equal(code,0,`${protocol}/${scenario}${signal?` terminated by ${signal} (timeout ${timeout} ms)`:""}: ${stderr}`);
  assert.equal(JSON.parse(stdout).result,scenario);assert.equal(stderr,"");
}
async function sdk(protocol,format,scenario) {
  const legacy=protocol.endsWith("legacy")||scenario==="modern-reject";
  const daemon=spawn("target/debug/examples/sdk_http_server",[legacy?"legacy":"modern",format,scenario==="cache-zero"?"0":"60000"],{stdio:["pipe","pipe","pipe"]});
  let stdout="",stderr="",ready;
  const readiness=new Promise((resolve,reject)=>{ready=resolve;daemon.once("error",reject);});
  const exit=new Promise(resolve=>daemon.once("exit",resolve));
  daemon.stdout.on("data",chunk=>{stdout+=chunk;if(stdout.includes("\n"))ready(JSON.parse(stdout.trim()));});
  daemon.stderr.on("data",chunk=>stderr+=chunk);
  const startup=setTimeout(()=>daemon.kill("SIGKILL"),10000);
  const setup=await Promise.race([readiness,exit.then(code=>{throw new Error(`SDK daemon exited ${code}: ${stderr}`);})]);clearTimeout(startup);
  const backend=new URL(setup.endpoint),seen=[],mimes=[],upstreams=new Set();let failure;
  const handler=async(req,res)=>{
    try {
      let raw="";for await(const chunk of req)raw+=chunk;
      const rpc=raw?JSON.parse(raw):undefined;
      assert.equal(req.headers.authorization,"Bearer wasi-sdk-fixture");
      if(httpsMode)assert.equal(req.socket.encrypted,true,"SDK request bypassed TLS");
      seen.push({method:rpc?.method??req.method,message:rpc?.params?.arguments?.message,session:req.headers["mcp-session-id"]});
      const upstream=request(backend,{method:req.method,headers:{...req.headers,host:backend.host},agent:false},response=>{
        const info={method:rpc?.method??req.method,message:rpc?.params?.arguments?.message,type:response.headers["content-type"]??"",session:response.headers["mcp-session-id"],cache:response.headers["cache-control"]};mimes.push(info);
        if(["server/discover","tools/list"].includes(rpc?.method)&&mimes.length<4)response.on("data",chunk=>info.body=(info.body??"")+chunk);
        if(rpc?.params?.arguments?.message==="wasi-timeout") {response.resume();return;}
        res.writeHead(response.statusCode,response.headers);response.pipe(res);
      });
      upstreams.add(upstream);upstream.once("close",()=>upstreams.delete(upstream));
      upstream.on("error",error=>{if(!res.destroyed){failure??=error;res.destroy();}});
      res.once("close",()=>upstream.destroy());
      upstream.end(raw);
    }catch(error){failure??=error;res.writeHead(500).end();}
  };
  const proxy=httpsMode?createSecureServer(tls.good,handler):createServer(handler);
  await new Promise(resolve=>proxy.listen(0,"127.0.0.1",resolve));
  const endpoint=`${httpsMode?"https":"http"}://127.0.0.1:${proxy.address().port}/mcp`;
  let metrics;
  try {
    await execute(endpoint,protocol,scenario);
    assert.ifError(failure);
    metrics=await (await fetch(new URL("/stats",backend))).json();
    assert.equal(metrics.active,0);
    if(scenario==="modern-reject") {
      assert.equal(metrics.calls,0);
      assert.deepEqual(seen.map(r=>r.method),["server/discover"],"Modern version rejection downgraded or retried");
    } else {
    assert.equal(metrics.calls,{serial:6,"cache-zero":6,parallel:17,resume:32,timeout:2,manual:1,notifications:2,reconnect:2}[scenario],"SDK business call was lost, duplicated or dispatched after local rejection");
    // Count actual RPCs independently from handler implementation metrics.
    assert.equal(seen.filter(r=>r.method==="tools/list").length,scenario==="cache-zero"?9:["notifications","reconnect"].includes(scenario)?2:1,JSON.stringify(mimes));
    if(scenario==="parallel"){assert.equal(metrics.parallel,16);assert.equal(metrics.maximum,16);}
    if(scenario==="resume"){assert.equal(metrics.resumes,16);assert.equal(metrics.maximum,16);}
    assert.equal(seen.filter(r=>r.method==="tools/call").length,metrics.calls);
    assert.equal(seen.filter(r=>r.method==="notifications/cancelled").length,legacy&&scenario==="timeout"?1:0);
    const deletes=seen.filter(r=>r.method==="DELETE");assert.equal(deletes.length,legacy?(scenario==="reconnect"?2:1):0);
    if(legacy)assert.equal(deletes[0].session,mimes.find(r=>r.method==="initialize").session);
    if(scenario==="reconnect"&&legacy){
      const initialized=mimes.filter(r=>r.method==="initialize").map(r=>r.session);
      assert.equal(new Set(initialized).size,2,"SDK reused an old session after reconnect");
      assert.deepEqual(deletes.map(r=>r.session),initialized);
      assert.ok(seen.findIndex(r=>r.method==="DELETE")<seen.findLastIndex(r=>r.method==="initialize"),"new initialize overtook old session cleanup");
    }
    if(["serial","cache-zero"].includes(scenario)) {
      const terminal=mimes.find(r=>r.message==="wasi-json-terminal");assert.ok(terminal);
      // This SDK keeps legacy session calls on SSE even with JSON preference.
      assert.match(terminal.type,!legacy&&format==="json"?/application\/json/:/text\/event-stream/);
      assert.ok(mimes.some(r=>r.method==="tools/call"&&r.type.startsWith("text/event-stream")),"SDK progress did not exercise SSE fallback");
    }
    }
    console.log(`PASS WASI official SDK${httpsMode?" HTTPS":""}: ${protocol} ${format}-preferred ${scenario}`);
  } finally {
    for(const upstream of upstreams)upstream.destroy();
    proxy.closeAllConnections();await new Promise(resolve=>proxy.close(resolve));
    daemon.stdin.end();
    const stop=setTimeout(()=>daemon.kill("SIGKILL"),5000);
    const code=await exit;clearTimeout(stop);assert.equal(code,0,stderr);assert.equal(stderr,"");servers++;
  }
}
let directory;
try {
  if(httpsMode)tls=await wasiTlsFixture();
  for(const protocol of ["modern","legacy","auto"])for(const format of ["json","sse"]) {
    for(const scenario of ["serial","parallel","manual","timeout","notifications","reconnect"])if(!filter||filter===scenario)await sdk(protocol,format,scenario);
    if(!protocol.endsWith("legacy")&&(!filter||filter==="resume"))await sdk(protocol,format,"resume");
    if(!protocol.endsWith("legacy")&&(!filter||filter==="cache-zero"))await sdk(protocol,format,"cache-zero");
    if(!protocol.endsWith("legacy")&&(!filter||filter==="modern-reject"))await sdk(protocol,format,"modern-reject");
  }
  if(!httpsMode&&(!filter||filter==="tls-reject")) {
    directory=await mkdtemp(join(tmpdir(),"mcpx-wasi-sdk-tls-"));
    const key=join(directory,"key.pem"),cert=join(directory,"cert.pem");
    await promisify(execFile)("openssl",["req","-x509","-newkey","rsa:2048","-nodes","-keyout",key,"-out",cert,"-days","1","-subj","/CN=localhost","-addext","subjectAltName=IP:127.0.0.1,DNS:localhost"]);
    for(const protocol of ["modern","legacy","auto"]) {
      let requests=0;const errors=[];
      const secure=createSecureServer({key:await readFile(key),cert:await readFile(cert)},(req,res)=>{requests++;res.writeHead(500).end();});
      secure.on("tlsClientError",error=>errors.push(error));
      await new Promise(resolve=>secure.listen(0,"127.0.0.1",resolve));
      try {
        await execute(`https://127.0.0.1:${secure.address().port}/mcp`,protocol,"tls-reject");
        assert.equal(requests,0,"Untrusted TLS certificate reached the application");
        assert.equal(errors.length,1,"TLS failure was retried or silently downgraded");
        assert.match(errors[0].message,/unknown ca|certificate unknown/i,"TLS did not fail because of certificate trust");
      } finally {secure.closeAllConnections();await new Promise(resolve=>secure.close(resolve));}
      console.log(`PASS WASI untrusted TLS rejected: ${protocol}`);
    }
  }
  if(httpsMode&&(!filter||filter==="tls-reject")) {
    for(const kind of ["ca","hostname","expired","origin","cleartext","redirect"])for(const protocol of ["modern","legacy","auto"]) {
      let requests=0,redirects=0;const errors=[];
      const sink=createServer((req,res)=>{redirects++;res.writeHead(500).end();});
      await new Promise(resolve=>sink.listen(0,"127.0.0.1",resolve));
      const handler=(req,res)=>{requests++;res.writeHead(kind==="redirect"?307:500,{location:`http://127.0.0.1:${sink.address().port}/target`}).end();};
      const server=kind==="cleartext"?createServer(handler):createSecureServer(tls[["hostname","expired"].includes(kind)?kind:"good"],handler);
      server.on("tlsClientError",error=>errors.push(error));
      await new Promise(resolve=>server.listen(0,"127.0.0.1",resolve));
      const origin=`https://127.0.0.1:${server.address().port}`;
      try {
        await execute(`${kind==="cleartext"?origin.replace("https:","http:"):origin}/mcp`,protocol,"tls-reject",{ca:kind==="ca"?tls.otherCa:tls.ca,origin:kind==="origin"?"https://127.0.0.1:1":origin});
        assert.equal(requests,kind==="redirect"?1:0,`${kind} reached an HTTP handler`);
        assert.equal(errors.length,["ca","hostname","expired"].includes(kind)?1:0,`${kind} unexpectedly retried TLS`);
        if(kind==="ca")assert.match(errors[0].message,/unknown ca|certificate unknown/i);
        if(kind==="expired")assert.match(errors[0].message,/certificate expired/i);
        assert.equal(redirects,0,"HTTPS redirect was followed to cleartext");
      } finally {
        server.closeAllConnections();sink.closeAllConnections();
        await Promise.all([new Promise(resolve=>server.close(resolve)),new Promise(resolve=>sink.close(resolve))]);
      }
      console.log(`PASS WASI HTTPS policy: ${protocol} ${kind}`);
    }
  }
  assert.ok(guests>0,"Unknown scenario filter");
  console.log(`PASS ${guests} ${httpsMode?"embedded HTTPS":"Wasmtime"} guests and ${servers} official SDK servers: JSON/SSE, dependent workflows, actual 16-call/resume barriers, notifications, timeout evidence and ${httpsMode?"configured trust/host policy":"untrusted TLS rejection"}`);
} finally {if(directory)await rm(directory,{recursive:true,force:true});await tls?.close();}
