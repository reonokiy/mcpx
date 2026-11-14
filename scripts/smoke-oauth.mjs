import assert from "node:assert/strict";
import {createServer,request} from "node:http";
import {spawn,execFile} from "node:child_process";
import {promisify} from "node:util";
import {mkdtemp,readFile,writeFile,readdir,stat,rm} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {createHash} from "node:crypto";
import {createMockServer} from "./mock-server.mjs";
const exec=promisify(execFile);
const cli=process.env.MCPX_CLI??(process.platform==="win32"?"target/debug/mcpx.exe":"target/debug/mcpx");
for(const mode of ["registered","dcr","cimd","well-known","oidc","legacy"]){
 const directory=await mkdtemp(join(tmpdir(),"mcpx-oauth-"));const env={...process.env,MCPX_AUTH_DIR:directory};
 const backend=createMockServer(undefined,{legacy:mode==="legacy",sessions:mode==="legacy"});await new Promise(r=>backend.server.listen(0,"127.0.0.1",r));
 let origin,resource,failure,authorizations=0,exchanges=0,refreshes=0,registrations=0,revocations=0,access="access-0",refresh="refresh-0";
 const codes=new Map(),seen=[];
 const server=createServer(async(req,res)=>{
  try{
   const url=new URL(req.url,origin);let body="";for await(const b of req)body+=b;
   const send=(value,status=200)=>res.writeHead(status,{"content-type":"application/json"}).end(JSON.stringify(value));
   if(url.pathname==="/mcp"){
    if(req.headers.authorization!==`Bearer ${access}`){res.setHeader("WWW-Authenticate",`Bearer ${mode==="well-known"?'':`resource_metadata="${origin}/.well-known/oauth-protected-resource/mcp", `}scope="tools.read"`);return res.writeHead(401).end();}
    const rpc=body?JSON.parse(body):{};seen.push({method:rpc.method,token:req.headers.authorization});
    const proxy=request(`http://127.0.0.1:${backend.server.address().port}/mcp`,{method:req.method,headers:{...req.headers,host:`127.0.0.1:${backend.server.address().port}`}},response=>{res.writeHead(response.statusCode,response.headers);response.pipe(res);});proxy.on("error",e=>res.destroy(e));proxy.end(body);return;
   }
   if(url.pathname==="/.well-known/oauth-protected-resource/mcp")return send({resource,authorization_servers:[origin],scopes_supported:["tools.read"]});
   if(url.pathname==="/.well-known/oauth-authorization-server"&&mode==="oidc")return res.writeHead(404).end();
   if(["/.well-known/oauth-authorization-server","/.well-known/openid-configuration"].includes(url.pathname))return send({issuer:origin,authorization_endpoint:`${origin}/authorize`,token_endpoint:`${origin}/token`,registration_endpoint:`${origin}/register`,revocation_endpoint:`${origin}/revoke`,code_challenge_methods_supported:["S256"],authorization_response_iss_parameter_supported:true,client_id_metadata_document_supported:mode==="cimd"});
   if(url.pathname==="/register"){
    registrations++;const value=JSON.parse(body);assert.equal(value.token_endpoint_auth_method,"none");assert.equal(value.application_type,"native");assert.equal(new URL(value.redirect_uris[0]).hostname,"127.0.0.1");return send({client_id:"dynamic-mcpx",token_endpoint_auth_method:"none"},201);
   }
   if(url.pathname==="/authorize"){
    authorizations++;assert.equal(url.searchParams.get("resource"),resource);assert.equal(url.searchParams.get("code_challenge_method"),"S256");assert.equal(url.searchParams.get("scope"),"tools.read");
    if(mode==="cimd")assert.equal(url.searchParams.get("client_id"),"https://reonokiy.github.io/mcpx/oauth/native.json");
    const code=`code-${authorizations}`;codes.set(code,new URL(url));const callback=new URL(url.searchParams.get("redirect_uri"));callback.searchParams.set("code",code);callback.searchParams.set("state",url.searchParams.get("state"));callback.searchParams.set("iss",origin);res.writeHead(302,{location:String(callback)}).end();return;
   }
   if(url.pathname==="/token"){
    assert.equal(req.headers.authorization,undefined);const form=new URLSearchParams(body);assert.equal(form.get("resource"),resource);
    if(form.get("grant_type")==="authorization_code"){
     exchanges++;const auth=codes.get(form.get("code"));assert.ok(auth);codes.delete(form.get("code"));
     assert.equal(createHash("sha256").update(form.get("code_verifier")).digest("base64url"),auth.searchParams.get("code_challenge"));assert.equal(form.get("client_id"),auth.searchParams.get("client_id"));assert.equal(form.get("redirect_uri"),auth.searchParams.get("redirect_uri"));
    }else{
     assert.equal(form.get("grant_type"),"refresh_token");assert.equal(form.get("refresh_token"),refresh);refreshes++;await new Promise(r=>setTimeout(r,80));access=`access-${refreshes}`;refresh=`refresh-${refreshes}`;
    }
    return send({access_token:access,refresh_token:refresh,token_type:"Bearer",expires_in:3600,scope:"tools.read"});
   }
   if(url.pathname==="/revoke"){const form=new URLSearchParams(body);assert.equal(form.get("token"),refresh);revocations++;return send({});}
   res.writeHead(404).end();
  }catch(e){failure??=e;res.writeHead(500).end();}
 });
 await new Promise(r=>server.listen(0,"127.0.0.1",r));origin=`http://127.0.0.1:${server.address().port}`;resource=`${origin}/mcp`;
 const config=join(directory,"mcpx.toml");await writeFile(config,`schema_version=1\n${["default","another-user"].map(profile=>`[profiles.${profile}.servers.demo]\ntransport="http"\nurl="${resource}"\ncredential="oauth"\nprotocol="${mode==="legacy"?"2025-11-25":"2026-07-28"}"\n`).join("")}`);
 const base=["--config",config];const run=(...args)=>exec(cli,[...base,...args],{env,timeout:15000});
 try{
  await assert.rejects(run("demo","echo","--message","before-login"),e=>e.code===4&&e.stderr.includes("LoginRequired"));assert.equal(seen.length,0);
  const args=[...base,"auth","login","demo","--no-browser","--store","file"];if(mode==="registered")args.push("--client-id","registered-mcpx");if(mode!=="cimd")args.push("--redirect-port","0");
  const child=spawn(cli,args,{env,stdio:["ignore","pipe","pipe"]});let stdout="",stderr="",authorize,settled=false;
  const ready=new Promise(resolve=>authorize=resolve);child.stdout.on("data",b=>stdout+=b);child.stderr.on("data",b=>{stderr+=b;const found=stderr.match(/https?:\/\/[^\s]+\/authorize\?[^\s]+/);if(found)authorize(found[0]);});
  const done=new Promise((resolve,reject)=>{child.once("error",reject);child.once("exit",code=>{settled=true;resolve(code);});});
  const timer=setTimeout(()=>child.kill(),15000);
  const url=await Promise.race([ready,done.then(code=>{throw new Error(`Login exited ${code}: ${stderr}`);})]);
  const stale = new URL(new URL(url).searchParams.get("redirect_uri"));
  stale.searchParams.set("code","stale-code");
  stale.searchParams.set("state","previous-login-state");
  for (const state of ["previous-login-state", ""]) {
    stale.searchParams.set("state",state);
    const rejected=await fetch(stale);
    assert.equal(rejected.status,400);
    assert.match(await rejected.text(),/latest authorization URL/);
    assert.equal(exchanges,0,"An unrelated callback must never exchange tokens");
    assert.equal(settled,false,"An unrelated callback must not terminate login");
  }
  const result=await fetch(url);assert.equal(result.status,200);assert.match(await result.text(),/Return to your terminal/);assert.equal(await done,0,stderr);clearTimeout(timer);assert.ok(settled);assert.match(stdout,/Logged in/);
  assert.equal(authorizations,1);assert.equal(exchanges,1);assert.equal(registrations,mode==="registered"||mode==="cimd"?0:1);
  const tokens=(await readdir(directory)).find(name=>name.endsWith(".tokens.json"));assert.ok(tokens);const path=join(directory,tokens);const initial=JSON.parse(await readFile(path));
  if(process.platform!=="win32")assert.equal((await stat(path)).mode&0o777,0o600);
  assert.ok((await run("demo","echo","--message","logged-in")).stdout.includes("logged-in"));assert.equal(refreshes,0);
  initial.tokens.expires_at=1;await writeFile(path,JSON.stringify(initial));
  const replies=await Promise.all([run("demo","echo","--message","refresh-one"),run("demo","echo","--message","refresh-two")]);assert.ok(replies[0].stdout.includes("refresh-one"));assert.ok(replies[1].stdout.includes("refresh-two"));
  assert.equal(refreshes,1,"Concurrent CLI processes must share one rotating refresh");const renewed=JSON.parse(await readFile(path));assert.equal(renewed.context_id,initial.context_id);assert.equal(renewed.tokens.refresh_token,"refresh-1");assert.ok(seen.filter(r=>r.method==="tools/call").slice(-2).every(r=>r.token==="Bearer access-1"));
  const status=await exec(cli,["--config",config,"--output","json","auth","status","demo"],{env});assert.equal(JSON.parse(status.stdout).logged_in,true);assert.ok(!status.stdout.includes("access-1")&&!status.stdout.includes("refresh-1"));
  await run("profiles","use","another-user");
  const savedStatus=await run("--output","json","auth","status","demo");
  assert.equal(JSON.parse(savedStatus.stdout).logged_in,false,"Saved default must scope OAuth status");
  await run("profiles","use","default");
  const other=["--config",config,"--profile","another-user"];
  const otherStatus=await exec(cli,[...other,"--output","json","auth","status","demo"],{env});assert.equal(JSON.parse(otherStatus.stdout).logged_in,false);
  await assert.rejects(exec(cli,[...other,"demo","echo","--message","wrong-user"],{env}),e=>e.code===4&&e.stderr.includes("LoginRequired"));
  await exec(cli,[...other,"auth","logout","demo"],{env});assert.equal(revocations,0,"Logging out another user must not revoke this profile");
  await run("auth","logout","demo");assert.equal(revocations,1);assert.equal((await readdir(directory)).filter(n=>n.endsWith("tokens.json")).length,0);
  await assert.rejects(run("demo","echo","--message","after-logout"),e=>e.code===4&&e.stderr.includes("LoginRequired"));assert.equal(seen.filter(r=>r.method==="tools/call").length,3);assert.ifError(failure);
  console.log(`PASS native OAuth ${mode}: PKCE login, resource/issuer binding, persistence, one rotating refresh across processes, stable context, revocation/logout and no replay`);
 }finally{server.closeAllConnections?.();backend.server.closeAllConnections?.();await Promise.all([new Promise(r=>server.close(r)),new Promise(r=>backend.server.close(r))]);await rm(directory,{recursive:true,force:true});}
}
