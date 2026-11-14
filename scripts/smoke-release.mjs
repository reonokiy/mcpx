import assert from "node:assert/strict";
import {execFile} from "node:child_process";
import {promisify} from "node:util";
import {readFile} from "node:fs/promises";
import {createMockServer} from "./mock-server.mjs";
const exec=promisify(execFile);
const {version:releaseVersion}=JSON.parse(await readFile(new URL("../package.json",import.meta.url)));
const onlyNative=process.argv.includes("--native");
const native=process.platform==="win32"?"target/release/mcpx.exe":"target/release/mcpx";
const targets=[["native",native,[]]];
if(!onlyNative)targets.push(["WASI",process.env.WASMTIME??"target/tools/wasmtime/wasmtime",["run","-S","http","target/wasm32-wasip2/release/mcpx.wasm"]]);
for(const [name,binary,prefix]of targets){
 const version=await exec(binary,[...prefix,"--version"]);assert.equal(version.stdout.trim(),`mcpx ${releaseVersion}`);
 for(const legacy of [false,true]){
  const {server,requests}=createMockServer(undefined,{legacy});await new Promise(resolve=>server.listen(0,"127.0.0.1",resolve));
  try{
   const {stdout,stderr}=await exec(binary,[...prefix,"--url",`http://127.0.0.1:${server.address().port}/mcp`,"--protocol",legacy?"2025-11-25":"2026-07-28","--output","json","echo","--message","release-smoke"],{timeout:15000});
   assert.equal(stderr,"");assert.ok(stdout.includes("release-smoke"));assert.equal(requests.filter(r=>r.method==="tools/call").length,1);
   console.log(`PASS release ${name} ${legacy?"legacy":"modern"}: version and one actual CLI tool call`);
  }finally{server.closeAllConnections?.();await new Promise(resolve=>server.close(resolve));}
 }
}
