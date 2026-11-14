import assert from "node:assert/strict";
import {componentRuntime} from "../web/component.js";
import {WorkerRemote} from "../web/worker.js";
import {httpSessionRecoveryPolicy} from "../web/session-recovery.js";
const max=(1n<<64n)-1n;
let state={expiredAtMs:41n,lastToolResult:max};
const runtime=componentRuntime({httpSessionStatus:()=>state});
assert.deepEqual(JSON.parse(runtime.http_session_status()),{expired_at_ms:41,last_tool_result:String(max)});
assert.equal(runtime.http_session_status(),runtime.http_session_status(),"Status must not consume evidence");
state={expiredAtMs:9007199254740992n,lastToolResult:undefined};assert.throws(()=>runtime.http_session_status(),e=>e.code==="LimitExceeded");
assert.equal(httpSessionRecoveryPolicy(undefined),undefined);
for(const value of [false,{},[],{maxAttempts:0,backoff:0,timeout:1},{maxAttempts:9,backoff:0,timeout:1},{maxAttempts:1,backoff:60001,timeout:1},{maxAttempts:1,backoff:0,timeout:600001},{maxAttempts:1,backoff:0,timeout:0},{maxAttempts:1,backoff:0,timeout:1,retryTools:true}])assert.throws(()=>httpSessionRecoveryPolicy(value));
function owner(enabled=true) {
 const remote=Object.create(WorkerRemote.prototype),binding={stale:false};let error;
 Object.assign(remote,{pending:new Map(),transportEpoch:0,httpSessionRecovery:enabled,bindings:new Set([binding]),continuations:new WeakSet(),fail:e=>{error=e;}});
 return {remote,binding,error:()=>error};
}
{
 const o=owner();o.remote.receive({v:1,kind:"transport_reset",id:null,epoch:1,automatic:true});
 assert.equal(o.remote.transportEpoch,1);assert.equal(o.binding.stale,true);assert.equal(o.remote.bindings.size,0);assert.equal(o.error(),undefined);
 o.remote.receive({v:1,kind:"transport_reset",id:null,epoch:3,automatic:true});assert.equal(o.error()?.code,"WorkerProtocolError");
}
{
 const o=owner(false);o.remote.receive({v:1,kind:"transport_reset",id:null,epoch:1,automatic:true});assert.equal(o.error()?.code,"WorkerProtocolError");
}
{
 const o=owner(false),job={method:"reconnect"};o.remote.pending.set(17,job);o.remote.receive({v:1,kind:"transport_reset",id:17,epoch:1,automatic:false});assert.equal(job.reset,true);assert.equal(o.error(),undefined);
 o.remote.receive({v:1,kind:"transport_reset",id:17,epoch:2,automatic:false});assert.equal(o.error()?.code,"WorkerProtocolError");
}
console.log("PASS HTTP session boundary: exact uint64 receipt evidence, safe clock, opt-in bounded policy and independent validated Worker epochs");
