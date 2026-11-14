import assert from "node:assert/strict";
import {WorkerFetchHost} from "../web/worker-fetch-host.js";

const wait=async predicate=>{for(let n=0;n<100;n++){if(predicate())return;await new Promise(resolve=>setTimeout(resolve,1));}throw new Error("I/O did not settle");};
async function exercise({streaming=false,method="GET",headers={"mcp-protocol-version":"2025-11-25"},chunkSize=65536,chunks=20,expectError=false}) {
  const events=[];let fetched=0,cancelled=0;
  const host=new WorkerFetchHost(message=>events.push(message),async()=>{
    fetched++;let remaining=chunks;
    return new Response(new ReadableStream({pull(controller){if(remaining-->0)controller.enqueue(new Uint8Array(chunkSize));else controller.close();},cancel(){cancelled++;}}));
  });
  host.receive({op:"fetch",id:1,url:"http://fixture/mcp",init:{method,headers,maxResponseBytes:1024*1024,notificationStream:streaming}});
  await wait(()=>events.length);
  let delivered=0;
  while(host.records.size&&events.at(-1).op!=="error") {
    const before=events.length;host.receive({op:"pull",id:1});await wait(()=>events.length>before);if(events.at(-1).op==="chunk")delivered++;
  }
  assert.equal(events.at(-1).op,expectError?"error":"end");
  assert.equal(host.records.size,0);assert.equal(host.buffered,0);
  if(!expectError)assert.equal(delivered,chunks);
  await host.close();return {fetched,cancelled,delivered};
}
assert.equal((await exercise({expectError:true})).delivered,16,"Ordinary responses lost their cumulative cap");
await exercise({streaming:true});
await exercise({streaming:true,method:"POST",headers:{"mcp-protocol-version":"2026-07-28","mcp-method":"subscriptions/listen"}});
assert.equal((await exercise({streaming:true,method:"POST",expectError:true})).fetched,0,"Ordinary POST bypassed the cap");
assert.equal((await exercise({streaming:"true",expectError:true})).fetched,0,"Streaming flag was coerced");
assert.equal((await exercise({streaming:true,chunkSize:8*1024*1024+1,chunks:1,expectError:true})).delivered,0,"Streaming lost its retained-buffer cap");
console.log("PASS Worker fetch: long notification streams, ordinary cumulative cap, retained-buffer cap and strict stream classification");
