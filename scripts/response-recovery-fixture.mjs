import assert from "node:assert/strict";
import {createServer} from "node:http";
import {readFile} from "node:fs/promises";

export function responseRecoveryFixture(serverFactory=createServer) {
  const records=new Map(),streams=new Set(),sessions=new Set();let error,preflights=0;
  const frame=value=>`data: ${JSON.stringify(value)}\n\n`;
  const server=serverFactory(async(req,res)=>{
    try {
      const url=new URL(req.url,"http://localhost"),q=url.searchParams,id=q.get("case"),legacy=q.get("legacy")!=="0";
      res.setHeader("access-control-allow-origin","*");res.setHeader("access-control-allow-headers","authorization,content-type,mcp-protocol-version,mcp-session-id,mcp-method,mcp-name,last-event-id");res.setHeader("access-control-expose-headers","mcp-session-id");res.setHeader("access-control-allow-methods","GET,POST,DELETE,OPTIONS");
      if(req.method==="OPTIONS"){preflights++;return res.writeHead(204).end();}
      if(url.pathname==="/")return res.writeHead(200,{"content-type":"text/html"}).end("<!doctype html><title>mcpx response recovery</title>");
      const file=/^\/(web\/[a-z-]+\.js|dist\/web\/mcpx_web(?:_bg\.wasm|\.js)|scripts\/response-recovery-host\.mjs)$/.exec(url.pathname)?.[1];
      if(file)return res.writeHead(200,{"content-type":file.endsWith(".wasm")?"application/wasm":"text/javascript"}).end(await readFile(file));
      if(!["/mcp","/stats","/release","/emit"].includes(url.pathname))return res.writeHead(404).end();
      if(!records.has(id))records.set(id,{calls:[],gets:[],replies:[],acks:[],cancelled:[],requests:[],initialized:0,deleted:0,opened:0,closed:0,listens:0,pending:[],getCount:{}});
      const record=records.get(id);
      const stats=()=>{const {pending,emit,...safe}=record;return {...safe,held:pending.length};};
      if(url.pathname==="/stats")return res.writeHead(200,{"content-type":"application/json"}).end(JSON.stringify(stats()));
      if(url.pathname==="/release"){for(const send of record.pending.splice(0))send();return res.writeHead(204).end();}
      if(url.pathname==="/emit"){assert.ok(record.emit);record.emit();return res.writeHead(204).end();}
      const token=req.headers.authorization,session=req.headers["mcp-session-id"],cursor=req.headers["last-event-id"]===undefined?undefined:Buffer.from(req.headers["last-event-id"],"latin1").toString("utf8");
      assert.ok(["Bearer response-first","Bearer response-second"].includes(token));
      if(req.method==="DELETE"){record.deleted++;return res.writeHead(204).end();}
      let rpc;if(req.method==="POST"){let body="";for await(const part of req)body+=part;rpc=JSON.parse(body);assert.equal(cursor,undefined,"cursor leaked onto POST");}
      record.requests.push({method:rpc?.method??req.method,rpc:rpc?.id,token,session,cursor,at:performance.now()});
      const track=()=>{streams.add(res);record.opened++;res.once("close",()=>{streams.delete(res);record.closed++;});};
      const sse=(original,n)=>{
        res.writeHead(200,{"content-type":"text/event-stream"});
        if(q.get("cursor")!=="none")res.write(`id: ${original.id}/游标/${n}\nretry: ${q.get("retry")??0}\ndata:\n\n`);
        if(q.get("cursor")==="reset")res.write("id:\n\n");
        let body=q.get("caps")==="1"&&legacy?frame({jsonrpc:"2.0",id:`cap-${original.id}-${n}`,method:"roots/list",params:{}}):"";
        if(q.get("bad")==="1")return res.end(body+"data: {bad}\n\n");
        if(q.get("padding")==="1")body+=`:${"x".repeat(160000)}\n\n`;
        if(n===0||q.get("flap")==="1"||original.params.arguments.message==="gc-failure")return res.end(body+"data: {");
        record.emit=()=>{for(let n=0;n<32;n++)res.write(frame({jsonrpc:"2.0",method:"notifications/progress",params:{progress:n,progressToken:original.params._meta.progressToken}}));};
        if(q.get("hang")==="1"){res.write(body);return;}
        const structured=q.get("large")==="1"?'"value":18446744073709551615':`"message":${JSON.stringify(original.params.arguments.message)}`;
        res.end(body+`data: {"jsonrpc":"2.0","id":${original.id},"result":{"content":[],"structuredContent":{${structured}}}}\n\n`);
      };
      if(req.method==="GET") {
        assert.ok(legacy);track();
        if(cursor===undefined){record.listens++;res.writeHead(200,{"content-type":"text/event-stream"});res.write("id: listener\ndata:\n\n");return;}
        const originalId=Number(cursor.split('/')[0]),original=record.calls.find(call=>call.rpc.id===originalId)?.rpc;assert.ok(original);
        const n=(record.getCount[originalId]??0)+1;record.getCount[originalId]=n;
        assert.equal(cursor,`${originalId}/游标/${n-1}`);assert.equal(session,"response-session");
        record.gets.push({rpc:originalId,token,session,cursor,at:performance.now()});
        if(q.has("status"))return res.writeHead(Number(q.get("status"))).end();
        if(q.get("opening")==="1")return;
        return sse(original,n);
      }
      if(rpc.method===undefined) {
        assert.equal(rpc.error.code,-32601);const original=Number(rpc.id.split('-')[1]);record.replies.push({rpc:original,token,session,at:performance.now()});
        const delay=Number(!rpc.id.endsWith("-0")&&q.has("finaldelay")?q.get("finaldelay"):q.get("replydelay")??0);
        const timer=setTimeout(()=>{record.acks.push({rpc:original,at:performance.now()});res.writeHead(Number(q.get("replystatus")??202)).end();},delay);res.once("close",()=>clearTimeout(timer));return;
      }
      const send=result=>res.writeHead(200,{"content-type":"application/json"}).end(JSON.stringify({jsonrpc:"2.0",id:rpc.id,result}));
      if(rpc.method==="notifications/cancelled"){record.cancelled.push({rpc:rpc.params.requestId,token,session});return res.writeHead(202).end();}
      if(rpc.method==="notifications/initialized")return res.writeHead(202).end();
      if(rpc.method==="server/discover")return legacy?res.writeHead(400,{"content-type":"application/json"}).end(JSON.stringify({jsonrpc:"2.0",id:rpc.id,error:{code:-32601,message:"initialize first"}})):send({supportedVersions:["2026-07-28"]});
      if(rpc.method==="initialize"){record.initialized++;res.setHeader("mcp-session-id","response-session");return send({protocolVersion:"2025-11-25",capabilities:{tools:{listChanged:true}}});}
      if(rpc.method==="tools/list")return send({tools:[{name:"echo",inputSchema:{type:"object",properties:{message:{type:"string"}},required:["message"]}}]});
      assert.equal(rpc.method,"tools/call");record.calls.push({rpc,token,session});track();
      const respond=()=>q.get("json")==="1"?send({content:[],structuredContent:rpc.params.arguments}):sse(rpc,0);
      if(q.get("hold")==="1"){record.pending.push(respond);return;}
      const barrier=Number(q.get("barrier")??1);
      if(record.calls.length<barrier){record.pending.push(respond);return;}
      if(record.calls.length===barrier)for(const pending of record.pending.splice(0))pending();
      respond();
    }catch(cause){error??=cause;if(!res.headersSent)res.writeHead(500);res.end();}
  });
  server.on("session",session=>{sessions.add(session);session.once("close",()=>sessions.delete(session));});
  return {server,records,streams,check:()=>assert.ifError(error),get preflights(){return preflights;},async close(){for(const response of streams)response.destroy();for(const session of sessions)session.destroy();server.closeAllConnections?.();await new Promise(resolve=>server.close(resolve));}};
}
