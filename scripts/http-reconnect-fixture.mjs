import assert from "node:assert/strict";
import {createServer} from "node:http";
import {readFile} from "node:fs/promises";
export function reconnectFixture(serverFactory=createServer) {
  const records=new Map(),streams=new Set(),sessions=new Set();let error;
  const frame=value=>`data: ${JSON.stringify(value)}\n\n`;
  const server=serverFactory(async(req,res)=>{
    try {
      const url=new URL(req.url,"http://localhost"),q=url.searchParams,id=q.get("case");
      res.setHeader("access-control-allow-origin","*");res.setHeader("access-control-allow-headers","authorization,content-type,mcp-protocol-version,mcp-session-id,mcp-method,mcp-name,last-event-id");res.setHeader("access-control-expose-headers","mcp-session-id");res.setHeader("access-control-allow-methods","GET,POST,DELETE,OPTIONS");
      if(req.method==="OPTIONS")return res.writeHead(204).end();
      if(url.pathname==="/")return res.writeHead(200,{"content-type":"text/html"}).end("<!doctype html><title>HTTP reconnect</title>");
      const file=/^\/(web\/[a-z-]+\.js|dist\/web\/mcpx_web(?:_bg\.wasm|\.js)|scripts\/(?:http-reconnect-host|http-session-host)\.mjs)$/.exec(url.pathname)?.[1];
      if(file)return res.writeHead(200,{"content-type":file.endsWith(".wasm")?"application/wasm":"text/javascript"}).end(await readFile(file));
      if(!["/mcp","/stats","/config","/release","/end"].includes(url.pathname))return res.writeHead(404).end();
      if(!records.has(id))records.set(id,{legacy:q.get("legacy")==="1",settings:{},sessions:{},owners:{},epoch:0,requests:[],calls:[],deleted:[],listens:[],opened:0,closed:0,pending:[]});
      const r=records.get(id);
      if(r.settings.freshConnections)res.setHeader("connection","close");
      if(url.pathname==="/stats"){const {pending,...safe}=r;return res.writeHead(200,{"content-type":"application/json"}).end(JSON.stringify({...safe,held:pending.map(p=>p.role)}));}
      if(url.pathname==="/config"){for(const [key,value]of q)if(!["case","legacy"].includes(key)){if(key==="modern")r.legacy=value!=="1";else r.settings[key]=Number(value);}return res.writeHead(204).end();}
      if(url.pathname==="/release"){const pending=r.pending.filter(p=>!q.has("role")||p.role===q.get("role"));r.pending=r.pending.filter(p=>!pending.includes(p));for(const p of pending)if(!p.res.destroyed)p.send();return res.writeHead(204).end();}
      if(url.pathname==="/end"){for(const p of r.pending.filter(p=>p.role==="listener")){if(!r.legacy)p.res.write(frame({jsonrpc:"2.0",id:p.rpc.id,result:{resultType:"complete",_meta:{"io.modelcontextprotocol/subscriptionId":p.rpc.id}}}));p.res.end();}r.pending=r.pending.filter(p=>p.role!=="listener");return res.writeHead(204).end();}
      assert.ok(["Bearer reconnect-first","Bearer reconnect-second"].includes(req.headers.authorization));
      const token=req.headers.authorization,session=req.headers["mcp-session-id"],cursor=req.headers["last-event-id"];
      let rpc;if(req.method==="POST"){let body="";for await(const part of req)body+=part;rpc=JSON.parse(body);assert.equal(cursor,undefined);}
      r.requests.push({method:rpc?.method??req.method,rpc:rpc?.id,token,session,cursor,at:performance.now()});
      const track=()=>{streams.add(res);r.opened++;res.once("close",()=>{streams.delete(res);r.closed++;});};
      const hold=(role,send)=>{track();r.pending.push({res,role,send,rpc});};
      if(req.method==="DELETE") {
        r.deleted.push({token,session});assert.equal(r.owners[session],token,"DELETE used another session's credential");
        if(!r.sessions[session])return res.writeHead(404).end();
        if(r.settings.failDelete){r.settings.failDelete--;return res.writeHead(503).end();}
        const send=()=>{delete r.sessions[session];res.writeHead(204).end();};
        if(r.settings.holdDelete)return hold("delete",send);send();return;
      }
      if(req.method==="GET"&&cursor!==undefined){
        assert.ok(r.legacy);assert.ok(r.calls.some(c=>cursor===`tool-${c.rpc.id}`));
        assert.equal(r.sessions[session],token);track();
        if(r.settings.resume404){r.settings.resume404--;return res.writeHead(404).end();}
        throw new Error("Unexpected response recovery GET");
      }
      if(req.method==="GET"||rpc?.method==="subscriptions/listen") {
        assert.equal(req.method,r.legacy?"GET":"POST");
        if(r.legacy){assert.equal(r.sessions[session],token);assert.equal(cursor,undefined,"Reconnection reused a listener cursor");}
        if(r.settings.expireListen){r.settings.expireListen--;return res.writeHead(404).end();}
        track();r.listens.push({token,session,rpc:rpc?.id});
        res.writeHead(200,{"content-type":"text/event-stream"});
        if(r.legacy)res.write("id: old-checkpoint\nretry: 600\ndata:\n\n");
        else res.write(frame({jsonrpc:"2.0",method:"notifications/subscriptions/acknowledged",params:{_meta:{"io.modelcontextprotocol/subscriptionId":rpc.id},notifications:{toolsListChanged:true}}}));
        r.pending.push({res,role:"listener",rpc,send:()=>{}});return;
      }
      const send=result=>res.writeHead(200,{"content-type":"application/json"}).end(JSON.stringify({jsonrpc:"2.0",id:rpc.id,result}));
      if(rpc.method==="server/discover")return r.legacy?res.writeHead(400,{"content-type":"application/json"}).end(JSON.stringify({jsonrpc:"2.0",id:rpc.id,error:{code:-32601,message:"initialize first"}})):send({supportedVersions:["2026-07-28"],capabilities:{tools:{listChanged:true}}});
      if(rpc.method==="initialize"){const id=`session-${++r.epoch}`;if(!r.settings.stateless){r.sessions[id]=r.owners[id]=token;res.setHeader("mcp-session-id",id);}return send({protocolVersion:"2025-11-25",capabilities:{tools:{listChanged:true}}});}
      if(rpc.method==="notifications/initialized"&&r.settings.expireAck){r.settings.expireAck--;return res.writeHead(404).end();}
      if(rpc.method==="notifications/initialized"||rpc.method==="notifications/cancelled")return res.writeHead(202).end();
      if(rpc.method==="tools/list") {
        if(r.settings.page&&r.epoch===1){
          if(!rpc.params?.cursor)return send({tools:[{name:"obsolete",inputSchema:{type:"object"}}],nextCursor:"next"});
          return res.writeHead(404).end();
        }
        if(r.settings.dropList){r.settings.dropList--;req.socket.destroy();return;}
        if(r.settings.dropListBody){r.settings.dropListBody--;track();res.writeHead(200,{"content-type":"application/json"});res.write('{"jsonrpc":');setTimeout(()=>res.destroy(),15);return;}
        if(r.settings.expireList){r.settings.expireList--;if(r.settings.pending404){track();res.writeHead(404,{"content-type":"application/json"});res.write('{');return;}return res.writeHead(404).end();}
        if(r.settings.failList){r.settings.failList--;return res.writeHead(503).end();}
        const schema=r.settings.ref?{$ref:"https://fixture.invalid/reconnect-word"}:{type:r.settings.integer?"integer":"string"};
        const respond=()=>send({tools:[{name:"echo",inputSchema:{type:"object",properties:{message:schema},required:["message"]},outputSchema:{type:"object",properties:{message:schema},required:["message"]}}]});
        if(r.settings.holdList)return hold("list",respond);return respond();
      }
      assert.equal(rpc.method,"tools/call");r.calls.push({rpc,token,session});
      if(r.settings.expire){r.settings.expire--;const expired=()=>res.writeHead(r.settings.expireStatus||404).end();if(r.settings.holdExpired)return hold("expired",expired);return expired();}
      if(r.legacy&&!r.settings.stateless)assert.equal(r.sessions[session],token,"call used an invalid session");
      if(rpc.params.arguments.message==="interactive"&&!r.legacy&&!rpc.params.inputResponses)return send({resultType:"input_required",requestState:"old-state",inputRequests:{confirm:{method:"elicitation/create",params:{message:"Continue?",requestedSchema:{type:"object",properties:{yes:{type:"boolean"}},required:["yes"]}}}}});
      if(rpc.params.arguments.message==="resume-expired"){
        assert.ok(r.legacy);track();res.writeHead(200,{"content-type":"text/event-stream"});return res.end(`id: tool-${rpc.id}\nretry: 0\ndata:\n\ndata: {`);
      }
      const respond=()=>send({content:[],structuredContent:rpc.params.arguments});
      if(r.settings.holdCalls)return hold("call",respond);return respond();
    }catch(cause){error??=cause;if(!res.headersSent)res.writeHead(500);res.end();}
  });
  server.on("session",s=>{sessions.add(s);s.once("close",()=>sessions.delete(s));});
  return {server,records,streams,check:()=>assert.ifError(error),async close(){for(const res of streams)res.destroy();for(const s of sessions)s.destroy();server.closeAllConnections?.();await new Promise(resolve=>server.close(resolve));}};
}
