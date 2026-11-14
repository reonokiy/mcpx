import assert from "node:assert/strict";
import {createServer} from "node:http";
import {readFile} from "node:fs/promises";
import {once} from "node:events";

export function notificationFixture() {
  const records=new Map(),streams=new Map(),held=new Map(),replies=new Map(),deletes=new Map();let error;
  const frame=value=>`data: ${JSON.stringify(value)}\r\n\r\n`;
  const server=createServer(async(req,res)=>{
    try {
      const url=new URL(req.url,"http://localhost"),id=url.searchParams.get("case"),legacy=url.searchParams.get("legacy")==="1";
      res.setHeader("access-control-allow-origin","*");
      res.setHeader("access-control-allow-headers","authorization, content-type, mcp-protocol-version, mcp-session-id, mcp-method, mcp-name, last-event-id");
      res.setHeader("access-control-expose-headers","mcp-session-id");
      res.setHeader("access-control-allow-methods","GET, POST, DELETE, OPTIONS");
      if(req.method==="OPTIONS")return res.writeHead(204).end();
      if(url.pathname==="/")return res.writeHead(200,{"content-type":"text/html"}).end("<!doctype html><title>Remote notifications</title>");
      const file=/^\/(web\/[a-z-]+\.js|dist\/web\/mcpx_web(?:_bg\.wasm|\.js)|scripts\/remote-(?:notification|recovery)-host\.mjs)$/.exec(url.pathname)?.[1];
      if(file)return res.writeHead(200,{"content-type":file.endsWith(".wasm")?"application/wasm":"text/javascript"}).end(await readFile(file));
      if(!["/mcp","/stats","/emit","/release"].includes(url.pathname))return res.writeHead(404).end();
      if(!records.has(id))records.set(id,{opens:0,closed:0,calls:0,replies:0,replyClosed:0,cancellations:0,deleted:[],deleteClosed:0,requests:[],callTokens:[],listenTokens:[],replyTokens:[],held:0,lists:0,listClosed:0});
      const record=records.get(id);
      if(url.pathname==="/stats")return res.writeHead(200,{"content-type":"application/json"}).end(JSON.stringify(record));
      if(url.pathname==="/release") {
        const sends=held.get(id)??[];
        if(url.searchParams.get("reverse")==="1")sends.reverse().forEach((send,n)=>setTimeout(send,n*3));else for(const send of sends)send();
        held.delete(id);record.held=0;
        for(const response of replies.get(id)??[])response.writeHead(202).end();replies.delete(id);
        for(const response of deletes.get(id)??[])response.writeHead(204).end();deletes.delete(id);
        return res.writeHead(204).end();
      }
      if(url.pathname==="/emit") {
        const stream=streams.get(id);assert.ok(stream,"No active notification stream");
        const kind=url.searchParams.get("kind");
        if(kind==="flood") {
          const chunk=`:${" ".repeat(32764)}\r\n\r\n`;
          for(let n=0;n<290;n++)if(!stream.response.write(chunk))await once(stream.response,"drain");
        }
        if(kind==="change"||kind==="bad"||kind==="flood")stream.response.write(frame({jsonrpc:"2.0",method:"notifications/tools/list_changed",params:legacy?{}:{_meta:{"io.modelcontextprotocol/subscriptionId":stream.rpc+(kind==="bad"?2**32:0)}}}));
        if(kind==="request-end"||kind==="request")stream.response.write(frame({jsonrpc:"2.0",id:-7,method:"roots/list",params:{}}));
        if(kind==="reset")stream.response.write("id:\r\n\r\n");
        if(kind==="broken"){stream.response.write("data: {not-json}\r\n\r\n");stream.response.end();}
        if(kind==="oversize")stream.response.write(`data: "${"x".repeat(9*1024*1024)}"\r\n\r\n`);
        if(kind==="end"||kind==="request-end") {
          if(!legacy)stream.response.write(frame({jsonrpc:"2.0",id:stream.rpc,result:{resultType:"complete",_meta:{"io.modelcontextprotocol/subscriptionId":stream.rpc}}}));
          stream.response.end();
        }
        return res.writeHead(204).end();
      }
      assert.ok(["Bearer notify-snapshot","Bearer notify-second"].includes(req.headers.authorization));
      if(req.method==="DELETE") {
        record.deleted.push({token:req.headers.authorization,session:req.headers["mcp-session-id"]});res.once("close",()=>record.deleteClosed++);
        if(record.deleted.length===1&&url.searchParams.get("holddelete")==="1"){deletes.set(id,[res]);return;}
        if(record.deleted.length===1&&url.searchParams.get("rejectdelete")==="1")return res.writeHead(503).end();
        return res.writeHead(204).end();
      }
      let rpc;
      if(req.method==="POST"){let raw="";for await(const chunk of req)raw+=chunk;rpc=JSON.parse(raw);}
      record.requests.push({method:rpc?.method??req.method,token:req.headers.authorization,session:req.headers["mcp-session-id"],lastEventId:req.headers["last-event-id"]===undefined?undefined:Buffer.from(req.headers["last-event-id"],"latin1").toString("utf8"),at:performance.now()});
      if(req.method==="GET"||rpc?.method==="subscriptions/listen") {
        assert.equal(req.method,legacy?"GET":"POST");
        record.opens++;record.listenTokens.push(req.headers.authorization);
        const stream={response:res,rpc:rpc?.id};streams.set(id,stream);
        res.once("close",()=>{record.closed++;if(streams.get(id)===stream)streams.delete(id);});
        if(url.searchParams.get("reject")==="1")return res.writeHead(405).end();
        if(record.opens>1&&url.searchParams.has("resumestatus"))return res.writeHead(Number(url.searchParams.get("resumestatus"))).end();
        if(record.opens>1&&url.searchParams.get("resumehang")==="1")return;
        if(url.searchParams.get("noheaders")==="1")return;
        res.writeHead(200,{"content-type":url.searchParams.get("badmime")==="1"?"application/json":"text/event-stream"});res.write(": open\r\n\r\n");
        if(legacy&&url.searchParams.get("recovery")==="1") {
          res.write(`id: 游标/${record.opens}\r\nretry: ${url.searchParams.get("retry")??0}\r\n\r\n`);
          if(url.searchParams.get("flap")==="1")res.end();
        }
        if(!legacy&&url.searchParams.get("noack")!=="1")res.write(frame({jsonrpc:"2.0",method:"notifications/subscriptions/acknowledged",params:{_meta:{"io.modelcontextprotocol/subscriptionId":rpc.id},notifications:{toolsListChanged:true}}}));
        return;
      }
      if(rpc.method==null) {
        assert.equal(rpc.id,-7);assert.equal(rpc.error.code,-32601);
        record.replies++;record.replyTokens.push(req.headers.authorization);
        res.once("close",()=>record.replyClosed++);
        if(url.searchParams.get("holdreply")==="1"){replies.set(id,[...(replies.get(id)??[]),res]);return;}
        return res.writeHead(202).end();
      }
      const send=result=>res.writeHead(200,{"content-type":"application/json"}).end(JSON.stringify({jsonrpc:"2.0",id:rpc.id,result}));
      if(legacy&&rpc.method==="server/discover")return res.writeHead(400,{"content-type":"application/json"}).end(JSON.stringify({jsonrpc:"2.0",id:rpc.id,error:{code:-32601,message:"initialize first"}}));
      if(rpc.method==="notifications/initialized")return res.writeHead(202).end();
      if(rpc.method==="notifications/cancelled"){record.cancellations++;return res.writeHead(202).end();}
      if(rpc.method==="initialize") {
        res.setHeader("mcp-session-id",req.headers.authorization==="Bearer notify-snapshot"?"notify-session":"second-session");
        return send({protocolVersion:"2025-11-25",capabilities:{tools:{listChanged:true}}});
      }
      if(rpc.method==="server/discover")return send({supportedVersions:["2026-07-28"],capabilities:{tools:{listChanged:true}}});
      if(rpc.method==="tools/list") {
        record.lists++;
        if(record.lists===Number(url.searchParams.get("rejectlistat")))return res.writeHead(503).end();
        const message=url.searchParams.get("registryref")==="1"?{$ref:"https://fixture.invalid/word"}:{type:"string"};
        const respond=()=>send({tools:[{name:"echo",inputSchema:{type:"object",properties:{message}}}]});
        const holdAt=Number(url.searchParams.get("holdlistat")??(url.searchParams.get("holdlist")==="1"?1:0));
        if(record.lists===holdAt){res.once("close",()=>record.listClosed++);held.set(id,[...(held.get(id)??[]),respond]);return;}
        return respond();
      }
      assert.equal(rpc.method,"tools/call");record.calls++;
      record.callTokens.push(req.headers.authorization);
      if(rpc.params.arguments.message==="interactive"&&!legacy) {
        if(!rpc.params.inputResponses) {
          assert.deepEqual(rpc.params._meta["io.modelcontextprotocol/clientCapabilities"].elicitation.form,{});
          return send({resultType:"input_required",requestState:"fixture opaque\u0000state",inputRequests:{confirm:{method:"elicitation/create",params:{message:"Continue?",requestedSchema:{type:"object",properties:{yes:{type:"boolean"}},required:["yes"]}}}}});
        }
        assert.equal(rpc.params.requestState,"fixture opaque\u0000state");
        assert.deepEqual(rpc.params.inputResponses,{confirm:{action:"accept",content:{yes:true}}});
      }
      if(rpc.params.arguments.message==="broken-response")return res.writeHead(200,{"content-type":"text/event-stream"}).end("data: {not-json}\r\n\r\n");
      const respond=()=>send({content:[],structuredContent:{message:rpc.params.arguments.message}});
      if(rpc.params.arguments.message?.startsWith("held-")||rpc.params.inputResponses&&url.searchParams.get("holdresume")==="1"){record.held++;held.set(id,[...(held.get(id)??[]),respond]);return;}
      respond();
    }catch(cause){error??=cause;if(!res.headersSent)res.writeHead(500);res.end();}
  });
  return {server,records,streams,check:()=>assert.ifError(error)};
}
