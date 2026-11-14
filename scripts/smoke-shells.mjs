import assert from "node:assert/strict";
import {spawn} from "node:child_process";
import {mkdtemp,writeFile,readFile,rm,mkdir,access} from "node:fs/promises";
import {join,resolve} from "node:path";
import {tmpdir} from "node:os";
import {createMockServer,echo} from "./mock-server.mjs";

const root=await mkdtemp(join(tmpdir(),"mcpx-shells-"));
const mode=process.argv[2]??"native";
assert.ok(["native","wasi"].includes(mode));
const binary=resolve("target/debug/mcpx"), trace=join(root,"trace.jsonl"), results=join(root,"argv.jsonl"), marker=join(root,"executed");
const backend=mode==="native"?{command:binary,args:[]}:{command:process.env.WASMTIME??resolve("target/tools/wasmtime/wasmtime"),args:["run","-S","http","-S","cli-exit-with-code","-S",`cwd=${root}`,"--dir",`${root}::${root}`,resolve("target/wasm32-wasip2/debug/mcpx.wasm")]};
const dangerous=`zz$(touch\${IFS}${marker})`;
const fishDanger=`ff(touch ${marker})`;
const values=["two words",`quote'\" with spaces`,dangerous,fishDanger,"hh!unknown-history-event","col:on","雪だるま","existing-dir","bad\nrecord"];
const tool={...echo,inputSchema:{type:"object",properties:{mode:{type:"string",enum:values}}}};
const maliciousName=`evil$(touch\${IFS}${marker})`;
const fixture=createMockServer(undefined,{tools:[tool,{...tool,name:maliciousName},{...tool,name:"servers"}]});
await new Promise(resolve=>fixture.server.listen(0,"127.0.0.1",resolve));
const endpoint=`http://127.0.0.1:${fixture.server.address().port}/mcp`;
const config=join(root,"config with'quote.toml");
const quote=value=>"'"+value.replaceAll("'", "'\\''")+"'";
async function run(command,args,env={}) {
  return await new Promise((resolve,reject)=>{
    const child=spawn(command,args,{env:{...process.env,...env},stdio:["ignore","pipe","pipe"]});let stdout="",stderr="";
    const timer=setTimeout(()=>{child.kill("SIGKILL");reject(new Error(`shell test timed out: ${stderr}`));},45000);
    child.stdout.on("data",chunk=>stdout+=chunk);child.stderr.on("data",chunk=>stderr+=chunk);
    child.on("error",error=>{clearTimeout(timer);reject(error);});child.on("exit",code=>{clearTimeout(timer);code===0?resolve(stdout):reject(new Error(`${command} exited ${code}: ${stderr}\n${stdout}`));});
  });
}
try {
  await writeFile(config,`[servers.demo]\ntransport="http"\nurl=${JSON.stringify(endpoint)}\nprotocol="2026-07-28"\n`);
  await mkdir(join(root,"bin"));await mkdir(join(root,"existing-dir"));
  await mkdir(join(root,"nested-dir"));
  await writeFile(join(root,"nested-dir","config.toml"),"");
  await writeFile(join(root,"picked config.toml"),"");
  await writeFile(join(root,"bin","mcpx"),`#!${process.execPath}\nimport fs from 'node:fs';import{spawnSync}from'node:child_process';\nconst args=process.argv.slice(2),backend=${JSON.stringify(backend)};if(args[0]==='__complete'){const r=spawnSync(backend.command,[...backend.args,...args],{encoding:'utf8'});fs.appendFileSync(${JSON.stringify(trace)},JSON.stringify({args,status:r.status,stdout:r.stdout,stderr:r.stderr})+'\\n');process.stdout.write(r.stdout??'');process.exit(r.status??1);}fs.appendFileSync(${JSON.stringify(results)},JSON.stringify(args)+'\\n');\n`,{mode:0o755});
  // A module suffix is unnecessary when package.json explicitly chooses modules.
  await writeFile(join(root,"bin","package.json"),'{"type":"module"}');
  const env={PATH:`${join(root,"bin")}:${process.env.PATH}`,MCPX_COMPLETION_TRACE:trace,XDG_CONFIG_HOME:join(root,"xdg-config"),XDG_CACHE_HOME:join(root,"xdg-cache")};
  const prefix=`mcpx --config ${quote(config)} demo echo --mode `;
  const cases=[
    {line:`mcpx --config ${quote(config)} de`,expected:["--config",config,"demo"]},
    {line:`mcpx --config ${quote(config)} demo ec`,expected:["--config",config,"demo","echo"]},
    {line:`${prefix}tw`,expected:["--config",config,"demo","echo","--mode","two words"]},
    {line:`${prefix}'two w`,expected:["--config",config,"demo","echo","--mode","two words"]},
    {line:`${prefix}t'wo w`,expected:["--config",config,"demo","echo","--mode","two words"]},
    {line:`${prefix}quo`,expected:["--config",config,"demo","echo","--mode",values[1]]},
    {line:`${prefix}zz`,expected:["--config",config,"demo","echo","--mode",dangerous]},
    {line:`${prefix}'zz`,expected:["--config",config,"demo","echo","--mode",dangerous]},
    {line:`${prefix}"zz`,expected:["--config",config,"demo","echo","--mode",dangerous]},
    {line:`${prefix}'quo`,expected:["--config",config,"demo","echo","--mode",values[1]]},
    {line:`${prefix}ff`,expected:["--config",config,"demo","echo","--mode",fishDanger]},
    {line:`${prefix}"hh`,expected:["--config",config,"demo","echo","--mode","hh!unknown-history-event"]},
    {line:`${prefix}雪`,expected:["--config",config,"demo","echo","--mode","雪だるま"]},
    {line:`${prefix}existing-`,expected:["--config",config,"demo","echo","--mode","existing-dir"]},
    {line:`mcpx --config ${quote(config)} demo echo --mode=tw`,expected:["--config",config,"demo","echo","--mode=two words"]},
    {line:`mcpx --config ${quote(config)} demo echo --mo`,expected:["--config",config,"demo","echo","--mode"]},
    {line:`mcpx --url ${quote(endpoint)} completi`,expected:["--url",endpoint,"completions"]},
    {line:`mcpx --url ${quote(endpoint)} servers --mo`,expected:["--url",endpoint,"servers","--mode"]},
    {line:`mcpx --protocol=2026`,expected:["--protocol=2026-07-28"]},
    {line:`mcpx --config ${root}/picked`,expected:["--config",join(root,"picked config.toml")]},
    {line:`mcpx --config ${root}/nested-`,afterTab:"config.toml",expected:["--config",join(root,"nested-dir","config.toml")]},
    {line:`mcpx --config ${quote(config)} demo ev`,expected:["--config",config,"demo",maliciousName]},
  ];
  if(mode==="native") {
    const stdioConfig=join(root,"stdio.toml"), eof=join(root,"stdio-eof"), log=join(root,"stdio-requests.jsonl");
    const args=[resolve("tests/fixtures/stdio_server.py"),"--eof-marker",eof,"--requests-log",log];
    await writeFile(stdioConfig,`[servers.local]\ntransport="stdio"\nprogram="python3"\nargs=${JSON.stringify(args)}\nprotocol="2026-07-28"\n`);
    assert.equal(await run(binary,["__complete","fish","--line",`mcpx --config ${quote(stdioConfig)} local ec`]),"echo\n");
    assert.equal(await readFile(eof,"utf8"),"closed");
    const records=(await readFile(log,"utf8")).trim().split('\n').map(JSON.parse);
    assert.ok(records.some(record=>record.method==="tools/list"));
    assert.ok(records.every(record=>record.method!=="tools/call"));
    for(const pid of new Set(records.map(record=>record.pid))) assert.throws(()=>process.kill(pid,0),error=>error.code==="ESRCH");
    console.log("PASS native stdio completion: metadata only, EOF observed and child reaped");
  }
  for(const shell of ["bash","zsh","fish"]) {
    const script=await run(backend.command,[...backend.args,"--config","/not-readable","completions",shell]);
    const scriptPath=join(root,`completion.${shell}`);await writeFile(scriptPath,script);
    const executable=process.env[`MCPX_${shell.toUpperCase()}`]??shell;
    let setup,command;
    if(shell==="bash") {command=[executable,"--noprofile","--norc","-i"];setup=`source ${quote(scriptPath)}; PS1='MCPX_READY> '; cd ${quote(root)}`;}
    else if(shell==="zsh") {
      const dot=join(root,"zsh");await mkdir(dot);env.ZDOTDIR=dot;
      const extracted=process.env.MCPX_SHELL_ROOT;
      await writeFile(join(dot,".zshenv"),extracted?`module_path=(${quote(join(extracted,"usr/lib/x86_64-linux-gnu/zsh/5.9"))})\nfpath=(${quote(join(extracted,"usr/share/zsh/functions"))}/**/*(/))\n`:"");
      // Ignore insecure runner-installed completion directories without prompting
      // or trusting them; keep compinit's security checks enabled.
      command=[executable,"-d","-i"];setup=`autoload -Uz compinit; compinit -i -D; source ${quote(scriptPath)}; PROMPT='MCPX_READY> '; cd ${quote(root)}`;
    } else {command=[executable,"--no-config","-i"];setup=`source ${quote(scriptPath)}; function fish_prompt; printf 'MCPX_READY> '; end; cd ${quote(root)}`;}
    const request=join(root,`request-${shell}.json`);await writeFile(request,JSON.stringify({command,setup,env,trace,results,marker,cases}));
    let output;
    try { output=await run("python3",["scripts/shell-pty.py",request]); }
    catch(error) { throw new Error(`${error.message}\nBackend trace:\n${(await readFile(trace,'utf8')).split('\n').slice(-5).join('\n')}`); }
    assert.equal(JSON.parse(output).cases,cases.length);
    console.log(`PASS ${mode} ${shell} interactive completion: ${cases.length} actual TAB/argv cases`);
    await assert.rejects(access(marker),"Completion executed a candidate as shell code");
  }
  assert.equal(fixture.requests.filter(request=>request.method==="tools/call").length,0,"Completion dispatched business calls");
  console.log("PASS completion quoting: no candidate execution and zero tools/call");
} finally {fixture.server.closeAllConnections();await new Promise(resolve=>fixture.server.close(resolve));await rm(root,{recursive:true,force:true});}
