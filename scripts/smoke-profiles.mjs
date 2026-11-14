import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp, mkdir, readFile, writeFile, rm, symlink, lstat} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
const exec = promisify(execFile);
const binary = resolve(process.env.MCPX_CLI ?? 'target/debug/mcpx');
const root = await mkdtemp(join(tmpdir(), 'mcpx-profiles-'));
const run = (...args) => exec(binary, args, {cwd: root});
try {
  await run('servers', 'add', 'demo', '--profile', 'dev', '--url', 'https://dev.example.com/mcp');
  await run('--profile', 'prod', 'servers', 'add', 'demo', '--url', 'https://prod.example.com/mcp');
  for (const args of [['tools'], ['tool'], ['call', 'demo']]) {
    await assert.rejects(run(...args), e => {
      assert.equal(e.code, 2);
      assert.match(e.stderr, /^error:/);
      assert.match(e.stderr, /Usage: mcpx/);
      if (args[0] === 'tools') assert.match(e.stderr, /Usage: mcpx tools/);
      if (args[0] === 'call') assert.match(e.stderr, /Usage: mcpx call/);
      if (args[0] === 'tool') assert.match(e.stderr, /Commands:/);
      assert.doesNotMatch(e.stderr, /"execution_certainty"|"CommandParseError"/);
      return true;
    });
  }
  for (const args of [['profiles','use'], ['servers','add','example'], ['auth','login','--store','invalid'], ['--profile','dev','tools']]) {
    await assert.rejects(run(...args), e => {
      assert.equal(e.code, 2);
      const path = args[0] === '--profile' ? 'tools' : args.slice(0, 2).join(' ');
      assert.ok(e.stderr.includes(`Usage: mcpx ${path}`), e.stderr);
      assert.doesNotMatch(e.stderr, /"execution_certainty"/);
      return true;
    });
  }
  let text = await readFile(join(root, 'mcpx.toml'), 'utf8');
  assert.match(text, /profiles\.dev\.servers\.demo/);
  assert.match(text, /profiles\.prod\.servers\.demo/);
  assert.match((await run('--profile', 'dev', 'servers', 'list')).stdout, /demo/);
  await assert.rejects(run('servers', 'list'), e => e.stderr.includes('profile'));
  await assert.rejects(run('--profile', 'dev', 'servers', 'add', 'demo', '--url', 'https://other.example.com'), e => e.code === 2);
  assert.equal(await readFile(join(root, 'mcpx.toml'), 'utf8'), text);
  await run('--profile', 'dev', 'servers', 'add', 'local', '--', 'node', 'server.mjs', '--read-only');
  text = await readFile(join(root, 'mcpx.toml'), 'utf8');
  assert.match(text, /program = "node"/);
  assert.match(text, /"--read-only"/);
  assert.doesNotMatch((await run('--profile', 'prod', 'servers', 'list')).stdout, /local/);
  assert.match((await run('__complete', 'fish', '--line', 'mcpx --profile dev tools lo')).stdout, /local/);
  assert.equal((await run('__complete', 'fish', '--line', 'mcpx --profile prod tools lo')).stdout, '');
  await assert.rejects(run('--profile', 'dev', 'servers', 'add', 'bad', '--url', 'not-a-url'));
  assert.equal(await readFile(join(root, 'mcpx.toml'), 'utf8'), text);
  const legacy = join(root, 'legacy.toml');
  await writeFile(legacy, '# keep this comment\n[servers.original]\ntransport="http"\nurl="https://example.com/mcp"\n');
  await run('--config', legacy, '--profile', 'dev', 'servers', 'add', 'original', '--url', 'https://dev.example.com/mcp');
  const migrated = await readFile(legacy, 'utf8');
  assert.match(migrated, /keep this comment/);
  assert.match(migrated, /profiles\.default\.servers\.original/);
  assert.match((await run('--config', legacy, 'servers', 'list')).stdout, /original/);
  await mkdir(join(root, 'child'));
  assert.match((await exec(binary, ['--profile', 'prod', 'servers', 'list'], {cwd: join(root, 'child')})).stdout, /demo/);
  await run('profiles', 'use', 'dev');
  assert.match((await run('profiles', 'list')).stdout, /dev \(default\) \*/);
  const profiles=JSON.parse((await run('--output','json','profiles','list')).stdout);
  assert.deepEqual(profiles.find(p=>p.name==='dev'), {name:'dev',default:true,selected:true});
  assert.match((await run('servers', 'list')).stdout, /local/);
  assert.doesNotMatch((await run('--profile','prod','servers','list')).stdout, /local/);
  await run('servers','add','saved-default','--url','https://example.com/mcp');
  assert.match((await run('--profile','dev','servers','list')).stdout, /saved-default/);
  assert.doesNotMatch((await run('--profile','prod','servers','list')).stdout, /saved-default/);
  assert.match((await run('__complete','fish','--line','mcpx tools saved')).stdout, /saved-default/);
  assert.match((await run('__complete','fish','--line','mcpx profiles use pr')).stdout, /prod/);
  const beforeUnknown=await readFile(join(root,'mcpx.toml'),'utf8');
  await assert.rejects(run('profiles','use','missing'));
  assert.equal(await readFile(join(root,'mcpx.toml'),'utf8'), beforeUnknown);
  await exec(binary,['profiles','use','prod'],{cwd:join(root,'child')});
  assert.doesNotMatch((await run('servers','list')).stdout,/local/);
  await run('--config',legacy,'profiles','use','default');
  assert.match((await run('--config',legacy,'profiles','list')).stdout,/default \(default\)/);
  const mixed = join(root,'empty-legacy.toml');
  await writeFile(mixed,'servers = {}\n[profiles.default.servers.keep]\ntransport="http"\nurl="https://example.com/mcp"\n');
  await run('--config',mixed,'--profile','dev','servers','add','added','--url','https://dev.example.com/mcp');
  assert.match((await run('--config',mixed,'servers','list')).stdout,/keep/);
  assert.match((await run('--config',mixed,'--profile','dev','servers','list')).stdout,/added/);
  if (process.platform !== 'win32') {
    const link=join(root,'child','linked.toml');
    await symlink('../mcpx.toml',link);
    await run('--config',link,'profiles','use','dev');
    assert.equal((await lstat(link)).isSymbolicLink(),true);
    assert.match((await run('profiles','list')).stdout,/dev \(default\)/);
    await run('--config',link,'servers','add','via-link','--url','https://example.com/mcp');
    assert.equal((await lstat(link)).isSymbolicLink(),true);
    assert.match((await run('servers','list')).stdout,/via-link/);
  }
  console.log('PASS profiles: isolated duplicate names, add HTTP/stdio, atomic validation, migration, discovery, completion and saved default selection');
} finally {
  await rm(root, {recursive: true, force: true});
}
