#!/usr/bin/env python3
"""Package already built release outputs; used by GitHub Actions."""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('kind', choices=['native', 'wasm', 'source'])
args = parser.parse_args()
version = tomllib.loads(Path('Cargo.toml').read_text())['workspace']['package']['version']
tag = f'v{version}'
if os.environ.get('GITHUB_REF_TYPE') == 'tag' and os.environ['GITHUB_REF_NAME'] != tag:
    raise SystemExit('Git tag must match the workspace package version')
output = ROOT / 'dist' / 'release'
output.mkdir(parents=True, exist_ok=True)

def command(*parts):
    return subprocess.check_output(parts, text=True).strip()

commit = command('git', 'rev-parse', 'HEAD')
compiler = command('rustc', '--version')
host = next(line.split(': ', 1)[1] for line in command('rustc', '-vV').splitlines() if line.startswith('host: '))

def copy(source, destination):
    source, destination = Path(source), Path(destination)
    if source.is_dir():
        shutil.copytree(source, destination)
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)


def notices(destination):
    """Keep the license/notice files shipped by installed Cargo dependencies."""
    cargo_home = Path(os.environ.get('CARGO_HOME', Path.home() / '.cargo'))
    summary = []
    for manifest in sorted((cargo_home / 'registry' / 'src').glob('*/*/Cargo.toml')):
        package = tomllib.loads(manifest.read_text())['package']
        name, v = package['name'], package['version']
        summary.append(f'{name} {v}: {package.get("license", "see included notices")}')
        for path in manifest.parent.rglob('*'):
            if path.is_file() and path.name.upper().startswith(('LICENSE', 'LICENCE', 'NOTICE', 'COPYING', 'COPYRIGHT')):
                relative = path.relative_to(manifest.parent)
                copy(path, destination / 'third-party-licenses' / f'{name}-{v}' / relative)
    sysroot = Path(command('rustc', '--print', 'sysroot'))
    for name in ['COPYRIGHT', 'LICENSE-MIT', 'LICENSE-APACHE']:
        path = sysroot / 'share' / 'doc' / 'rust' / name
        if path.is_file():
            copy(path, destination / 'third-party-licenses' / 'rust' / name)
    (destination / 'THIRD_PARTY_NOTICES.txt').write_text(
        'Notices from installed build dependencies; this may include build tools.\n\n' + '\n'.join(summary) + '\n'
    )


def bundle(suffix, populate, instructions):
    name = f'mcpx-{tag}-{suffix}'
    with tempfile.TemporaryDirectory(prefix='mcpx-release-') as temporary:
        root = Path(temporary) / name
        root.mkdir()
        for filename in ['LICENSE-MIT', 'LICENSE-APACHE', 'mcpx.example.toml']:
            copy(ROOT / filename, root / filename)
        (root / 'README.txt').write_text(instructions + '\n')
        (root / 'release.json').write_text(json.dumps({'version': version, 'tag': tag, 'commit': commit, 'compiler': compiler, 'artifact': suffix}, indent=2) + '\n')
        populate(root)
        notices(root)
        for path in root.rglob('*'):
            if path.is_file():
                path.chmod(0o755 if path.name in ['mcpx', 'mcpx.exe'] else 0o644)
            elif path.is_dir():
                path.chmod(0o755)
        archive = output / f'{name}.tar.gz'
        with tarfile.open(archive, 'w:gz') as target:
            target.add(root, arcname=name)
        print(archive.name, flush=True)


if args.kind == 'native':
    binary = ROOT / 'target' / 'release' / ('mcpx.exe' if os.name == 'nt' else 'mcpx')
    actual = command(str(binary), '--version')
    if actual != f'mcpx {version}':
        raise SystemExit(f'Wrong executable version: {actual}')
    bundle(host, lambda root: copy(binary, root / binary.name),
           f'mcpx {version}\nRun ./{binary.name} --help.\nSource: https://github.com/reonokiy/mcpx\n')
elif args.kind == 'wasm':
    bundle('wasm32-wasip2', lambda root: copy(ROOT / 'target/wasm32-wasip2/release/mcpx.wasm', root / 'mcpx.wasm'),
           f'mcpx {version} standalone WASI CLI\nwasmtime run -S http mcpx.wasm --help\n')
    def web(root):
        for name in ['web', 'wit', 'examples/web-worker', 'dist/web', 'dist/component']:
            copy(ROOT / name, root / name)
        copy(ROOT / 'target/wasm32-wasip2/release/mcpx_component.wasm', root / 'mcpx_component.wasm')
    bundle('web', web,
           f'mcpx {version} Web and Component bundle\nRun python3 -m http.server 8080 --bind 127.0.0.1 here.\nOpen http://127.0.0.1:8080/examples/web-worker/\nThe MCP endpoint must permit the browser origin through CORS.\n')
else:
    archive = output / f'mcpx-{tag}-source.tar.gz'
    with tempfile.TemporaryDirectory(prefix='mcpx-source-') as temporary:
        info=Path(temporary) / 'release.json'
        info.write_text(json.dumps({'version':version,'tag':tag,'commit':commit,'compiler':compiler,'artifact':'source'},indent=2)+'\n')
        subprocess.run(['git', 'archive', '--format=tar.gz', f'--prefix=mcpx-{tag}-source/', f'--add-file={info}', f'--output={archive}', 'HEAD'], check=True)
    print(archive.name, flush=True)
