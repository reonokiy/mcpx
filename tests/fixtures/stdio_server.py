import argparse
import json
import os
import pathlib
import sys
import time
import threading

parser = argparse.ArgumentParser()
parser.add_argument('--legacy', action='store_true')
parser.add_argument('--exit-after-calls', type=int, default=0)
parser.add_argument('--exit-message')
parser.add_argument('--fail-restarts', type=int, default=0)
parser.add_argument('--integer-after-restart', action='store_true')
parser.add_argument('--legacy-after-restart', action='store_true')
parser.add_argument('--legacy-first-launch', action='store_true')
parser.add_argument('--eof-delay', type=float, default=0)
parser.add_argument('--restart-discovery-delay', type=float, default=0)
parser.add_argument('--restart-exit-after-list', action='store_true')
parser.add_argument('--silent-probe', action='store_true')
parser.add_argument('--verbose', action='store_true')
parser.add_argument('--delay', type=float, default=0)
parser.add_argument('--invalid', action='store_true')
parser.add_argument('--eof-marker')
parser.add_argument('--start-count')
parser.add_argument('--requests-log')
parser.add_argument('--async-calls', action='store_true')
parser.add_argument('--selective-delay', action='store_true')
parser.add_argument('--barrier', type=int, default=0)
parser.add_argument('--partial-marker')
parser.add_argument('--fragment-pause', type=float, default=0.2)
parser.add_argument('--block-input-marker')
parser.add_argument('--interactive', action='store_true')
parser.add_argument('--resume-barrier', type=int, default=0)
parser.add_argument('--schema-refs', action='store_true')
parser.add_argument('literal', nargs='?')
args = parser.parse_args()
launch = 1
if args.start_count:
    path = pathlib.Path(args.start_count)
    launch = int(path.read_text()) + 1 if path.exists() else 1
    path.write_text(str(launch))
if 1 < launch <= 1 + args.fail_restarts:
    sys.exit(0)
if args.legacy_after_restart and launch > 1:
    args.legacy = True
if args.legacy_first_launch:
    args.legacy = launch == 1

tools = [
    {'name': 'echo', 'inputSchema': {'type': 'object', 'properties': {'message': {'type': 'string'}}, 'required': ['message'], 'additionalProperties': False}},
    {'name': 'inspect', 'inputSchema': {'type': 'object'}},
]
if args.integer_after_restart and launch > 1:
    tools[0]['inputSchema']['properties']['message']['type'] = 'integer'
if args.schema_refs:
    tools[0]['inputSchema'] = {'type': 'object', 'properties': {'message': {'$ref': 'https://schemas.mcpx.test/common.json#/$defs/message'}}, 'required': ['message']}
    tools[0]['outputSchema'] = {'$ref': 'https://schemas.mcpx.test/result.json'}

output_lock = threading.Lock()
barrier = threading.Barrier(args.barrier) if args.barrier else None
resume_barrier = threading.Barrier(args.resume_barrier) if args.resume_barrier else None

def emit(value, partial=False):
    wire = (json.dumps(value, ensure_ascii=False) + '\n').encode('utf-8')
    with output_lock:
        if partial and args.partial_marker:
            crab = wire.find('🦀'.encode('utf-8'))
            split = crab + 2 if crab >= 0 else len(wire) // 2
            sys.stdout.buffer.write(wire[:split])
            sys.stdout.buffer.flush()
            pathlib.Path(args.partial_marker).write_text(str(value['id']))
            time.sleep(args.fragment_pause)
            wire = wire[split:]
        sys.stdout.buffer.write(wire)
        sys.stdout.buffer.flush()

def progress(request):
    emit({'jsonrpc': '2.0', 'method': 'notifications/progress', 'params': {'progress': 1, 'progressToken': request.get('params', {}).get('_meta', {}).get('progressToken')}})

def async_call(request):
    progress(request)
    value = request['params']['arguments']
    message = value.get('message', '')
    if args.interactive and message.startswith('interactive-'):
        state = 'state:' + json.dumps(value, ensure_ascii=False)
        if 'inputResponses' not in request['params']:
            assert request['params']['_meta']['io.modelcontextprotocol/clientCapabilities']['elicitation']['form'] == {}
            result = {'resultType': 'input_required', 'requestState': state, 'inputRequests': {
                'confirm': {'method': 'elicitation/create', 'params': {'message': 'Continue?', 'requestedSchema': {
                    'type': 'object', 'properties': {'yes': {'type': 'boolean'}}, 'required': ['yes']}}}}}
            emit({'jsonrpc': '2.0', 'id': request['id'], 'result': result})
            return
        assert request['params']['requestState'] == state
        assert request['params']['inputResponses']['confirm']['content']['yes'] is True
        if resume_barrier:
            resume_barrier.wait(timeout=5)
    if barrier and message.startswith('batch-'):
        barrier.wait(timeout=5)
    if not args.selective_delay or message in ('slow', 'interactive-slow'):
        time.sleep(args.delay)
    if args.invalid:
        with output_lock:
            print('not protocol JSON', flush=True)
        return
    result = {'content': [{'type': 'text', 'text': json.dumps(value)}], 'structuredContent': value}
    emit({'jsonrpc': '2.0', 'id': request['id'], 'result': result}, partial=isinstance(message, str) and message.startswith('partial'))

call_count = 0
for line in sys.stdin:
    request = json.loads(line)
    method = request['method']
    if args.requests_log:
        with open(args.requests_log, 'a') as log:
            log.write(json.dumps({'method': method, 'pid': os.getpid(), 'id': request.get('id'), 'requestId': request.get('params', {}).get('requestId')}) + '\n')
    if method == 'tools/call':
        if args.exit_message and request.get('params', {}).get('arguments', {}).get('message') == args.exit_message:
            os._exit(0)
        call_count += 1
        if args.exit_after_calls and launch == 1 and call_count >= args.exit_after_calls:
            os._exit(0)
    if launch > 1 and method in ('server/discover', 'initialize') and args.restart_discovery_delay:
        time.sleep(args.restart_discovery_delay)
    if args.verbose:
        sys.stderr.write('diagnostic ' * 20000 + '\n')
        sys.stderr.flush()
    if method == 'server/discover' and (args.legacy or args.silent_probe):
        if args.silent_probe:
            continue
        response = {'jsonrpc': '2.0', 'id': request['id'], 'error': {'code': -32602, 'message': 'initialize first'}}
    elif method.startswith('notifications/'):
        continue
    else:
        if method == 'server/discover':
            result = {'supportedVersions': ['2026-07-28'], 'capabilities': {'tools': {}}}
        elif method == 'initialize':
            result = {'protocolVersion': '2025-11-25', 'capabilities': {'tools': {}}}
        elif method == 'tools/list':
            result = {'tools': tools}
        elif method == 'tools/call':
            if args.async_calls:
                threading.Thread(target=async_call, args=(request,), daemon=True).start()
                continue
            if args.delay:
                time.sleep(args.delay)
            if args.invalid:
                print('not protocol JSON', flush=True)
                continue
            params = request['params']
            if params['name'] == 'inspect':
                value = {'literal': args.literal, 'environment': os.getenv('MCPX_FIXTURE_VALUE'), 'cwd': os.getcwd()}
            else:
                value = params['arguments']
            progress(request)
            result = {'content': [{'type': 'text', 'text': json.dumps(value)}], 'structuredContent': value}
        else:
            result = {}
        response = {'jsonrpc': '2.0', 'id': request['id'], 'result': result}
    emit(response)
    if launch > 1 and method == 'tools/list' and args.restart_exit_after_list:
        os._exit(0)
    if method == 'tools/list' and args.block_input_marker:
        sys.stdin.buffer.read(1)
        pathlib.Path(args.block_input_marker).write_text(str(os.getpid()))
        time.sleep(10)

if args.eof_delay:
    time.sleep(args.eof_delay)
if args.eof_marker:
    pathlib.Path(args.eof_marker).write_text('closed')
