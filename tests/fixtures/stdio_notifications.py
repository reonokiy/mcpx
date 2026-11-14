import argparse
import json
import os
import sys
import threading
import time
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("--directory", required=True)
p.add_argument("--legacy", action="store_true")
p.add_argument("--mode", default="normal")
a = p.parse_args()
directory = Path(a.directory)
lock = threading.RLock()
subscription = None
integer = False
held = []
metadata = []
lists = 0
command_offset = (directory / "commands").stat().st_size

def record(value):
    with lock, (directory / "requests").open("a") as f:
        f.write(json.dumps(dict(pid=os.getpid(), **value)) + "\n")

def send(value):
    with lock:
        sys.stdout.write(json.dumps(value) + "\n")
        sys.stdout.flush()

def response(request, result):
    send(dict(jsonrpc="2.0", id=request["id"], result=result))

def ack(rpc):
    send(dict(jsonrpc="2.0", method="notifications/subscriptions/acknowledged", params={"_meta":{"io.modelcontextprotocol/subscriptionId":rpc}, "notifications":{"toolsListChanged":True}}))

def change(rpc):
    send(dict(jsonrpc="2.0", method="notifications/tools/list_changed", params={} if a.legacy else {"_meta":{"io.modelcontextprotocol/subscriptionId":rpc}}))

def complete(request):
    response(request, dict(content=[], structuredContent=request["params"]["arguments"]))

def catalog(request):
    response(request, dict(tools=[dict(name="echo", inputSchema=dict(type="object", properties=dict(message=dict(type="integer" if integer else "string"))))]))

def commands():
    global integer, subscription
    with (directory / "commands").open() as f:
        f.seek(command_offset)
        while True:
            position = f.tell()
            line = f.readline()
            if not line.endswith("\n"):
                f.seek(position)
                time.sleep(0.005)
                continue
            command = json.loads(line)
            with lock:
                if command["kind"] == "change":
                    integer = True
                    change(subscription)
                elif command["kind"] == "release":
                    for request in reversed(held):
                        complete(request)
                    held.clear()
                elif command["kind"] == "release_metadata":
                    for request in metadata:
                        catalog(request)
                    metadata.clear()
                elif command["kind"] == "late":
                    ack(command["rpc"])
                    change(command["rpc"])
                elif command["kind"] == "graceful":
                    response(dict(id=subscription), dict(resultType="complete", _meta={"io.modelcontextprotocol/subscriptionId":subscription}))
                    subscription = None
                elif command["kind"] == "exit":
                    os._exit(0)
                record(dict(command=command))

record(dict(start=True))
threading.Thread(target=commands, daemon=True).start()
for line in sys.stdin:
    request = json.loads(line)
    record(dict(request=request))
    method = request.get("method")
    if method == "server/discover":
        if a.legacy:
            send(dict(jsonrpc="2.0", id=request["id"], error=dict(code=-32601, message="legacy initialize required")))
        else:
            response(request, dict(supportedVersions=["2026-07-28"], capabilities=dict(tools=dict(listChanged=True))))
    elif method == "initialize":
        response(request, dict(protocolVersion="2025-11-25", capabilities=dict(tools=dict(listChanged=True))))
    elif method == "tools/list":
        with lock:
            lists += 1
            if a.mode == "metadata-hold" and lists == 2:
                metadata.append(request)
            else:
                catalog(request)
    elif method == "subscriptions/listen":
        assert not a.legacy
        assert request["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"] == "2026-07-28"
        subscription = request["id"]
        if a.mode == "reject":
            send(dict(jsonrpc="2.0", id=subscription, error=dict(code=-32601, message="subscription unsupported")))
        elif a.mode != "noack":
            ack(subscription + 2**32 if a.mode == "badack" else subscription)
    elif method == "tools/call":
        if str(request["params"]["arguments"].get("message", "")).startswith("held-"):
            with lock:
                held.append(request)
        else:
            complete(request)
    elif method == "notifications/cancelled":
        assert request["params"]["requestId"] is not None
    elif method == "notifications/initialized" or method is None:
        pass
    else:
        raise AssertionError(method)
record(dict(closed=True))
