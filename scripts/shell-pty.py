"""Exercise real interactive completion and capture argv without calling a tool."""
import json
import fcntl
import os
import pty
import re
import select
import subprocess
import sys
import time
import termios
from pathlib import Path

request = json.loads(Path(sys.argv[1]).read_text())
env = {key:value for key,value in os.environ.items() if not key.startswith("BASH_FUNC_") and key not in ("BASH_ENV","ENV")}
env.update(request["env"], TERM="dumb", HISTFILE="/dev/null", INPUTRC="/dev/null")
master, slave = pty.openpty()
def child_session():
    os.setsid()
    fcntl.ioctl(0, termios.TIOCSCTTY, 0)
    os.tcsetpgrp(0, os.getpgrp())
process = subprocess.Popen(request["command"], stdin=slave, stdout=slave, stderr=slave, env=env, preexec_fn=child_session)
os.close(slave)
transcript = bytearray()

def receive(timeout=0.05):
    if select.select([master], [], [], timeout)[0]:
        try:
            data = os.read(master, 65536)
        except OSError:
            data = b""
        transcript.extend(data)
        return data
    return b""

def until(predicate, seconds=8):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        receive()
        if predicate():
            return
        if process.poll() is not None:
            break
    raise RuntimeError(transcript.decode(errors="replace")[-12000:])

def lines(path):
    return Path(path).read_text().splitlines() if Path(path).exists() else []

def screen():
    return re.sub(rb"\x1b\[[0-?]*[ -/]*[@-~]", b"", bytes(transcript)).replace(b"\r", b"\n")

try:
    os.write(master, (request["setup"] + "\n").encode())
    until(lambda: b"\nMCPX_READY> " in screen())
    for case in request["cases"]:
        before = len(lines(request["trace"]))
        os.write(master, (case["line"] + "\t").encode())
        until(lambda: len(lines(request["trace"])) > before)
        os.write(master, (case.get("afterTab", "") + "\n").encode())
        until(lambda: len(lines(request["results"])) > 0)
        values = lines(request["results"])
        actual = json.loads(values[-1])
        if Path(request["marker"]).exists():
            raise AssertionError("A completion candidate was executed as shell code")
        if actual != case["expected"]:
            raise AssertionError(f"{case['line']!r}: expected {case['expected']!r}, got {actual!r}\n{transcript.decode(errors='replace')[-8000:]}")
        Path(request["results"]).unlink()
        until(lambda: screen().rstrip().endswith(b"MCPX_READY>"))
    print(json.dumps({"cases": len(request["cases"])}))
finally:
    process.terminate()
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    os.close(master)
