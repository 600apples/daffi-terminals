"""
Minimal PTY-spawning helper subprocess (macOS only).

On macOS, Python's ``subprocess``/``posix_spawn`` and bare ``os.forkpty()``
called from a non-main thread can interact badly with daffi's native RPC
transport — the parent's socket fd may be closed as a side effect,
surfacing as ``error.ReadError`` / ``EBADF`` in daffi's read loop.

To sidestep that entirely, we do the one thing the worker process must
*never* do itself on macOS: fork a child.  This helper is launched once
at worker startup (while the parent is still single-threaded, before
daffi is imported), and from then on every ``pty.fork()`` call happens
in this dedicated subprocess.  The worker talks to it over a Unix
socketpair, and the master-PTY fd is passed back via ``SCM_RIGHTS``.

Protocol (one JSON object per line, framed by newlines):

  request  {"action": "spawn", "shell": "/bin/zsh", "env": {...}, "cwd": "..."}
  response {"pid": 12345}                               + one fd via SCM_RIGHTS
  response {"error": "..."}                             (on failure)

  request  {"action": "kill", "pid": 12345, "sig": 1}
  response {"ok": true}

The helper must be started with the child end of the socketpair already
inherited on a known fd, passed via the ``DAFFI_PTY_HELPER_FD`` env var.
"""
from __future__ import annotations

import array
import json
import os
import pty
import signal
import socket
import sys


def _log(msg: str) -> None:
    if os.environ.get("DAFFI_PTY_HELPER_DEBUG"):
        sys.stderr.write(f"pty_helper[{os.getpid()}]: {msg}\n")
        sys.stderr.flush()


def _send(sock: socket.socket, payload: dict, fds: tuple[int, ...] = ()) -> None:
    data = (json.dumps(payload) + "\n").encode()
    if fds:
        sock.sendmsg(
            [data],
            [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", fds))],
        )
    else:
        sock.sendall(data)


def _recv_line(sock: socket.socket, buf: bytearray) -> bytes | None:
    while b"\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            return None
        buf.extend(chunk)
    line, _nl, rest = bytes(buf).partition(b"\n")
    buf.clear()
    buf.extend(rest)
    return line


def _handle_spawn(sock: socket.socket, req: dict) -> None:
    shell = req.get("shell") or os.environ.get("SHELL") or "/bin/sh"
    env   = req.get("env") or dict(os.environ)
    cwd   = req.get("cwd") or None
    _log(f"spawn shell={shell!r} cwd={cwd!r}")

    try:
        pid, master_fd = pty.fork()
    except OSError as exc:
        _log(f"pty.fork failed: {exc}")
        _send(sock, {"error": f"pty.fork failed: {exc}"})
        return

    if pid == 0:
        try:
            if cwd:
                os.chdir(cwd)
            os.execvpe(shell, [shell], env)
        except Exception as exc:
            sys.stderr.write(f"pty_helper child execvpe failed: {exc}\n")
        os._exit(1)

    try:
        _send(sock, {"pid": pid}, fds=(master_fd,))
    finally:
        # Parent no longer needs the master fd — the worker now owns a copy
        # of it (received via SCM_RIGHTS) and will close it when the session
        # ends.
        os.close(master_fd)


def _handle_kill(sock: socket.socket, req: dict) -> None:
    pid = int(req.get("pid", 0))
    sig = int(req.get("sig", signal.SIGHUP))
    _log(f"kill pid={pid} sig={sig}")
    try:
        os.kill(pid, sig)
        _send(sock, {"ok": True})
    except ProcessLookupError:
        _send(sock, {"ok": True, "note": "already exited"})
    except OSError as exc:
        _send(sock, {"error": str(exc)})


def main() -> None:
    # Reap children automatically so we don't accumulate zombies when a shell
    # exits.  We don't need to wait() on them — the worker learns about session
    # end by reading EOF on the master fd it got via SCM_RIGHTS.
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)
    # Exit silently on Ctrl-C / SIGTERM so the worker's teardown doesn't print
    # a scary traceback from the helper.
    signal.signal(signal.SIGINT,  lambda *_: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    fd_str = os.environ.get("DAFFI_PTY_HELPER_FD")
    if fd_str is None:
        sys.stderr.write("pty_helper: DAFFI_PTY_HELPER_FD not set\n")
        sys.exit(2)

    sock = socket.socket(fileno=int(fd_str))
    _log("started, awaiting requests")

    buf = bytearray()
    try:
        while True:
            line = _recv_line(sock, buf)
            if line is None:
                _log("parent closed the socket; exiting")
                return
            try:
                req = json.loads(line.decode())
            except Exception as exc:
                _send(sock, {"error": f"bad request: {exc}"})
                continue

            action = req.get("action")
            if action == "spawn":
                _handle_spawn(sock, req)
            elif action == "kill":
                _handle_kill(sock, req)
            else:
                _send(sock, {"error": f"unknown action: {action!r}"})
    except KeyboardInterrupt:
        pass
    finally:
        try:
            sock.close()
        except OSError:
            pass


if __name__ == "__main__":
    main()
