"""
daffi-terminals worker — exposes a PTY shell to the TermRouter.

Registered @callback functions (called remotely by the TermRouter):
  get_worker_info()                        → dict
  get_host_facts()                         → dict
  start_terminal(term_id)                  → bool
  stop_terminal(term_id)                   → None
  receive_terminal_input(term_id, data)    → None
  resize_terminal(term_id, rows, cols)     → None
"""
import os
import fcntl
import tty
import struct
import socket
import uuid
import platform
import subprocess
import logging
from threading import Thread, Event

from daffi import callback
from daffi.utils.logger import get_daffi_logger
from daffi.utils import colors

logger = get_daffi_logger("worker", colors.cyan)

BYTES_CHUNK = 64 * 1024  # max bytes per PTY read

# Set by start_worker() immediately after client.connect().
# All callbacks below reference this at call time, not at decoration time.
_conn  = None
_GROUP = ""   # optional group label, set from --group CLI arg

# Active PTY sessions: term_id (str) → master fd (int)
_terminals: dict = {}

# Stable per-process UUID — lets the TermRouter detect duplicate worker names.
_WORKER_UUID = str(uuid.uuid4())


# ─── background thread ────────────────────────────────────────────────────────

def _run_terminal(term_id: str, ready: Event) -> None:
    """
    Forks a PTY shell, then streams every byte of output to the TermRouter
    via fire-and-forget rpc_nowait calls.  Signals *ready* as soon as the
    master fd is stored so that start_terminal() can return (and the caller
    can safely send resize events).
    """
    exeargv = [os.getenv("SHELL", "sh")]
    env = os.environ.copy()

    pid, fdm = os.forkpty()
    if pid == 0:
        # child → become the shell
        os.execvpe(exeargv[0], exeargv, env)
        os._exit(1)  # reached only if execvpe fails

    _terminals[term_id] = fdm
    ready.set()   # PTY is ready — unblock start_terminal()
    try:
        while True:
            try:
                data = os.read(fdm, BYTES_CHUNK)
            except OSError:
                break
            if not data:
                break
            try:
                _conn.rpc_nowait(receiver="TermRouter").send_terminal_output(term_id, data)
            except Exception as exc:
                logger.warning("send_terminal_output failed: %s", exc)
                break
    finally:
        _terminals.pop(term_id, None)
        try:
            os.close(fdm)
        except OSError:
            pass
        try:
            _conn.rpc_nowait(receiver="TermRouter").terminal_closed(term_id)
        except Exception as exc:
            logger.warning("terminal_closed notification failed: %s", exc)


# ─── @callback functions exposed to the TermRouter ───────────────────────────

@callback
def get_worker_info() -> dict:
    """Return identifying metadata for this worker node."""
    host = socket.gethostname()
    mac = ":".join(
        ["{:02x}".format((uuid.getnode() >> e) & 0xFF) for e in range(0, 48, 8)][::-1]
    )
    return {"host": host, "mac": mac, "id": _WORKER_UUID, "group": _GROUP}


@callback
def get_host_facts() -> dict:
    """
    Return Ansible-style host facts gathered from the local system.
    Uses only stdlib — no psutil or distro required.
    """
    u = platform.uname()

    # ── uptime ────────────────────────────────────────────────────────────────
    uptime_str = ""
    try:
        with open("/proc/uptime") as f:
            secs = float(f.read().split()[0])
        days, rem  = divmod(int(secs), 86400)
        hours, rem = divmod(rem, 3600)
        mins       = rem // 60
        parts = []
        if days:  parts.append(f"{days}d")
        if hours: parts.append(f"{hours}h")
        parts.append(f"{mins}m")
        uptime_str = " ".join(parts)
    except Exception:
        try:
            uptime_str = subprocess.check_output(
                ["uptime", "-p"], text=True, timeout=3
            ).strip().removeprefix("up ")
        except Exception:
            pass

    # ── memory (MB) ───────────────────────────────────────────────────────────
    mem_total_mb = mem_free_mb = 0
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                key, val = line.split(":", 1)
                kb = int(val.split()[0])
                if key == "MemTotal":
                    mem_total_mb = kb // 1024
                elif key == "MemAvailable":
                    mem_free_mb  = kb // 1024
    except Exception:
        pass

    # ── distro ────────────────────────────────────────────────────────────────
    distro_name = distro_version = ""
    try:
        import distro as _distro
        distro_name    = _distro.name()
        distro_version = _distro.version()
    except ImportError:
        try:
            with open("/etc/os-release") as f:
                info = dict(
                    line.strip().split("=", 1)
                    for line in f
                    if "=" in line
                )
            distro_name    = info.get("NAME",    "").strip('"')
            distro_version = info.get("VERSION", "").strip('"')
        except Exception:
            pass

    raw = {
        "hostname":        u.node,
        "kernel":          u.release,
        "arch":            u.machine,
        "os":              u.system,
        "distro":          distro_name,
        "distro_version":  distro_version,
        "cpu_count":       os.cpu_count() or 0,
        "uptime":          uptime_str,
        "mem_total_mb":    mem_total_mb,
        "mem_free_mb":     mem_free_mb,
    }
    # Return only fields that were successfully obtained (no empty strings, no zeros).
    return {k: v for k, v in raw.items() if v}


@callback
def start_terminal(term_id: str) -> bool:
    """
    Spawn a PTY shell and wait until the master fd is ready before returning.
    This ensures that any resize_terminal() call that arrives immediately
    after (triggered by the browser's initial fitAddon.fit()) finds a valid
    fd in _terminals and isn't silently dropped.
    """
    ready = Event()
    Thread(target=_run_terminal, args=(term_id, ready), daemon=True).start()
    ready.wait(timeout=5)   # practically instant; timeout guards against fork failure
    return True


@callback
def stop_terminal(term_id: str):
    """Send Ctrl-D to the shell to close the session gracefully."""
    fdm = _terminals.get(term_id)
    if fdm is not None:
        try:
            os.write(fdm, b"\x04")  # Ctrl-D → EOF
        except OSError:
            pass


@callback
def receive_terminal_input(term_id: str, data: bytes):
    """Write keyboard bytes from the browser directly into the PTY."""
    fdm = _terminals.get(term_id)
    if fdm is not None:
        try:
            os.write(fdm, data)
        except OSError:
            pass


@callback
def resize_terminal(term_id: str, rows: int, cols: int):
    """Resize the PTY window (TIOCSWINSZ ioctl)."""
    fdm = _terminals.get(term_id)
    if fdm is not None:
        try:
            winsize = struct.pack("HHHH", rows, cols, 0, 0)
            fcntl.ioctl(fdm, tty.TIOCSWINSZ, winsize)
        except OSError:
            pass
