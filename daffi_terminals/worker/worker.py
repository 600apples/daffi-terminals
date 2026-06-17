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
import array
import ctypes
import ctypes.util
import fcntl
import json
import multiprocessing
import os
import platform
import re
import socket
import struct
import subprocess
import sys
import time
import tty
import uuid
from threading import Event, Lock, Thread

from daffi import callback
from daffi.utils.logger import get_daffi_logger
from daffi.utils import colors

logger = get_daffi_logger("worker", colors.cyan)

BYTES_CHUNK = 64 * 1024  # max bytes per PTY read


def _detect_pty_helper_needed() -> bool:
    """
    Return True if PTY spawning must be routed through the helper subprocess.

    The helper is needed when ``fork()`` from a non-main thread is unsafe —
    i.e. when Python's default multiprocessing start method is ``"spawn"``.
    Python set that default on macOS (3.8+) for exactly this reason.
    ``"forkserver"`` is excluded: it is a user/library choice that says nothing
    about raw ``os.forkpty()`` safety.

    ``DAFFI_USE_PTY_HELPER=1/0`` overrides the detection for edge cases.
    """
    env = os.environ.get("DAFFI_USE_PTY_HELPER", "").lower()
    if env in ("1", "true", "yes"):
        return True
    if env in ("0", "false", "no"):
        return False

    try:
        return multiprocessing.get_start_method() == "spawn"
    except Exception:
        return sys.platform == "darwin"


_USE_PTY_HELPER = _detect_pty_helper_needed()
logger.info(
    "PTY helper: %s (platform=%s fork_method=%s)",
    "enabled" if _USE_PTY_HELPER else "disabled",
    sys.platform,
    multiprocessing.get_start_method(),
)

# Set by start_worker() immediately after client.connect().
# All callbacks below reference this at call time, not at decoration time.
_conn  = None
_GROUP = ""   # optional group label, set from --group CLI arg

# Set by start_worker() before daffi is connected when _USE_PTY_HELPER is True.
# The helper must be forked while the process is still single-threaded so that
# it never inherits live RPC sockets.
_helper_sock: "socket.socket | None" = None
_helper_proc = None
_helper_lock = Lock()

# Active PTY sessions: term_id (str) → master fd (int)
_terminals: dict = {}

# On macOS we additionally track the shell PID per session so we can ask the
# helper to signal it on teardown.  Unused on Linux.
_terminal_pids: dict = {}

# Stable per-process UUID — lets the TermRouter detect duplicate worker names.
_WORKER_UUID = str(uuid.uuid4())



def _helper_request(req: dict, expect_fd: bool = False) -> tuple[dict, list[int]]:
    """
    Send one JSON request to the PTY helper and read exactly one JSON response.
    Optionally receives a single file descriptor attached via ``SCM_RIGHTS``.
    """
    if _helper_sock is None:
        raise RuntimeError("PTY helper is not running")

    # Strip env from the debug log so we don't dump the whole environment.
    log_req = {k: v for k, v in req.items() if k != "env"}
    logger.debug("helper -> %s", log_req)

    payload = (json.dumps(req) + "\n").encode()
    with _helper_lock:
        _helper_sock.sendall(payload)

        buf = bytearray()
        received_fds: list[int] = []
        ancbufsize = socket.CMSG_LEN(array.array("i").itemsize) if expect_fd else 0

        while b"\n" not in buf:
            if expect_fd:
                chunk, ancdata, _flags, _addr = _helper_sock.recvmsg(4096, ancbufsize)
                for lvl, typ, cmsg in ancdata:
                    if lvl == socket.SOL_SOCKET and typ == socket.SCM_RIGHTS:
                        fds = array.array("i")
                        valid = len(cmsg) - (len(cmsg) % fds.itemsize)
                        fds.frombytes(cmsg[:valid])
                        received_fds.extend(fds.tolist())
            else:
                chunk = _helper_sock.recv(4096)
            if not chunk:
                raise RuntimeError("PTY helper closed its connection")
            buf.extend(chunk)

        line, _nl, _rest = bytes(buf).partition(b"\n")
        resp = json.loads(line.decode())
        logger.debug("helper <- %s  fds=%s", resp, received_fds)
        return resp, received_fds



def _spawn_pty(shell: str, env: dict) -> tuple[int, int] | None:
    """
    Spawn a PTY shell and return ``(pid, master_fd)``.

    When ``_USE_PTY_HELPER`` is True the call is delegated to the helper
    subprocess; otherwise we call ``os.forkpty()`` directly.  Returns None on
    failure.
    """
    if _USE_PTY_HELPER:
        try:
            resp, fds = _helper_request(
                {"action": "spawn", "shell": shell, "env": env},
                expect_fd=True,
            )
        except Exception as exc:
            logger.error("PTY helper spawn failed: %s", exc)
            return None

        if "error" in resp or not fds:
            logger.error("PTY helper refused spawn: %s", resp.get("error"))
            for fd in fds:
                try:
                    os.close(fd)
                except OSError:
                    pass
            return None
        return int(resp["pid"]), fds[0]

    pid, fdm = os.forkpty()
    if pid == 0:
        # child → become the shell
        try:
            os.execvpe(shell, [shell], env)
        finally:
            os._exit(1)  # reached only if execvpe fails
    return pid, fdm


def _run_terminal(term_id: str, ready: Event) -> None:
    """
    Spawn a PTY shell, then stream every byte of output to the TermRouter
    via fire-and-forget rpc_nowait calls.  Signals *ready* as soon as the
    master fd is stored so start_terminal() can return (and the caller
    can safely send resize events).
    """
    shell = os.getenv("SHELL") or "/bin/sh"
    env = os.environ.copy()
    logger.debug("_run_terminal: term_id=%s shell=%s", term_id, shell)

    spawned = _spawn_pty(shell, env)
    if spawned is None:
        ready.set()
        return
    pid, fdm = spawned

    _terminals[term_id] = fdm
    _terminal_pids[term_id] = pid
    logger.debug("_run_terminal: term_id=%s pid=%d fd=%d", term_id, pid, fdm)
    ready.set()
    try:
        while True:
            try:
                data = os.read(fdm, BYTES_CHUNK)
            except OSError as exc:
                logger.debug("PTY read EOF/err on term_id=%s: %s", term_id, exc)
                break
            if not data:
                logger.debug("PTY read returned 0 bytes on term_id=%s", term_id)
                break
            logger.debug("PTY → router: term_id=%s bytes=%d", term_id, len(data))
            try:
                _conn.rpc_nowait(receiver="TermRouter").send_terminal_output(term_id, data)
            except Exception as exc:
                logger.warning("send_terminal_output failed: %s", exc)
                break
    finally:
        logger.debug("_run_terminal cleanup: term_id=%s pid=%d", term_id, pid)
        _terminals.pop(term_id, None)
        _terminal_pids.pop(term_id, None)
        try:
            os.close(fdm)
        except OSError:
            pass
        _kill_shell(pid)
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
    info = {"host": host, "mac": mac, "id": _WORKER_UUID, "group": _GROUP}
    logger.debug("get_worker_info → %s", info)
    return info


def _format_uptime(secs: float) -> str:
    days, rem  = divmod(int(secs), 86400)
    hours, rem = divmod(rem, 3600)
    mins       = rem // 60
    parts = []
    if days:  parts.append(f"{days}d")
    if hours: parts.append(f"{hours}h")
    parts.append(f"{mins}m")
    return " ".join(parts)


# ─── platform-independent host fact gathering ────────────────────────────────
#
# Every helper below runs on each ``get_host_facts()`` call so the facts that
# change at runtime (uptime, free memory) are always fresh.
#
# IMPORTANT (macOS): we deliberately avoid ``subprocess`` here and use
# ``ctypes`` → sysctlbyname / Mach host_statistics64 instead.  Python's
# subprocess on macOS uses posix_spawn with POSIX_SPAWN_CLOEXEC_DEFAULT,
# which — when called from a daffi callback thread with open RPC sockets —
# has been observed to close the parent's socket fd (the callback returns
# EBADF on the very next read and the daffi client is torn down).  Calling
# the kernel directly via libc avoids the fd-table side effect entirely.


# ─── macOS: ctypes → libc/Mach (no subprocess) ───────────────────────────────

if sys.platform == "darwin":
    _libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True) \
        if hasattr(ctypes, "util") else ctypes.CDLL("libc.dylib", use_errno=True)

    def _mac_sysctl_u64(name: bytes) -> int:
        val  = ctypes.c_uint64(0)
        size = ctypes.c_size_t(ctypes.sizeof(val))
        if _libc.sysctlbyname(name, ctypes.byref(val), ctypes.byref(size), None, 0) != 0:
            raise OSError(ctypes.get_errno(), f"sysctlbyname({name!r}) failed")
        return val.value

    class _MacTimeval(ctypes.Structure):
        _fields_ = [("tv_sec", ctypes.c_int64), ("tv_usec", ctypes.c_int32)]

    def _mac_boottime_sec() -> int:
        tv   = _MacTimeval()
        size = ctypes.c_size_t(ctypes.sizeof(tv))
        if _libc.sysctlbyname(
            b"kern.boottime", ctypes.byref(tv), ctypes.byref(size), None, 0
        ) != 0:
            raise OSError(ctypes.get_errno(), "sysctlbyname(kern.boottime) failed")
        return tv.tv_sec

    # Mach VM statistics (host_statistics64 / HOST_VM_INFO64).
    # See <mach/vm_statistics.h>.  We only need free_count + inactive_count +
    # speculative_count, but we declare the full struct so the kernel copies
    # the complete payload without truncation warnings.
    class _VmStats64(ctypes.Structure):
        _fields_ = [
            ("free_count",                            ctypes.c_uint32),
            ("active_count",                          ctypes.c_uint32),
            ("inactive_count",                        ctypes.c_uint32),
            ("wire_count",                            ctypes.c_uint32),
            ("zero_fill_count",                       ctypes.c_uint64),
            ("reactivations",                         ctypes.c_uint64),
            ("pageins",                               ctypes.c_uint64),
            ("pageouts",                              ctypes.c_uint64),
            ("faults",                                ctypes.c_uint64),
            ("cow_faults",                            ctypes.c_uint64),
            ("lookups",                               ctypes.c_uint64),
            ("hits",                                  ctypes.c_uint64),
            ("purges",                                ctypes.c_uint64),
            ("purgeable_count",                       ctypes.c_uint32),
            ("speculative_count",                     ctypes.c_uint32),
            ("decompressions",                        ctypes.c_uint64),
            ("compressions",                          ctypes.c_uint64),
            ("swapins",                               ctypes.c_uint64),
            ("swapouts",                              ctypes.c_uint64),
            ("compressor_page_count",                 ctypes.c_uint32),
            ("throttled_count",                       ctypes.c_uint32),
            ("external_page_count",                   ctypes.c_uint32),
            ("internal_page_count",                   ctypes.c_uint32),
            ("total_uncompressed_pages_in_compressor", ctypes.c_uint64),
        ]

    _HOST_VM_INFO64       = 4
    _HOST_VM_INFO64_COUNT = ctypes.sizeof(_VmStats64) // ctypes.sizeof(ctypes.c_int32)

    _libc.mach_host_self.restype  = ctypes.c_uint  # host_t
    _libc.host_statistics64.argtypes = [
        ctypes.c_uint,                          # host_t
        ctypes.c_int,                           # host_flavor_t
        ctypes.c_void_p,                        # host_info64_t
        ctypes.POINTER(ctypes.c_uint32),        # host_info64_outCnt
    ]
    _libc.host_statistics64.restype = ctypes.c_int   # kern_return_t

    def _mac_vm_free_pages() -> int:
        host  = _libc.mach_host_self()
        stats = _VmStats64()
        count = ctypes.c_uint32(_HOST_VM_INFO64_COUNT)
        rc = _libc.host_statistics64(
            host, _HOST_VM_INFO64, ctypes.byref(stats), ctypes.byref(count)
        )
        if rc != 0:
            raise OSError(f"host_statistics64 failed: kern_return_t={rc}")
        return stats.free_count + stats.inactive_count + stats.speculative_count


def _uptime_seconds() -> float:
    """Seconds since boot, or 0.0 if the platform doesn't expose it."""
    if sys.platform.startswith("linux"):
        try:
            return time.clock_gettime(time.CLOCK_BOOTTIME)
        except (AttributeError, OSError):
            pass
        try:
            with open("/proc/uptime") as f:
                return float(f.read().split()[0])
        except Exception:
            return 0.0

    if sys.platform == "darwin":
        try:
            return max(0.0, time.time() - float(_mac_boottime_sec()))
        except Exception:
            return 0.0

    if sys.platform.startswith(("freebsd", "openbsd", "netbsd")):
        try:
            out = subprocess.check_output(
                ["sysctl", "-n", "kern.boottime"], text=True, timeout=3
            )
            m = re.search(r"sec\s*=\s*(\d+)", out)
            if m:
                return max(0.0, time.time() - float(int(m.group(1))))
        except Exception:
            return 0.0
        return 0.0

    if sys.platform == "win32":
        try:
            return ctypes.windll.kernel32.GetTickCount64() / 1000.0
        except Exception:
            return 0.0

    return 0.0


def _memory_mb() -> tuple[int, int]:
    """(total_mb, free_mb) for the local system."""
    if sys.platform.startswith("linux"):
        total = free = 0
        try:
            with open("/proc/meminfo") as f:
                for line in f:
                    key, val = line.split(":", 1)
                    kb = int(val.split()[0])
                    if key == "MemTotal":
                        total = kb // 1024
                    elif key == "MemAvailable":
                        free = kb // 1024
        except Exception:
            pass
        return total, free

    if sys.platform == "darwin":
        total = free = 0
        try:
            total = _mac_sysctl_u64(b"hw.memsize") // (1024 * 1024)
        except Exception:
            pass
        try:
            page_size = _mac_sysctl_u64(b"hw.pagesize")
            free = (_mac_vm_free_pages() * page_size) // (1024 * 1024)
        except Exception:
            pass
        return total, free

    if sys.platform.startswith(("freebsd", "openbsd", "netbsd")):
        total = 0
        try:
            total = int(subprocess.check_output(
                ["sysctl", "-n", "hw.physmem"], text=True, timeout=3
            ).strip()) // (1024 * 1024)
        except Exception:
            pass
        return total, 0

    if sys.platform == "win32":
        class _MEMSTATUSEX(ctypes.Structure):
            _fields_ = [
                ("dwLength",                ctypes.c_ulong),
                ("dwMemoryLoad",            ctypes.c_ulong),
                ("ullTotalPhys",            ctypes.c_ulonglong),
                ("ullAvailPhys",            ctypes.c_ulonglong),
                ("ullTotalPageFile",        ctypes.c_ulonglong),
                ("ullAvailPageFile",        ctypes.c_ulonglong),
                ("ullTotalVirtual",         ctypes.c_ulonglong),
                ("ullAvailVirtual",         ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]
        try:
            stat = _MEMSTATUSEX()
            stat.dwLength = ctypes.sizeof(_MEMSTATUSEX)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
            return (stat.ullTotalPhys // (1024 * 1024),
                    stat.ullAvailPhys // (1024 * 1024))
        except Exception:
            return 0, 0

    return 0, 0


def _distro_info() -> tuple[str, str]:
    """Return (name, version) for the OS distribution."""
    try:
        import distro as _distro
        name = _distro.name() or ""
        ver  = _distro.version() or ""
        if name:
            return name, ver
    except ImportError:
        pass

    if sys.platform.startswith("linux"):
        try:
            with open("/etc/os-release") as f:
                info = dict(
                    line.strip().split("=", 1)
                    for line in f
                    if "=" in line
                )
            return (info.get("NAME", "").strip('"'),
                    info.get("VERSION", "").strip('"'))
        except Exception:
            return "", ""

    if sys.platform == "darwin":
        ver, _, _ = platform.mac_ver()
        return "macOS", ver

    if sys.platform == "win32":
        rel, ver, _, _ = platform.win32_ver()
        return f"Windows {rel}" if rel else "Windows", ver

    return "", ""


@callback
def get_host_facts() -> dict:
    """
    Return live Ansible-style host facts for the worker machine.
    Computed fresh on every call so dynamic values (uptime, free memory)
    are never stale.
    """
    u = platform.uname()
    distro_name, distro_version = _distro_info()
    mem_total_mb, mem_free_mb   = _memory_mb()
    uptime_s = _uptime_seconds()

    raw = {
        "hostname":        u.node,
        "kernel":          u.release,
        "arch":            u.machine,
        "os":              u.system,
        "distro":          distro_name,
        "distro_version":  distro_version,
        "cpu_count":       os.cpu_count() or 0,
        "uptime":          _format_uptime(uptime_s) if uptime_s > 0 else "",
        "mem_total_mb":    mem_total_mb,
        "mem_free_mb":     mem_free_mb,
    }
    facts = {k: v for k, v in raw.items() if v}
    logger.debug("get_host_facts → %s", facts)
    return facts


@callback
def start_terminal(term_id: str) -> bool:
    """
    Spawn a PTY shell and wait until the master fd is ready before returning.
    This ensures that any resize_terminal() call that arrives immediately
    after (triggered by the browser's initial fitAddon.fit()) finds a valid
    fd in _terminals and isn't silently dropped.
    """
    logger.debug("start_terminal: term_id=%s", term_id)
    ready = Event()
    Thread(target=_run_terminal, args=(term_id, ready), daemon=True).start()
    got_ready = ready.wait(timeout=5)
    ok = got_ready and term_id in _terminals
    logger.debug("start_terminal: term_id=%s ready=%s in _terminals=%s",
                 term_id, got_ready, term_id in _terminals)
    return ok


def _kill_shell(pid: int) -> None:
    """Send SIGHUP to the shell process owning this PTY."""
    if _USE_PTY_HELPER:
        try:
            _helper_request({"action": "kill", "pid": pid, "sig": 1})  # SIGHUP
        except Exception as exc:
            logger.debug("_kill_shell helper kill failed: %s", exc)
        return
    try:
        os.kill(pid, 1)  # SIGHUP
    except ProcessLookupError:
        pass
    except OSError as exc:
        logger.debug("_kill_shell os.kill failed: %s", exc)


@callback
def stop_terminal(term_id: str):
    """Force the shell to exit so _run_terminal can tear the session down."""
    logger.debug("stop_terminal: term_id=%s", term_id)
    pid = _terminal_pids.get(term_id)
    if pid is None:
        logger.debug("stop_terminal: unknown term_id=%s", term_id)
        return
    _kill_shell(pid)


@callback
def receive_terminal_input(term_id: str, data: bytes):
    """Write keyboard bytes from the browser directly into the PTY."""
    fdm = _terminals.get(term_id)
    if fdm is not None:
        try:
            os.write(fdm, data)
        except OSError as exc:
            logger.debug("receive_terminal_input write failed: %s", exc)
    else:
        logger.debug("receive_terminal_input: unknown term_id=%s", term_id)


@callback
def resize_terminal(term_id: str, rows: int, cols: int):
    """Resize the PTY window (TIOCSWINSZ ioctl)."""
    logger.debug("resize_terminal: term_id=%s rows=%d cols=%d", term_id, rows, cols)
    fdm = _terminals.get(term_id)
    if fdm is not None:
        try:
            winsize = struct.pack("HHHH", rows, cols, 0, 0)
            fcntl.ioctl(fdm, tty.TIOCSWINSZ, winsize)
        except OSError as exc:
            logger.debug("resize_terminal ioctl failed: %s", exc)
