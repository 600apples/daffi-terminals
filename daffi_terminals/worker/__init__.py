import atexit
import os
import signal
import socket
import subprocess
import sys
import uuid
from argparse import Namespace

from daffi.utils.logger import get_daffi_logger
from daffi.utils import colors

logger = get_daffi_logger("worker", colors.cyan)


def _spawn_pty_helper(debug: bool) -> tuple[socket.socket, subprocess.Popen]:
    """
    Fork the PTY helper subprocess (macOS only) *before* daffi is imported.

    Must be called while the worker process is still single-threaded:
    after daffi's native client is created, spawning a child from a daffi
    callback thread on macOS has been observed to close the parent's RPC
    socket fd as a side effect.  Spawning the helper first means every
    later ``pty.fork()`` happens in this dedicated, single-purpose child
    instead of in the worker itself.
    """
    parent_sock, child_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    env = dict(os.environ)
    env["DAFFI_PTY_HELPER_FD"] = str(child_sock.fileno())
    if debug:
        env["DAFFI_PTY_HELPER_DEBUG"] = "1"

    logger.debug(
        "spawning PTY helper: py=%s cwd=%s fd=%d",
        sys.executable, os.getcwd(), child_sock.fileno(),
    )
    proc = subprocess.Popen(
        [sys.executable, "-m", "daffi_terminals.worker.pty_helper"],
        pass_fds=(child_sock.fileno(),),
        env=env,
        stdin=subprocess.DEVNULL,
        close_fds=True,
    )
    logger.debug("PTY helper spawned: pid=%d", proc.pid)
    # Helper inherits its end of the pair; our end stays open for IPC.
    child_sock.close()

    def _cleanup() -> None:
        logger.debug(
            "PTY helper cleanup: pid=%d alive=%s", proc.pid, proc.poll() is None
        )
        try:
            parent_sock.close()
        except OSError:
            pass
        if proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=1)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    atexit.register(_cleanup)
    return parent_sock, proc


def start_worker(args: Namespace) -> None:
    """
    Start a worker node:
      1. (macOS only) Spawn the PTY helper while we're still single-threaded.
      2. Import worker.py — this registers all @callback functions globally.
      3. Connect to the daffi Router.
      4. Block until killed (daffi autoreconnect handles reconnection).

    The TermRouter discovers this worker via a 'connected' daffi event and
    calls get_worker_info() to fetch metadata (host, mac, id).
    """
    debug = bool(getattr(args, "debug", False))
    logger.debug("start_worker: args=%r (pid=%d)", vars(args), os.getpid())

    helper_sock = None
    helper_proc = None
    if sys.platform == "darwin":
        helper_sock, helper_proc = _spawn_pty_helper(debug=debug)

    # Import triggers @callback registration for all worker functions.
    logger.debug("importing daffi_terminals.worker.worker …")
    import daffi_terminals.worker.worker as _worker_module

    if helper_sock is not None:
        _worker_module._helper_sock = helper_sock
        _worker_module._helper_proc = helper_proc

    logger.debug("importing daffi.Client …")
    from daffi import Client

    worker_name = args.name or f"{socket.gethostname()}-0x{uuid.uuid4().hex[:8]}"

    ssl_cert = getattr(args, "ssl_cert", None) or ""
    ssl_key = getattr(args, "ssl_key", None) or ""
    use_tls = bool(ssl_cert and ssl_key)

    client = Client(
        app_name=worker_name,
        host=args.rpc_host,
        port=int(args.rpc_port),
        tls=use_tls,
        cert_file=ssl_cert,
        key_file=ssl_key,
        autoreconnect=True,
        reconnect_delay=3.0,
    )
    # Set _GROUP before connect() so get_worker_info() returns the correct value.
    # The router calls get_worker_info() immediately on the 'connected' event —
    # if we set _GROUP after connect(), the router races and reads "" instead.
    _worker_module._GROUP = getattr(args, "group", None) or ""

    logger.debug(
        "calling client.connect(): host=%s port=%s tls=%s autoreconnect=True",
        args.rpc_host, args.rpc_port, use_tls,
    )
    conn = client.connect()
    _worker_module._conn = conn
    logger.debug("client.connect() returned: %r", conn)

    logger.info(
        "%r connected to router at %s:%s", worker_name, args.rpc_host, args.rpc_port
    )

    # Block indefinitely — daffi handles reconnects transparently.
    logger.debug("entering signal.pause() (pid=%d)", os.getpid())
    signal.pause()
