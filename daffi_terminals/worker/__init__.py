import atexit
import os
import signal
import socket
import subprocess
import sys
import threading
import uuid
from argparse import Namespace

from daffi.utils.logger import get_daffi_logger
from daffi.utils import colors

from daffi_terminals._utils import silence_native_stdio

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

    from daffi import Client

    worker_name = args.name or f"{socket.gethostname()}-0x{uuid.uuid4().hex[:8]}"

    ssl_cert = getattr(args, "ssl_cert", None) or ""
    ssl_key = getattr(args, "ssl_key", None) or ""
    use_tls = bool(ssl_cert and ssl_key)

    # Set _GROUP before the first connect() so get_worker_info() returns the
    # correct value.  The router calls get_worker_info() immediately on the
    # 'connected' event — setting it after connect() would lose the race.
    _worker_module._GROUP = getattr(args, "group", None) or ""

    RECONNECT_DELAY = 3.0

    def _make_client() -> Client:
        return Client(
            app_name=worker_name,
            host=args.rpc_host,
            port=int(args.rpc_port),
            tls=use_tls,
            cert_file=ssl_cert,
            key_file=ssl_key,
        )

    # _should_exit is set by the signal handler to break the reconnect loop.
    # _current_client holds a mutable reference so the handler can always stop
    # whichever client happens to be active at interrupt time.
    _should_exit = threading.Event()
    _current_client: list[Client] = [None]  # type: ignore[list-item]

    def _graceful_exit(signum, _frame):
        logger.debug("signal %d received; stopping daffi client", signum)
        _should_exit.set()
        try:
            if _current_client[0] is not None:
                _current_client[0].stop()
        except Exception:
            pass

    signal.signal(signal.SIGINT,  _graceful_exit)
    signal.signal(signal.SIGTERM, _graceful_exit)

    logger.debug("entering reconnect loop (pid=%d)", os.getpid())

    client: Client | None = None

    def _fresh_connect() -> bool:
        """Create a new Client and connect, storing it in *client* / *_current_client*.

        Returns True on success, False if the exit signal is set.
        Retries indefinitely (with RECONNECT_DELAY between attempts) until either
        connected or the exit event fires.
        """
        nonlocal client
        while not _should_exit.is_set():
            if client is not None:
                try:
                    client.stop()
                except Exception:
                    pass
            client = _make_client()
            _current_client[0] = client
            # daffi's Application.__init__ calls set_signal_handler() internally,
            # which overwrites whatever SIGINT/SIGTERM handler was in place before.
            # Re-install ours immediately so Ctrl-C always reaches _graceful_exit.
            signal.signal(signal.SIGINT,  _graceful_exit)
            signal.signal(signal.SIGTERM, _graceful_exit)
            try:
                logger.debug(
                    "connecting: host=%s port=%s tls=%s",
                    args.rpc_host, args.rpc_port, use_tls,
                )
                conn = client.connect()
                _worker_module._conn = conn
                return True
            except Exception as exc:
                logger.warning(
                    "Connect failed (%s); retrying in %.1fs...", exc, RECONNECT_DELAY
                )
                _should_exit.wait(timeout=RECONNECT_DELAY)
        return False

    try:
        if not _fresh_connect():
            return  # exit signal arrived during initial connect retries

        logger.info(
            "%r connected to router at %s:%s", worker_name, args.rpc_host, args.rpc_port
        )

        while not _should_exit.is_set():
            try:
                # join() blocks until disconnect; it attempts one internal
                # reconnect automatically before returning.
                client.join()
            except Exception as exc:
                if _should_exit.is_set():
                    break
                logger.warning(
                    "Connection lost (%s); reconnecting in %.1fs...",
                    exc, RECONNECT_DELAY,
                )
                _should_exit.wait(timeout=RECONNECT_DELAY)
            else:
                if _should_exit.is_set():
                    break
                # join() returned normally.  If daffi's internal reconnect
                # succeeded, _conn_num is still populated — loop back to
                # re-join the same client without touching anything.
                if client._conn_num is not None:
                    continue
                # Internal reconnect failed silently (no exception raised);
                # fall through to do a full reconnect.
                logger.info(
                    "Connection lost; reconnecting in %.1fs...", RECONNECT_DELAY
                )
                _should_exit.wait(timeout=RECONNECT_DELAY)

            if _should_exit.is_set():
                break

            if not _fresh_connect():
                break

            logger.info(
                "%r reconnected to router at %s:%s",
                worker_name, args.rpc_host, args.rpc_port,
            )
    finally:
        silence_native_stdio()
        if client is not None:
            try:
                client.stop()
            except Exception:
                logger.debug("daffi Client.stop() raised", exc_info=True)
