import signal
import socket
import uuid
from argparse import Namespace

from daffi.utils.logger import get_daffi_logger
from daffi.utils import colors

logger = get_daffi_logger("worker", colors.cyan)


def start_worker(args: Namespace) -> None:
    """
    Start a worker node:
      1. Import worker.py — this registers all @callback functions globally.
      2. Connect to the daffi Router.
      3. Block until killed (daffi autoreconnect handles reconnection).

    The TermRouter discovers this worker via a 'connected' daffi event and
    calls get_worker_info() to fetch metadata (host, mac, id).
    """
    # Import triggers @callback registration for all worker functions.
    import daffi_terminals.worker.worker as _worker_module

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

    conn = client.connect()
    _worker_module._conn = conn

    logger.info(
        "%r connected to router at %s:%s", worker_name, args.rpc_host, args.rpc_port
    )

    # Block indefinitely — daffi handles reconnects transparently.
    signal.pause()
