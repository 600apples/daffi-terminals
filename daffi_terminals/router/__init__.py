from argparse import Namespace
import os
import shutil
import tempfile

from daffi.utils.logger import get_daffi_logger
from daffi.utils import colors

from daffi_terminals._utils import generate_self_signed_tls, silence_native_stdio

logger = get_daffi_logger("router", colors.green)


def start_router(args: Namespace) -> None:
    """
    Start the TermRouter process:
      1. Import router.py — this registers the TermRouter @callback functions.
      2. Start the daffi Router (native message-routing server).
      3. Register member-added/removed handlers, then connect the TermRouter daffi Client.
      4. Start the FastAPI web server (blocks until the process exits).
    """
    # Importing router.py triggers @callback registration for send_terminal_output
    # and terminal_closed.  This MUST happen before client.connect().
    import daffi_terminals.router.router as _router_module

    from daffi import Router, Runtime, TCPTransport, UnixTransport
    from daffi.aio import AsyncClient

    rpc_host = args.rpc_host
    rpc_port = int(args.rpc_port)
    ssl_cert = getattr(args, "ssl_cert", None) or ""
    ssl_key = getattr(args, "ssl_key", None) or ""
    use_tls = bool(ssl_cert and ssl_key)

    tcp_kwargs = {}
    if use_tls:
        tcp_kwargs = {"tls": True, "cert": ssl_cert, "key": ssl_key}
    tcp_transport = TCPTransport(rpc_host, rpc_port, **tcp_kwargs)
    sock_dir = tempfile.mkdtemp(prefix="daffi-terminals-")
    unix_path = os.path.join(sock_dir, "router.sock")
    local_cert, local_key = generate_self_signed_tls(days=3650)
    unix_transport = UnixTransport(
        unix_path,
        tls=True,
        cert=local_cert,
        key=local_key,
        ca=local_cert,
    )

    # ── 1. daffi Router ──────────────────────────────────────────────────────
    daffi_router = Router(
        transport=[tcp_transport, unix_transport],
        runtime=Runtime.EVENTED,
    )
    daffi_router.start()
    logger.info("daffi Router listening on %s", tcp_transport.describe())

    # ── 2. TermRouter AsyncClient (local Unix + ephemeral TLS) ────────────────
    # Connect over the Unix listener.
    client = AsyncClient(
        app_name="TermRouter",
        transport=unix_transport,
        workers=3,
        runtime=Runtime.EVENTED,
    )

    # Event handlers MUST be registered before connect().
    client.on_member_added(_router_module._on_member_added)
    client.on_member_removed(_router_module._on_member_removed)

    # ── 3. FastAPI web server ─────────────────────────────────────────────────
    web_ssl_cert = getattr(args, "web_ssl_cert", None) or ""
    web_ssl_key  = getattr(args, "web_ssl_key",  None) or ""

    web_handler = _router_module.WebHandler(
        web_host=args.web_host,
        web_port=int(args.web_port),
        ssl_cert=web_ssl_cert or None,
        ssl_key=web_ssl_key or None,
        daffi_client=client,
    )
    try:
        web_handler.run()  # blocks until the process is killed
    finally:
        # Silence native stdout/stderr for the rest of the process.  daffi's
        # Zig transport logs "error.ReadError" from a background thread that
        # only wakes up *after* daffi_router.stop() has returned — any
        # restore-on-exit wrapper would race against that.
        silence_native_stdio()
        try:
            daffi_router.stop()
        except Exception:
            logger.debug("daffi Router.stop() raised", exc_info=True)
        try:
            shutil.rmtree(sock_dir, ignore_errors=True)
        except Exception:
            pass
