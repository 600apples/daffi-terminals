from argparse import Namespace

from daffi.utils.logger import get_daffi_logger
from daffi.utils import colors

from daffi_terminals._utils import silence_native_stdio

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
    # and terminal_closed.  This MUST happen before Client.connect().
    import daffi_terminals.router.router as _router_module

    from daffi import Router, Client

    rpc_host = args.rpc_host
    rpc_port = int(args.rpc_port)
    ssl_cert = getattr(args, "ssl_cert", None) or ""
    ssl_key = getattr(args, "ssl_key", None) or ""
    use_tls = bool(ssl_cert and ssl_key)

    # ── 1. daffi Router ──────────────────────────────────────────────────────
    daffi_router = Router(
        host=rpc_host,
        port=rpc_port,
        tls=use_tls,
        cert_file=ssl_cert,
        key_file=ssl_key,
    )
    daffi_router.start()
    logger.info("daffi Router listening on %s:%s", rpc_host, rpc_port)

    # ── 2. TermRouter Client ─────────────────────────────────────────────────
    client = Client(
        app_name="TermRouter",
        host=rpc_host,
        port=rpc_port,
        tls=use_tls,
        cert_file=ssl_cert,
        key_file=ssl_key,
    )

    # Event handlers MUST be registered before connect().
    client.on_member_added(_router_module._on_member_added)
    client.on_member_removed(_router_module._on_member_removed)

    conn = client.connect()

    # Make the connection available to router.py's callbacks and WebHandler.
    _router_module._conn = conn

    # ── 3. FastAPI web server ─────────────────────────────────────────────────
    web_ssl_cert = getattr(args, "web_ssl_cert", None) or ""
    web_ssl_key  = getattr(args, "web_ssl_key",  None) or ""
    use_web_tls  = bool(web_ssl_cert and web_ssl_key)

    web_handler = _router_module.WebHandler(
        web_host=args.web_host,
        web_port=int(args.web_port),
        ssl_cert=web_ssl_cert or None,
        ssl_key=web_ssl_key or None,
    )
    try:
        web_handler.run()  # blocks until the process is killed
    finally:
        # Silence native stdout/stderr for the rest of the process.  daffi's
        # Zig transport logs "error.ReadError" from a background thread that
        # only wakes up *after* client.stop()/daffi_router.stop() have
        # returned — any restore-on-exit wrapper would race against that.
        silence_native_stdio()
        try:
            client.stop()
        except Exception:
            logger.debug("daffi Client.stop() raised", exc_info=True)
        try:
            daffi_router.stop()
        except Exception:
            logger.debug("daffi Router.stop() raised", exc_info=True)
