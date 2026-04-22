import logging
from argparse import ArgumentParser

logging.basicConfig(level=logging.INFO)


def main():
    parser = ArgumentParser()
    subparsers = parser.add_subparsers()
    subparsers.required = True
    subparsers.dest = 'command'
    router_parser = subparsers.add_parser("start-router", help="Start router")
    worker_parser = subparsers.add_parser("start-worker", help="Start worker")

    router_parser.add_argument("--rpc-host", help="Router rpc host (Interaction with workers)", required=True)
    router_parser.add_argument("--rpc-port", help="Router rpc port (Interaction with workers", type=int, required=True)
    router_parser.add_argument("--web-host", help="Router web host (Interaction with user)", required=True)
    router_parser.add_argument("--web-port", help="Router web port (Interaction with user", type=int, required=True)
    router_parser.add_argument("--ssl-cert", default=None, help="Path to TLS certificate for RPC (router ↔ workers)")
    router_parser.add_argument("--ssl-key", default=None, help="Path to TLS private key for RPC (router ↔ workers)")
    router_parser.add_argument("--web-ssl-cert", default=None, help="Path to TLS certificate for the web server (browser ↔ router). Enables HTTPS/WSS.")
    router_parser.add_argument("--web-ssl-key", default=None, help="Path to TLS private key for the web server (browser ↔ router).")

    worker_parser.add_argument("--name", default=None,
                               help="Worker name. If not provided default name will be generated")
    worker_parser.add_argument("--group", default=None,
                               help="Visual group label shown in the sidebar (optional, up to 7 unique groups)")
    worker_parser.add_argument("--rpc-host", help="Router host to connect", required=True)
    worker_parser.add_argument("--rpc-port", help="Router port to connect", type=int, required=True)
    worker_parser.add_argument("--ssl-cert", default=None, help="Path to ssl certificate")
    worker_parser.add_argument("--ssl-key", default=None, help="Path to ssl private key")

    args = parser.parse_args()
    if parser.parse_args().command == "start-router":
        from daffi_terminals.router import start_router
        start_router(args)
    else:
        from daffi_terminals.worker import start_worker
        start_worker(args)


if __name__ == '__main__':
    main()
