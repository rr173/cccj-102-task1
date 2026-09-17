"""Webhook delivery hub.

One command starts the full environment (control plane + delivery workers +
demo partner receiver):

    python3 -m hub --demo-receiver-port 9100
"""

import argparse
import threading

from .api import serve
from .config import Config
from .core import Hub
from .receiver import make_receiver_server


def main():
    parser = argparse.ArgumentParser(prog="hub", description="webhook delivery hub")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--db", default=None, help="SQLite database path")
    parser.add_argument("--demo-receiver-port", type=int, default=0,
                        help="also start a mock partner receiver on this port")
    args = parser.parse_args()

    cfg = Config.from_env()
    if args.host:
        cfg.host = args.host
    if args.port:
        cfg.port = args.port
    if args.db:
        cfg.db_path = args.db

    hub = Hub(cfg)

    if args.demo_receiver_port:
        server, _ = make_receiver_server(cfg.host, args.demo_receiver_port)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        print(f"[hub] demo partner receiver on http://{cfg.host}:{args.demo_receiver_port}",
              flush=True)

    serve(hub, cfg.host, cfg.port)


if __name__ == "__main__":
    main()
