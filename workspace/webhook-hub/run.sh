#!/usr/bin/env bash
# One command to start the complete runtime environment:
# control-plane API + delivery workers + a demo partner receiver.
set -euo pipefail
cd "$(dirname "$0")"
exec python3 -m hub --host "${HUB_HOST:-0.0.0.0}" --port "${HUB_PORT:-8080}" \
  --demo-receiver-port "${RECEIVER_PORT:-9100}"
