#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# start-worker.sh — connect a worker to the TLS router
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

CERT="$SCRIPT_DIR/certs/cert.pem"
KEY="$SCRIPT_DIR/certs/key.pem"

if [[ ! -f "$CERT" || ! -f "$KEY" ]]; then
    echo "No certificates found. Run gen-certs.sh first:"
    echo "  bash $SCRIPT_DIR/gen-certs.sh"
    exit 1
fi

# ── Locate dterm from the virtual environment ─────────────────────────────
if [[ -x "$PROJECT_DIR/.venv/bin/dterm" ]]; then
    DTERM="$PROJECT_DIR/.venv/bin/dterm"
elif command -v dterm &>/dev/null; then
    DTERM="dterm"
else
    echo "dterm not found. Activate your virtual environment or run: pip install daffi-terminals"
    exit 1
fi

# Override router host/port via environment variables if needed:
#   RPC_HOST=192.168.1.10 bash start-worker.sh
RPC_HOST="${RPC_HOST:-localhost}"
RPC_PORT="${RPC_PORT:-9999}"

echo "Using dterm: $DTERM"
echo "Connecting worker to $RPC_HOST:$RPC_PORT (TLS)..."

"$DTERM" start-worker \
    --rpc-host  "$RPC_HOST" \
    --rpc-port  "$RPC_PORT" \
    --ssl-cert  "$CERT"     \
    --ssl-key   "$KEY"
