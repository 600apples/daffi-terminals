#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# start-router.sh — start daffi-terminals router with full TLS
#
# - RPC broker (workers connect here): TLS on :9999
# - Web server  (browser connects here): HTTPS/WSS on :8888
#
# Run gen-certs.sh first to create certs/cert.pem and certs/key.pem
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
# Mirror the same search order as the project Makefile.
if [[ -x "$PROJECT_DIR/.venv/bin/dterm" ]]; then
    DTERM="$PROJECT_DIR/.venv/bin/dterm"
elif command -v dterm &>/dev/null; then
    DTERM="dterm"
else
    echo "dterm not found. Activate your virtual environment or run: pip install daffi-terminals"
    exit 1
fi

echo "Using dterm: $DTERM"
echo ""
echo "Starting router..."
echo "  RPC  (workers) : tls://localhost:9999"
echo "  Web  (browser) : https://localhost:8888"
echo ""
echo "Open https://localhost:8888 in your browser."
echo "(Self-signed cert: click Advanced → Accept the Risk and Continue)"
echo ""

"$DTERM" start-router \
    --rpc-host  localhost \
    --rpc-port  9999 \
    --web-host  localhost \
    --web-port  8888 \
    --ssl-cert      "$CERT" \
    --ssl-key       "$KEY"  \
    --web-ssl-cert  "$CERT" \
    --web-ssl-key   "$KEY"
