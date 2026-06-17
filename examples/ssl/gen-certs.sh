#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# gen-certs.sh — generate a locally-trusted TLS certificate for daffi-terminals
#
# Two strategies (tried in order):
#   1. mkcert  — installs a root CA into your system/browser trust store so
#                Chrome/Firefox accept the cert with NO security warning.
#                Install once with:  sudo apt install mkcert  (or brew install mkcert)
#
#   2. openssl — self-signed fallback.  The browser WILL show a warning.
#                Visit https://localhost:8888 and click "Advanced → Proceed".
#                For WebSocket connections the browser inherits that trust
#                decision, so wss:// works right after you accept the page.
# ---------------------------------------------------------------------------
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)/certs"
mkdir -p "$DIR"

CERT="$DIR/cert.pem"
KEY="$DIR/key.pem"

if [[ -f "$CERT" && -f "$KEY" ]]; then
    echo "Certificates already exist in $DIR — delete them to regenerate."
    exit 0
fi

if command -v mkcert &>/dev/null; then
    echo ">>> mkcert found — generating locally-trusted certificate"
    mkcert -install 2>/dev/null || true
    mkcert -cert-file "$CERT" -key-file "$KEY" localhost 127.0.0.1 ::1
    echo ""
    echo "✓ Certificate trusted by your browser. No security warning expected."
else
    echo ">>> mkcert not found — falling back to self-signed certificate"
    echo "    (browser will show a security warning — click Advanced → Proceed)"
    echo "    To avoid the warning: sudo apt install mkcert  then re-run this script."
    echo ""
    openssl req -x509 -newkey rsa:4096 \
        -keyout "$KEY" -out "$CERT" \
        -days 365 -nodes \
        -subj "/CN=localhost" \
        -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
    echo ""
    echo "✓ Self-signed certificate generated."
fi

echo ""
echo "Files:"
echo "  cert: $CERT"
echo "  key:  $KEY"
