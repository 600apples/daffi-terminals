# SSL / HTTPS example

This example runs the full daffi-terminals stack with TLS on every connection:

| Connection | Protocol | Port |
|------------|----------|------|
| Browser → router web UI | **HTTPS / WSS** | 8888 |
| Worker → router RPC | **TLS TCP** | 9999 |

## Quick start

```bash
# 1. Generate certificates (once)
bash gen-certs.sh

# 2. Start the router (terminal 1)
bash start-router.sh

# 3. Start one or more workers (terminal 2, 3, …)
bash start-worker.sh

# 4. Open https://localhost:8888 in your browser
```

## Certificate strategy

`gen-certs.sh` tries two approaches in order:

### Option A — mkcert (recommended, no browser warning)

[mkcert](https://github.com/FiloSottile/mkcert) installs a local root CA into
your system and browser trust stores so Chrome/Firefox accept the certificate
without any warning.

```bash
# Ubuntu / Debian
sudo apt install mkcert

# macOS
brew install mkcert
```

Then run `gen-certs.sh` — it calls `mkcert -install` automatically.

### Option B — openssl self-signed (fallback)

If `mkcert` is not installed, `gen-certs.sh` falls back to a standard
`openssl` self-signed certificate.  Your browser will show a security
warning the first time:

1. Navigate to `https://localhost:8888`
2. Click **Advanced → Proceed to localhost (unsafe)**

The browser remembers the exception.  WebSocket (`wss://`) connections on
the same origin inherit the trust decision automatically, so the terminal
works normally after that one-time click.

## Remote workers

Workers on other machines need a copy of the same `certs/cert.pem` and
`certs/key.pem`.  Copy them over and set the router host:

```bash
RPC_HOST=192.168.1.10 bash start-worker.sh
```

## Using a real certificate

Replace `certs/cert.pem` and `certs/key.pem` with your real certificate and
key (e.g. from Let's Encrypt).  The scripts pick them up automatically.
