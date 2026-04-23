"""
daffi-terminals router — FastAPI web server + daffi TermRouter node.

TermRouter @callback functions (called remotely by worker nodes):
  send_terminal_output(term_id, data)   → None   (PTY output → browser WebSocket)
  terminal_closed(term_id)             → None   (PTY session ended → close WebSocket)

The TermRouter Client also registers an event handler so it learns about worker
connect/disconnect events without any explicit "register" call from the worker.
When a worker connects, the event handler fetches its metadata by calling
get_worker_info() on it.
"""

import time
import uuid
import asyncio
import logging
from pathlib import Path
from threading import Event
from dataclasses import dataclass, asdict, field
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from typing import Dict

import uvicorn
from fastapi import FastAPI, WebSocket, APIRouter
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.websockets import WebSocketDisconnect

from daffi import callback
from daffi.utils.logger import get_daffi_logger
from daffi.utils import colors

logger = get_daffi_logger("router", colors.green)

ROUTER_ROOT = Path(__file__).parent
STOP_MARKER = None  # sentinel: put in queue to signal terminal session ended

# ─── Shared state (set by start_router before the web server starts) ──────────

_conn = None                              # daffi ClientConnection for TermRouter
_ws_queues: Dict[str, Queue] = {}        # term_id → Queue[bytes | None]
_workers: Dict[str, "Worker"] = {}       # process_name → Worker
_worker_terminals: Dict[str, set] = {}   # process_name → set of active term_ids
_worker_update_event = Event()            # set by event/callback threads, waited by async observer
_shutdown = Event()                       # set during lifespan shutdown to unblock executor threads

# Dedicated executor for every blocking call the router dispatches via
# run_in_executor().  We *must not* share asyncio's default executor here:
# open terminals each keep a thread permanently parked in queue.get(), and
# with the default pool size (min(32, cpu+4) = 12 on an 8-core laptop) that
# quickly starves unrelated coroutines — facts requests and tab spawns would
# stop working the moment the 13th tab opens.  64 threads is plenty of
# headroom for hundreds of simultaneous PTY sessions.
_router_pool = ThreadPoolExecutor(max_workers=64, thread_name_prefix="router-io")


# ─── @callback functions exposed to worker nodes ──────────────────────────────

@callback
def send_terminal_output(term_id: str, data: bytes):
    """Receive a PTY output chunk from a worker and queue it for the browser."""
    q = _ws_queues.get(term_id)
    if q is not None:
        q.put_nowait(data)


@callback
def terminal_closed(term_id: str):
    """Worker signals that the PTY session has ended — unblock the WebSocket reader."""
    q = _ws_queues.get(term_id)
    if q is not None:
        q.put_nowait(STOP_MARKER)


# ─── Worker metadata ──────────────────────────────────────────────────────────

@dataclass
class Worker:
    host: str
    mac: str
    process_name: str
    id: str
    group: str = field(repr=False, default="")
    active: bool = field(repr=False, default=True)

    def serialize(self) -> dict:
        return asdict(self)


# ─── daffi event handler (called from daffi's executor thread) ────────────────

def _fetch_worker_info(member: str) -> None:
    """Background-thread job: fetch metadata for a freshly-connected worker.

    Runs on the router's own thread pool — *never* on daffi's event-dispatch
    thread.  Performing RPCs on the event thread can soft-deadlock a
    reconnecting worker because the same thread must also deliver the
    worker's callback-registration frames before the RPC can be satisfied.
    A short retry loop absorbs the small window between `connected` firing
    and the registrations becoming visible.
    """
    attempts = 3
    delay = 0.5
    for attempt in range(1, attempts + 1):
        try:
            logger.debug("get_worker_info(%r) attempt %d …", member, attempt)
            info = _conn.rpc(timeout=5, receiver=member).get_worker_info()
            break
        except Exception as exc:
            if attempt == attempts:
                logger.error("Failed to fetch info from %r: %s", member, exc)
                return
            logger.debug(
                "get_worker_info(%r) failed (%s); retry %d/%d in %.1fs",
                member, exc, attempt, attempts, delay,
            )
            time.sleep(delay)
            delay *= 2

    worker = Worker(
        host=info["host"],
        mac=info["mac"],
        process_name=member,
        id=info["id"],
        group=info.get("group") or "",
        active=True,
    )
    existing = _workers.get(member)
    if existing and existing.id != worker.id and existing.active:
        logger.warning(
            "Duplicate worker name %r detected — previous session deactivated.", member
        )
        existing.active = False
    _workers[member] = worker
    logger.info("Worker connected: %s", worker)
    _worker_update_event.set()


def _daffi_event_handler(event: dict) -> None:
    """
    Handle member connected / disconnected events.

    Connected  → schedule metadata fetch on router pool (never block the
                 event-dispatch thread on an RPC).
    Disconnected → mark the worker as inactive and signal the async observer.
    """
    event_type = event.get("type")
    member = event.get("member", "")
    logger.debug("daffi event: type=%s member=%s raw=%s", event_type, member, event)

    # Ignore events for ourselves.
    if member == "TermRouter":
        return

    if event_type == "connected":
        _router_pool.submit(_fetch_worker_info, member)

    elif event_type == "disconnected":
        worker = _workers.get(member)
        if worker:
            worker.active = False
            logger.info("Worker disconnected: %s", worker)

        # Signal every open terminal session for this worker to close now.
        # Without this, _pump_output blocks on queue.get() forever because
        # the dead worker will never send terminal_closed().
        for term_id in list(_worker_terminals.pop(member, set())):
            q = _ws_queues.get(term_id)
            if q is not None:
                q.put_nowait(STOP_MARKER)

    # Signal the async update-workers observer regardless of event type.
    _worker_update_event.set()


# ─── Pure-ASGI wrapper ────────────────────────────────────────────────────────
#
# Starlette's BaseHTTPMiddleware has a long-standing quirk
# (https://github.com/encode/starlette/issues/1438) that turns a client-side
# disconnection mid-request into an `asyncio.CancelledError` surfacing as a 500
# with a noisy traceback.  We don't use BaseHTTPMiddleware any more, but we
# still want to guard against the same pattern at the edge: if the browser
# closes a tab while /api/workers/.../facts is blocked in a slow RPC, the
# inner task gets cancelled and the error would otherwise be logged by
# uvicorn.  Swallowing `CancelledError` here keeps the router silent and
# stable when clients come and go.

class _CancelSafeASGI:
    """Wraps an ASGI app, dropping CancelledError from disconnected clients."""

    def __init__(self, app):
        self._app = app

    async def __call__(self, scope, receive, send):
        # DevTools source-map probes produce harmless 404 noise.  Answer them
        # with a cheap 204 before they reach the router.
        if scope["type"] == "http" and scope.get("path", "").endswith(".map"):
            await send({"type": "http.response.start", "status": 204, "headers": []})
            await send({"type": "http.response.body", "body": b""})
            return
        try:
            await self._app(scope, receive, send)
        except asyncio.CancelledError:
            # Client went away mid-request; uvicorn's request task will be
            # torn down cleanly.  Swallowing here prevents a spurious
            # "Exception in ASGI application" log entry.
            return
        except Exception:
            logger.exception("unhandled error in ASGI app")
            raise


# ─── FastAPI web handler ───────────────────────────────────────────────────────

class WebHandler:
    """Manages the browser-facing FastAPI application."""

    def __init__(
        self,
        web_host: str,
        web_port: int,
        ssl_cert: str | None = None,
        ssl_key:  str | None = None,
    ) -> None:
        self.web_host = web_host
        self.web_port = web_port
        self.ssl_cert = ssl_cert
        self.ssl_key  = ssl_key
        self._director_sockets: Dict[int, WebSocket] = {}
        self._terminal_sockets: Dict[int, WebSocket] = {}
        self.app = _CancelSafeASGI(self._build_app())

    # ── app construction ──────────────────────────────────────────────────────

    def _build_app(self) -> FastAPI:
        app = FastAPI(lifespan=self._lifespan)

        static = StaticFiles(directory=ROUTER_ROOT / "static")
        app.mount("/static", static, name="static")

        api = APIRouter()
        api.add_api_route("/", self._index, methods=["GET"])
        api.add_api_route("/api/version", self._version, methods=["GET"])
        api.add_api_route("/api/workers/{worker_id}/facts", self._worker_facts, methods=["GET"])
        api.add_api_websocket_route("/director", self._director)
        api.add_api_websocket_route("/terminal", self._terminal)
        app.include_router(api)
        return app

    @asynccontextmanager
    async def _lifespan(self, *_):
        observer = asyncio.create_task(self._worker_update_observer())
        yield

        # ── Shutdown sequence ────────────────────────────────────────────────
        # uvicorn will not force-close open WebSocket connections on shutdown;
        # it just waits for their ASGI handlers to return (that's the
        # "Waiting for background tasks to complete" spinner).  We must
        # therefore proactively close every open WebSocket *and* unblock every
        # background thread before returning from the lifespan.

        _shutdown.set()
        _worker_update_event.set()   # unblock the observer's wait() call

        # Wake every output-pump thread parked in queue.get() so their
        # _terminal handlers proceed into their finally-block and exit.
        for q in list(_ws_queues.values()):
            try:
                q.put_nowait(STOP_MARKER)
            except Exception:
                pass

        # Force-close every open director/terminal WebSocket.  Both handlers
        # are idle in iter_json()/iter_bytes() and will only return once the
        # socket is closed.
        for sock in list(self._director_sockets.values()):
            try:
                await sock.close(code=1001)
            except Exception:
                pass
        for sock in list(self._terminal_sockets.values()):
            try:
                await sock.close(code=1001)
            except Exception:
                pass

        observer.cancel()
        try:
            await asyncio.wait_for(asyncio.shield(observer), timeout=2.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        _router_pool.shutdown(wait=False, cancel_futures=True)

    # ── HTTP ──────────────────────────────────────────────────────────────────

    async def _index(self):
        return FileResponse(ROUTER_ROOT / "static" / "index.html", media_type="text/html")

    async def _version(self):
        try:
            from daffi_terminals.__about__ import __version__
            version = __version__
        except Exception:
            version = "1.0.0-debug"
        return {"version": version}

    async def _worker_facts(self, worker_id: str):
        """Fetch host facts live from the worker on demand (never cached).

        Uses a short RPC timeout and wraps the blocking call in
        ``asyncio.wait_for`` so that a slow/unresponsive worker can never
        pin an executor thread past the point at which the client has
        moved on.  Without this guard, rapid tab-switching against a
        stalled worker quickly saturates the default threadpool and the
        whole router becomes unresponsive.
        """
        worker = _workers.get(worker_id)
        if not worker or not worker.active:
            return {}
        loop = asyncio.get_event_loop()
        rpc_timeout = 3
        try:
            facts = await asyncio.wait_for(
                loop.run_in_executor(
                    _router_pool,
                    lambda: _conn.rpc(timeout=rpc_timeout, receiver=worker_id).get_host_facts(),
                ),
                timeout=rpc_timeout + 1,
            )
            return facts or {}
        except asyncio.TimeoutError:
            logger.debug("get_host_facts from %r timed out", worker_id)
            return {}
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("get_host_facts from %r: %s", worker_id, exc)
            return {}

    # ── /director WebSocket ───────────────────────────────────────────────────

    async def _director(self, websocket: WebSocket):
        """
        Keeps the browser's worker-list sidebar up to date.
        Sends the full worker list on connect and on every change.
        Also handles 'delete_terminal' commands from the UI.
        """
        await websocket.accept()
        did = id(websocket)
        self._director_sockets[did] = websocket
        await websocket.send_json([w.serialize() for w in _workers.values()])
        try:
            async for data in websocket.iter_json():
                if data.get("command") == "delete_terminal":
                    _workers.pop(data.get("term_id"), None)
                    await self._broadcast_workers()
        except (WebSocketDisconnect, RuntimeError, asyncio.CancelledError):
            pass
        finally:
            self._director_sockets.pop(did, None)

    # ── /terminal WebSocket ───────────────────────────────────────────────────

    async def _terminal(self, websocket: WebSocket):
        """
        Bridges a browser xterm.js session to a worker PTY.

        Protocol (first byte of each WebSocket frame from browser):
          0x01 DATA   — keyboard input; rest of bytes sent to PTY
          0x02 RESIZE — "rows,cols" string; resize the PTY window
        """
        await websocket.accept()
        worker_id = websocket.query_params.get("worker_id")
        if not worker_id or worker_id not in _workers or not _workers[worker_id].active:
            logger.warning(
                "terminal ws rejected: worker_id=%r known=%s",
                worker_id, list(_workers.keys()),
            )
            await websocket.close(code=1008)
            return

        term_id = str(uuid.uuid4())
        queue: Queue = Queue()
        _ws_queues[term_id] = queue
        _worker_terminals.setdefault(worker_id, set()).add(term_id)
        tid = id(websocket)
        self._terminal_sockets[tid] = websocket
        loop = asyncio.get_event_loop()

        # Tell the worker to start a PTY for this session.
        try:
            await loop.run_in_executor(
                _router_pool,
                lambda: _conn.rpc(timeout=10, receiver=worker_id).start_terminal(term_id),
            )
        except Exception as exc:
            logger.error("start_terminal on %r failed: %s", worker_id, exc)
            _ws_queues.pop(term_id, None)
            _worker_terminals.get(worker_id, set()).discard(term_id)
            await websocket.close()
            return

        # Pump PTY output (queued by send_terminal_output callback) → browser.
        async def _pump_output():
            def _get():
                """Blocking get that unblocks either on data or on STOP_MARKER.

                Using a single blocking get (no 0.5 s poll) keeps the thread
                idle when nothing is happening, and the finally block below
                guarantees a STOP_MARKER is enqueued on every session exit —
                so the thread always returns promptly instead of leaking
                for the lifetime of the router process.
                """
                return queue.get()

            while True:
                data = await loop.run_in_executor(_router_pool, _get)
                if data is STOP_MARKER:
                    break
                try:
                    await websocket.send_bytes(data)
                except Exception:
                    break
            try:
                await websocket.close()
            except Exception:
                pass

        output_task = asyncio.create_task(_pump_output())

        # Relay browser keyboard/resize input → worker.
        try:
            async for payload in websocket.iter_bytes():
                if not payload:
                    continue
                cmd, body = payload[0], payload[1:]
                if cmd == 0x01:  # DATA
                    await loop.run_in_executor(
                        _router_pool,
                        lambda b=body: _conn.rpc_nowait(receiver=worker_id)
                        .receive_terminal_input(term_id, b),
                    )
                elif cmd == 0x02:  # RESIZE
                    try:
                        rows, cols = map(int, body.decode().split(","))
                        await loop.run_in_executor(
                            _router_pool,
                            lambda r=rows, c=cols: _conn.rpc_nowait(receiver=worker_id)
                            .resize_terminal(term_id, r, c),
                        )
                    except (ValueError, UnicodeDecodeError):
                        pass
        except (WebSocketDisconnect, RuntimeError, asyncio.CancelledError):
            pass
        finally:
            # Wake the output pump immediately so its executor thread exits
            # instead of leaking until the next terminal_closed callback.
            try:
                queue.put_nowait(STOP_MARKER)
            except Exception:
                pass
            output_task.cancel()
            _ws_queues.pop(term_id, None)
            _worker_terminals.get(worker_id, set()).discard(term_id)
            self._terminal_sockets.pop(tid, None)
            try:
                await loop.run_in_executor(
                    _router_pool,
                    lambda: _conn.rpc_nowait(receiver=worker_id).stop_terminal(term_id),
                )
            except Exception:
                pass

    # ── worker list broadcast ─────────────────────────────────────────────────

    async def _broadcast_workers(self):
        """Push the current worker list to every open director socket."""
        data = [w.serialize() for w in _workers.values()]
        for sock in list(self._director_sockets.values()):
            try:
                await sock.send_json(data)
            except Exception:
                pass

    async def _worker_update_observer(self):
        """
        Bridge between the sync daffi event-handler thread and the async
        FastAPI world.  Waits for _worker_update_event (set by the event
        handler or callbacks) then broadcasts the updated worker list.
        """
        loop = asyncio.get_running_loop()

        def _wait():
            """Poll with a timeout so the thread exits promptly on shutdown."""
            while not _shutdown.is_set():
                if _worker_update_event.wait(timeout=0.5):
                    return True
            return False

        while True:
            triggered = await loop.run_in_executor(_router_pool, _wait)
            if not triggered:
                break
            _worker_update_event.clear()
            await self._broadcast_workers()

    # ── entry point ───────────────────────────────────────────────────────────

    def run(self):
        kwargs = dict(host=self.web_host, port=self.web_port)
        if self.ssl_cert and self.ssl_key:
            kwargs["ssl_certfile"] = self.ssl_cert
            kwargs["ssl_keyfile"]  = self.ssl_key
        uvicorn.run(self.app, **kwargs)
