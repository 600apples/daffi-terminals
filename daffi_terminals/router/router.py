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

import uuid
import asyncio
import logging
from pathlib import Path
from threading import Event
from dataclasses import dataclass, asdict, field
from contextlib import asynccontextmanager
from queue import Queue, Empty
from typing import Dict

import uvicorn
from fastapi import FastAPI, WebSocket, APIRouter
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.websockets import WebSocketDisconnect
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

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

def _daffi_event_handler(event: dict) -> None:
    """
    Handle member connected / disconnected events.

    Connected  → fetch worker metadata and register in _workers.
    Disconnected → mark the worker as inactive and signal the async observer.
    """
    event_type = event.get("type")
    member = event.get("member", "")

    # Ignore events for ourselves.
    if member == "TermRouter":
        return

    if event_type == "connected":
        try:
            info = _conn.rpc(timeout=5, receiver=member).get_worker_info()
            worker = Worker(
                host=info["host"],
                mac=info["mac"],
                process_name=member,
                id=info["id"],
                group=info.get("group") or "",
                active=True,
            )
            # Detect duplicate name: same process_name, different UUID, still active.
            existing = _workers.get(member)
            if existing and existing.id != worker.id and existing.active:
                logger.warning(
                    "Duplicate worker name %r detected — previous session deactivated.", member
                )
                existing.active = False
            _workers[member] = worker
            logger.info("Worker connected: %s", worker)
        except Exception as exc:
            logger.error("Failed to fetch info from %r: %s", member, exc)

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
        self.app = self._build_app()

    # ── app construction ──────────────────────────────────────────────────────

    def _build_app(self) -> FastAPI:
        app = FastAPI(lifespan=self._lifespan)

        # Silently return 204 for browser DevTools source-map requests (.map files).
        # Without this they generate harmless but noisy 404 log entries.
        class _SuppressMapRequests(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next):
                if request.url.path.endswith('.map'):
                    return Response(status_code=204)
                return await call_next(request)

        app.add_middleware(_SuppressMapRequests)

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
        # Signal all blocked executor threads to exit, then wait briefly for them.
        _shutdown.set()
        _worker_update_event.set()   # unblock the observer's wait() call
        observer.cancel()
        try:
            await asyncio.wait_for(asyncio.shield(observer), timeout=2.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

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
        """Fetch host facts live from the worker on demand (never cached)."""
        worker = _workers.get(worker_id)
        if not worker or not worker.active:
            return {}
        loop = asyncio.get_event_loop()
        try:
            facts = await loop.run_in_executor(
                None,
                lambda: _conn.rpc(timeout=10, receiver=worker_id).get_host_facts(),
            )
            return facts or {}
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
        loop = asyncio.get_event_loop()

        # Tell the worker to start a PTY for this session.
        try:
            await loop.run_in_executor(
                None,
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
                """Blocking get with a short timeout so the thread can exit on shutdown."""
                while not _shutdown.is_set():
                    try:
                        return queue.get(timeout=0.5)
                    except Empty:
                        pass
                return STOP_MARKER

            while True:
                data = await loop.run_in_executor(None, _get)
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
                        None,
                        lambda b=body: _conn.rpc_nowait(receiver=worker_id)
                        .receive_terminal_input(term_id, b),
                    )
                elif cmd == 0x02:  # RESIZE
                    try:
                        rows, cols = map(int, body.decode().split(","))
                        await loop.run_in_executor(
                            None,
                            lambda r=rows, c=cols: _conn.rpc_nowait(receiver=worker_id)
                            .resize_terminal(term_id, r, c),
                        )
                    except (ValueError, UnicodeDecodeError):
                        pass
        except (WebSocketDisconnect, RuntimeError, asyncio.CancelledError):
            pass
        finally:
            output_task.cancel()
            _ws_queues.pop(term_id, None)
            _worker_terminals.get(worker_id, set()).discard(term_id)
            try:
                await loop.run_in_executor(
                    None,
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
            triggered = await loop.run_in_executor(None, _wait)
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
