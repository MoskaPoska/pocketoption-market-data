"""
Market Data Provider — FastAPI Application Entry Point
======================================================

Startup sequence:
  1. Load cookies from storage_state.json   (SessionManager)
  2. Connect to Redis                        (RedisClient)
  3. Launch WebSocket collector as a task    (DirectCollector)

Shutdown sequence (reverse):
  1. Stop collector
  2. Close Redis
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import uvicorn
from fastapi import FastAPI, WebSocket
from fastapi.responses import JSONResponse

from app.config import settings
from app.session.manager import SessionManager
from app.collector.engine import DirectCollector
from app.collector.events_collector import EventsCollector
from app.broker.redis_client import RedisClient
from app.gateway.ws_handler import WebSocketGateway
from app.monitor.cookie_watcher import CookieWatcher

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format="%(asctime)s.%(msecs)03d | %(levelname)-8s | %(name)-30s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Singletons (module-level, shared across the process) ─────────────────────
redis_client     = RedisClient()
session_manager  = SessionManager(settings.STORAGE_STATE_PATH, redis_client)
quote_queue      = asyncio.Queue()  # Decouples collectors from Redis
collector        = DirectCollector(session_manager, quote_queue)
events_collector = EventsCollector(session_manager, quote_queue)
gateway          = WebSocketGateway(redis_client)
cookie_watcher   = CookieWatcher()

# ── Background Tasks ──────────────────────────────────────────────────────────
async def redis_publisher_task(queue: asyncio.Queue, redis: RedisClient) -> None:
    """Reads quotes from the queue and publishes them to Redis."""
    logger.info("Redis publisher task started.")
    while True:
        try:
            quote, latency_ms = await queue.get()
            await redis.publish_quote(quote, latency_ms)
            queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("Redis publisher task error: %s", exc)

# ── Application lifespan ──────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # ── Startup ──────────────────────────────────────────────────────────────
    logger.info("═══ Market Data Provider  starting ═══")

    await redis_client.connect()    # open Redis pool first
    await session_manager.load()    # parse storage_state.json or read from Redis

    # Start the publisher task
    publisher_task = asyncio.create_task(
        redis_publisher_task(quote_queue, redis_client), name="redis-publisher"
    )

    # Chat collector (chat-po.site) — trading signals from community
    collector_task = asyncio.create_task(
        collector.start(), name="direct-collector"
    )

    # Events collector (events-po.com) — real-time price feed ~0.5s/tick
    events_task = asyncio.create_task(
        events_collector.start(), name="events-collector"
    )

    # Cookie expiry watcher
    await cookie_watcher.start()

    yield   # ← application serves requests here

    logger.info("Shutting down …")
    await collector.stop()
    await events_collector.stop()
    await cookie_watcher.stop()

    for task in (collector_task, events_task, publisher_task):
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    await redis_client.close()
    logger.info("═══ Shutdown complete ═══")


# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="Market Data Provider",
    description=(
        "Low-latency WebSocket market data streaming service.\n\n"
        "**WS endpoint:** `ws://host:8000/ws/{symbol}`  \n"
        "**Message format:** `{symbol, price, timestamp, latency_ms, server_time}`"
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
)


# ── WebSocket gateway ─────────────────────────────────────────────────────────
@app.websocket("/ws/{symbol}")
async def ws_stream(websocket: WebSocket, symbol: str) -> None:
    """
    Stream real-time quotes for `symbol`.

    Connect with any WebSocket client:
        wscat -c ws://localhost:8000/ws/EURUSD_otc
    """
    await gateway.handle(websocket, symbol)


# ── REST monitoring endpoints ─────────────────────────────────────────────────
@app.get("/health", tags=["monitoring"], summary="System health check")
async def health() -> JSONResponse:
    redis_ok = await redis_client.ping()
    status = "ok" if (redis_ok and collector.is_connected) else "degraded"
    return JSONResponse(
        {
            "status": status,
            "collector_connected": collector.is_connected,
            "redis": "ok" if redis_ok else "error",
            "active_clients": gateway.connection_count,
        },
        status_code=200 if status == "ok" else 503,
    )


@app.get("/symbols", tags=["monitoring"], summary="Active streaming symbols")
async def active_symbols() -> dict:
    """Returns the list of symbols seen since service start."""
    return {"symbols": await redis_client.get_active_symbols()}


# ── Dev runner ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        log_level="debug" if settings.DEBUG else "info",
        access_log=False,
    )
