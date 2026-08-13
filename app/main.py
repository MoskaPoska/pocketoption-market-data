"""
PocketOption Market Data Provider
===================================
Uses pocketoptionapi-async for direct WebSocket connection.
Stores candles in Redis (LPUSH/LTRIM) so data survives C# bot restarts.

Env vars:
  REDIS_URL            - Redis connection URL
  PO_SESSION_TOKEN     - sessionToken from DevTools (just the value, not the full SSID)
  PO_UID               - Your PocketOption user ID
  PO_IS_DEMO           - "1" for demo, "0" for real (default: "1")
  PO_SYMBOLS           - Comma-separated symbols (default: "EURUSD_otc")
  PO_CANDLE_COUNT      - History candles to preload (default: 200)
  TELEGRAM_BOT_TOKEN   - For admin notifications
  TELEGRAM_ADMIN_IDS   - JSON array of admin IDs e.g. [123,456]
"""
import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI
from pocketoptionapi_async import AsyncPocketOptionClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
    datefmt="%H:%M:%S.%f"[:-3],
)
logger = logging.getLogger("app.main")

# ── Config ──────────────────────────────────────────────────────────────────

SESSION_TOKEN = os.environ["PO_SESSION_TOKEN"]
UID           = int(os.environ["PO_UID"])
IS_DEMO       = os.environ.get("PO_IS_DEMO", "1") == "1"
SYMBOLS       = [s.strip() for s in os.environ.get("PO_SYMBOLS", "EURUSD_otc").split(",")]
CANDLE_COUNT  = int(os.environ.get("PO_CANDLE_COUNT", "200"))
REDIS_URL     = os.environ["REDIS_URL"]

# Timeframes to subscribe: 60=1m, 300=5m, 900=15m
TIMEFRAMES = [60, 300, 900]

# Redis key: candles:{symbol}:{timeframe_seconds}
def redis_key(symbol: str, tf: int) -> str:
    return f"candles:{symbol.upper()}:{tf}"

# ── Redis ────────────────────────────────────────────────────────────────────

redis_pool: aioredis.Redis | None = None

async def get_redis() -> aioredis.Redis:
    global redis_pool
    if redis_pool is None:
        redis_pool = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    return redis_pool

async def push_candle(symbol: str, tf: int, candle: dict) -> None:
    """Push a closed candle to Redis list (newest at index 0), keep last 1000."""
    r = await get_redis()
    key = redis_key(symbol, tf)
    payload = json.dumps(candle)
    async with r.pipeline(transaction=False) as pipe:
        pipe.lpush(key, payload)
        pipe.ltrim(key, 0, 999)  # keep latest 1000
        await pipe.execute()

async def push_tick(symbol: str, price: float, ts: int) -> None:
    """Publish a raw tick for Pub/Sub (C# bot can also subscribe if needed)."""
    r = await get_redis()
    msg = json.dumps({"symbol": symbol, "price": price, "timestamp": ts})
    await r.publish(f"quotes:{symbol}", msg)

# ── Candle aggregation in memory ────────────────────────────────────────────

class CandleBuffer:
    """Aggregates ticks into OHLC candles per timeframe."""

    def __init__(self, symbol: str, tf_seconds: int):
        self.symbol = symbol
        self.tf = tf_seconds
        self.current: dict | None = None

    def _candle_start(self, ts: int) -> int:
        return (ts // self.tf) * self.tf

    async def on_tick(self, price: float, ts: int) -> None:
        start = self._candle_start(ts)

        if self.current is None:
            self.current = {"o": price, "h": price, "l": price, "c": price, "v": 1, "t": start}
            return

        if start == self.current["t"]:
            # Same candle — update
            self.current["h"] = max(self.current["h"], price)
            self.current["l"] = min(self.current["l"], price)
            self.current["c"] = price
            self.current["v"] += 1
        else:
            # New candle — close current, persist, start new
            await push_candle(self.symbol, self.tf, self.current)
            logger.info("[Candle %s/%ds] O=%.5f H=%.5f L=%.5f C=%.5f",
                        self.symbol, self.tf,
                        self.current["o"], self.current["h"],
                        self.current["l"], self.current["c"])
            self.current = {"o": price, "h": price, "l": price, "c": price, "v": 1, "t": start}


# symbol -> timeframe -> CandleBuffer
buffers: dict[str, dict[int, CandleBuffer]] = {}

def get_buffer(symbol: str, tf: int) -> CandleBuffer:
    if symbol not in buffers:
        buffers[symbol] = {}
    if tf not in buffers[symbol]:
        buffers[symbol][tf] = CandleBuffer(symbol, tf)
    return buffers[symbol][tf]

# ── PocketOption client ──────────────────────────────────────────────────────

po_client: AsyncPocketOptionClient | None = None

def build_ssid() -> str:
    return f'42["auth",{{"session":"{SESSION_TOKEN}","isDemo":{1 if IS_DEMO else 0},"uid":{UID},"platform":1}}]'

async def on_candle_event(data) -> None:
    """Called by pocketoptionapi-async on every candle update."""
    try:
        # data may be a Candle object or dict depending on version
        if hasattr(data, "asset"):
            symbol = data.asset.upper()
            price  = float(data.close)
            ts     = int(data.time.timestamp()) if hasattr(data.time, "timestamp") else int(data.time)
        elif isinstance(data, dict):
            symbol = str(data.get("asset", data.get("symbol", ""))).upper()
            price  = float(data.get("close", data.get("price", 0)))
            ts     = int(data.get("time", data.get("timestamp", time.time())))
        else:
            return

        if not symbol or price <= 0:
            return

        # Publish tick to Pub/Sub
        await push_tick(symbol, price, ts)

        # Update candle buffers
        for tf in TIMEFRAMES:
            await get_buffer(symbol, tf).on_tick(price, ts)

    except Exception as exc:
        logger.warning("on_candle_event error: %s", exc)


async def preload_history() -> None:
    """Fetch historical candles for all symbols/timeframes and store in Redis."""
    logger.info("Preloading history for %s symbols...", len(SYMBOLS))
    for symbol in SYMBOLS:
        for tf in TIMEFRAMES:
            try:
                candles = await po_client.get_candles(asset=symbol, timeframe=tf, count=CANDLE_COUNT)
                if not candles:
                    logger.warning("No historical candles for %s/%ds", symbol, tf)
                    continue

                r = await get_redis()
                key = redis_key(symbol, tf)
                # Store in Redis, newest first
                async with r.pipeline(transaction=False) as pipe:
                    for c in reversed(candles):  # oldest first so newest ends at index 0
                        ts = int(c.time.timestamp()) if hasattr(c.time, "timestamp") else int(c.time)
                        payload = json.dumps({
                            "o": float(c.open), "h": float(c.high),
                            "l": float(c.low),  "c": float(c.close),
                            "v": int(getattr(c, "volume", 1)), "t": ts
                        })
                        pipe.lpush(key, payload)
                    pipe.ltrim(key, 0, 999)
                    await pipe.execute()
                logger.info("Preloaded %d candles → %s", len(candles), key)
            except Exception as exc:
                logger.error("Preload error %s/%ds: %s", symbol, tf, exc)


# ── App lifecycle ────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global po_client
    logger.info("═══ Market Data Provider (pocketoptionapi) starting ═══")
    logger.info("Mode: %s | Symbols: %s | UID: %s", "DEMO" if IS_DEMO else "REAL", SYMBOLS, UID)

    # Connect to Redis
    r = await get_redis()
    await r.ping()
    logger.info("Redis connected ✓")

    # Build SSID and connect to PocketOption
    ssid = build_ssid()
    logger.info("SSID preview: %s", ssid[:70])

    po_client = AsyncPocketOptionClient(
        ssid=ssid,
        is_demo=IS_DEMO,
        auto_reconnect=True,
        enable_logging=True,   # verbose — we need to see what happens
    )
    po_client.add_event_callback("candles", on_candle_event)

    try:
        connected = await po_client.connect()
        if not connected:
            logger.error("PocketOption connect() returned False — check SSID and network")
            # Don't crash — keep the app alive so Railway doesn't restart loop
        else:
            logger.info("PocketOption connected ✓ (demo=%s)", IS_DEMO)
            await preload_history()
            logger.info("History preloaded ✓ — streaming live ticks now")
    except Exception as exc:
        logger.exception("PocketOption connection failed: %s", exc)

    yield

    # Shutdown
    if po_client:
        try:
            await po_client.disconnect()
        except Exception:
            pass
    if redis_pool:
        await redis_pool.aclose()
    logger.info("Shutdown complete.")



app = FastAPI(title="PO Market Data Provider", lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok", "demo": IS_DEMO, "symbols": SYMBOLS}

@app.get("/candles/{symbol}/{timeframe}")
async def get_candles_endpoint(symbol: str, timeframe: int, count: int = 100):
    """Get stored candles from Redis. timeframe in seconds (60=1m, 300=5m)."""
    r = await get_redis()
    key = redis_key(symbol, timeframe)
    raw = await r.lrange(key, 0, count - 1)
    return {"symbol": symbol.upper(), "timeframe": timeframe, "candles": [json.loads(c) for c in raw]}
