"""
Redis Pub/Sub Broker
=====================
Single connection pool shared by Publisher (collector) and
Subscriber (gateway).  Each subscriber gets its own pubsub handle.
"""

import json
import logging
import time
from typing import AsyncGenerator

import redis.asyncio as aioredis

from app.config import settings
from app.collector.parser import Quote

logger = logging.getLogger(__name__)

CHANNEL_PREFIX = "quotes"
ACTIVE_SYMBOLS_KEY = "mdp:active_symbols"


class RedisClient:
    """
    Async Redis client wrapper.

    Publisher side  (collector):
        await redis.publish_quote(quote, latency_ms)

    Subscriber side (gateway):
        async for msg in redis.subscribe("EURUSD_otc"):
            await websocket.send_json(msg)
    """

    def __init__(self) -> None:
        self._pool: aioredis.Redis | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        self._pool = aioredis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,      # auto-decode bytes → str
            socket_connect_timeout=5,
            socket_keepalive=True,
            health_check_interval=30,
            max_connections=20,
        )
        await self._pool.ping()
        logger.info("Redis connected ✓  url=%s", settings.REDIS_URL)

    async def close(self) -> None:
        if self._pool:
            await self._pool.aclose()
            logger.info("Redis connection closed.")

    # ── Publisher ─────────────────────────────────────────────────────────────

    async def publish_quote(self, quote: Quote, latency_ms: float) -> None:
        """
        Publish one quote to Redis channel  quotes:{symbol}
        and track the symbol in the active-symbols SET.

        Message payload:
            {"symbol": str, "price": float, "timestamp": int,
             "latency_ms": float, "server_time": int}
        """
        channel = f"{CHANNEL_PREFIX}:{quote.symbol}"
        message = json.dumps(
            {
                "symbol": quote.symbol,
                "price": quote.price,
                "timestamp": quote.timestamp,
                "latency_ms": latency_ms,
                "server_time": int(time.time() * 1000),  # UTC ms
            },
            # compact JSON — saves a few bytes on every message
            separators=(",", ":"),
        )

        # Pipeline → single round-trip for PUBLISH + SADD
        async with self._pool.pipeline(transaction=False) as pipe:
            pipe.publish(channel, message)
            pipe.sadd(ACTIVE_SYMBOLS_KEY, quote.symbol)
            await pipe.execute()

    # ── Subscriber ────────────────────────────────────────────────────────────

    async def subscribe(self, symbol: str) -> AsyncGenerator[dict, None]:
        """
        Subscribe to quotes:{symbol} and yield parsed quote dicts.

        Each call creates a dedicated pubsub handle so concurrent
        gateway connections don't share state.
        """
        channel = f"{CHANNEL_PREFIX}:{symbol}"

        async with self._pool.pubsub() as pubsub:
            await pubsub.subscribe(channel)
            logger.debug("Subscribed → %s", channel)

            try:
                async for raw in pubsub.listen():
                    if raw["type"] != "message":
                        continue        # skip subscribe/unsubscribe confirmations
                    try:
                        yield json.loads(raw["data"])
                    except json.JSONDecodeError as exc:
                        logger.error("Redis decode error: %s", exc)
            finally:
                await pubsub.unsubscribe(channel)
                logger.debug("Unsubscribed ← %s", channel)

    # ── Monitoring helpers ────────────────────────────────────────────────────

    async def get_active_symbols(self) -> list[str]:
        """Return the set of symbols seen since startup."""
        return sorted(await self._pool.smembers(ACTIVE_SYMBOLS_KEY))

    async def ping(self) -> bool:
        try:
            return bool(await self._pool.ping())
        except Exception:
            return False
