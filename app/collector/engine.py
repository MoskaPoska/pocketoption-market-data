"""
Direct Low-Latency Engine.IO / Socket.IO Collector
====================================================
Connects to the source WebSocket with cookies from SessionManager,
parses frames via EngineIOParser, and publishes quotes to Redis.

Connection lifecycle:
  connect → receive loop ──┐
      ↑                    │ disconnect / error
      └── reconnect ←──────┘
          (exponential backoff + session reload on auth failure)
"""

import asyncio
import logging
import random
import time
from typing import Optional

import aiohttp

from app.config import settings
from app.collector.parser import EngineIOParser, Quote
from app.broker.redis_client import RedisClient
from app.session.manager import SessionManager

logger = logging.getLogger(__name__)

# HTTP status codes that indicate auth failure (trigger session reload)
AUTH_FAILURE_CODES = {401, 403, 407}


class DirectCollector:
    """
    Async WebSocket collector.  Runs as a background asyncio Task.

    Usage:
        collector = DirectCollector(session_manager, redis_client)
        task = asyncio.create_task(collector.start())
        # ...
        await collector.stop()
    """

    def __init__(
        self,
        session_manager: SessionManager,
        redis_client: RedisClient,
    ) -> None:
        self._session = session_manager
        self._redis = redis_client
        self._parser = EngineIOParser()

        self._running: bool = False
        self._connected: bool = False
        self._reconnect_attempt: int = 0

        # Current WebSocket (kept for graceful close)
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None

    # ── Public API ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Main collector loop.  Connects, receives, reconnects.
        Runs indefinitely until stop() is called.
        """
        self._running = True
        logger.info("Collector starting → %s", settings.SOURCE_WS_URL)

        while self._running:
            try:
                await self._connect_and_run()

            except asyncio.CancelledError:
                break

            except Exception as exc:
                logger.error("Collector loop error: %s", exc, exc_info=True)

            if not self._running:
                break

            delay = self._backoff()
            logger.info(
                "Reconnecting in %.1fs … (attempt #%d)",
                delay,
                self._reconnect_attempt,
            )
            await asyncio.sleep(delay)

    async def stop(self) -> None:
        """Gracefully stop the collector."""
        logger.info("Collector stopping …")
        self._running = False
        self._connected = False

        if self._ws and not self._ws.closed:
            await self._ws.close()

        logger.info("Collector stopped.")

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ── Connection ────────────────────────────────────────────────────────────

    async def _connect_and_run(self) -> None:
        """Open the WebSocket, run heartbeat + receive loop."""
        headers = self._build_headers()

        # aiohttp ClientSession is lightweight; one per connection attempt is fine
        connector = aiohttp.TCPConnector(
            ttl_dns_cache=300,
            limit=1,
        )

        async with aiohttp.ClientSession(connector=connector) as http:
            try:
                async with http.ws_connect(
                    settings.SOURCE_WS_URL,
                    headers=headers,
                    heartbeat=None,          # We handle Engine.IO ping manually
                    max_msg_size=0,          # No limit
                    compress=False,          # Disable per-message deflate for min latency
                    autoclose=True,
                    autoping=False,          # Disable aiohttp-level PING (not EIO)
                ) as ws:
                    self._ws = ws
                    self._connected = True
                    self._reconnect_attempt = 0   # reset on successful connect

                    logger.info("WebSocket connected ✓")

                    # NOTE: EIO v4 — server initiates pings, not the client.
                    # We only respond to server PINGs with PONG (handled in _on_text).
                    try:
                        await self._receive_loop(ws)
                    finally:
                        self._connected = False

            except aiohttp.WSServerHandshakeError as exc:
                self._reconnect_attempt += 1
                if exc.status in AUTH_FAILURE_CODES:
                    logger.error(
                        "Auth failure (HTTP %d). Reloading session …", exc.status
                    )
                    self._session.reload()
                else:
                    logger.error("WS handshake error: HTTP %d", exc.status)
                raise

            except aiohttp.ClientConnectorError as exc:
                self._reconnect_attempt += 1
                logger.error("Connection refused: %s", exc)
                raise

    # ── Receive loop ──────────────────────────────────────────────────────────

    async def _receive_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Read frames from WebSocket until closed or stopped."""
        async for msg in ws:
            if not self._running:
                break

            recv_ts = time.monotonic()      # mark arrival time for latency

            match msg.type:
                case aiohttp.WSMsgType.TEXT:
                    await self._on_text(msg.data, ws, recv_ts)

                case aiohttp.WSMsgType.BINARY:
                    await self._on_binary(msg.data, recv_ts)

                case aiohttp.WSMsgType.CLOSE:
                    logger.warning("Server closed WS — code=%s", ws.close_code)
                    self._reconnect_attempt += 1
                    break

                case aiohttp.WSMsgType.ERROR:
                    logger.error("WS error: %s", ws.exception())
                    self._reconnect_attempt += 1
                    break

    # ── Frame handlers ────────────────────────────────────────────────────────

    async def _on_text(
        self,
        data: str,
        ws: aiohttp.ClientWebSocketResponse,
        recv_ts: float,
    ) -> None:
        logger.debug("TEXT ← %s", data[:200])

        action, quotes = self._parser.parse_text_frame(data)

        if action == "ping":
            await ws.send_str("3")
            logger.debug("PONG →")

        elif action == "open":
            await ws.send_str("40")
            logger.info("Sent SIO CONNECT (40) →")

        elif action == "message" and data.startswith("40"):
            await self._send_auth_and_subscribe(ws)

        elif action == "close":
            self._reconnect_attempt += 1
            return

        for quote in quotes:
            await self._publish(quote, recv_ts)

        if '["user_ready"' in data:
            await self._on_user_ready(ws)

    async def _on_binary(self, data: bytes, recv_ts: float) -> None:
        quote = self._parser.parse_binary_frame(data)
        if quote is not None:
            await self._publish(quote, recv_ts)

    async def _publish(self, quote: Quote, recv_ts: float) -> None:
        """Measure latency and publish to Redis."""
        latency_ms = round((time.monotonic() - recv_ts) * 1000, 3)
        await self._redis.publish_quote(quote, latency_ms)
        logger.debug(
            "Published %s @ %.5f | latency=%.3fms", quote.symbol, quote.price, latency_ms
        )

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    async def _send_auth_and_subscribe(
        self, ws: aiohttp.ClientWebSocketResponse
    ) -> None:
        """
        Step 1 of auth sequence: send user_init.
        Step 2 (chat_room_list) is sent in _on_user_ready(),
        which fires when the server responds with user_ready.
        This mirrors exact browser behavior from DevTools.
        """
        import json

        auth_payload = json.dumps(
            ["user_init", {"id": settings.SOCKET_USER_ID, "secret": settings.SOCKET_SECRET}],
            separators=(",", ":"),
        )
        await ws.send_str(f"42{auth_payload}")
        logger.info("Sent user_init (auth) → user_id=%d", settings.SOCKET_USER_ID)
        # chat_room_list will be sent by _on_user_ready() after server acks

    async def _on_user_ready(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Step 2: subscribe to trading room list after user_ready is received."""
        await ws.send_str('42["chat_room_list"]')
        logger.info("Sent chat_room_list (subscribe) →")

        # Step 3: subscribe to a specific symbol for continuous price ticks.
        # This mirrors the browser's `changeSymbol` event, which triggers
        # the server to start sending `updateStream` binary frames for the asset.
        symbol = settings.SUBSCRIBE_SYMBOL
        if symbol:
            import json as _json
            payload = _json.dumps(
                ["changeSymbol", {
                    "asset":    symbol,
                    "isDemo":   settings.SUBSCRIBE_IS_DEMO,
                    "openType": "binary",
                    "period":   60,  # m1 = 60 seconds
                }],
                separators=(",", ":"),
            )
            await ws.send_str(f"42{payload}")
            logger.info("Sent changeSymbol → %s (demo=%d)", symbol, settings.SUBSCRIBE_IS_DEMO)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _build_headers(self) -> dict[str, str]:
        """Construct WebSocket upgrade headers with session cookies."""
        return {
            "Cookie": self._session.get_cookie_header(),
            "Origin": settings.SOURCE_ORIGIN,
            # Mimic a real Chrome browser to satisfy Cloudflare checks
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

    def _backoff(self) -> float:
        """
        Exponential backoff with uniform jitter.
        delay = min(base * 2^attempt, max) + U(0, jitter)
        """
        raw = settings.RECONNECT_BASE_DELAY * (2 ** self._reconnect_attempt)
        clamped = min(raw, settings.RECONNECT_MAX_DELAY)
        jitter = random.uniform(0, settings.RECONNECT_MAX_JITTER)
        return clamped + jitter
