"""
EventsCollector — Real-time Price Feed from events-po.com
===========================================================
Connects to wss://events-po.com/socket.io/?EIO=4&transport=websocket
which streams updateStream binary frames every ~0.5 seconds for all
assets visible in the PocketOption trading interface.

Auth: via session cookies (no explicit user_init message needed).
Binary format: 39 bytes per frame (format TBD — logged for discovery).
"""

import asyncio
import json
import logging
import struct
import time
from typing import Optional

import aiohttp

from app.config import settings
from app.collector.parser import Quote
from app.broker.redis_client import RedisClient
from app.session.manager import SessionManager

logger = logging.getLogger(__name__)


class EventsCollector:
    """
    Secondary WebSocket collector for the events-po.com real-time price feed.

    Runs in parallel with DirectCollector (chat-po.site).
    Provides continuous updateStream price ticks at ~0.5s intervals.
    """

    def __init__(self, session_manager: SessionManager, redis_client: RedisClient) -> None:
        self._session = session_manager
        self._redis = redis_client
        self._running = False
        self._reconnect_attempt = 0

        # EIO binary event state
        self._pending_event: Optional[str] = None
        self._pending_count: int = 0
        self._pending_attachments: list[bytes] = []

    async def start(self) -> None:
        self._running = True
        logger.info("EventsCollector starting → %s", settings.EVENTS_WS_URL)

        while self._running:
            try:
                await self._connect_and_run()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._reconnect_attempt += 1
                logger.error("EventsCollector error (attempt #%d): %s", self._reconnect_attempt, exc)

            if not self._running:
                break

            delay = min(2 ** min(self._reconnect_attempt, 5), 30)
            logger.info("EventsCollector reconnecting in %.0fs …", delay)
            await asyncio.sleep(delay)

    async def stop(self) -> None:
        self._running = False
        logger.info("EventsCollector stopped.")

    async def _connect_and_run(self) -> None:
        cookies = self._session.get_cookie_header()
        headers = {
            "Origin":     settings.EVENTS_WS_ORIGIN,
            "Host":       "events-po.com",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/151.0.0.0 Safari/537.36"
            ),
        }
        if cookies:
            headers["Cookie"] = cookies

        connector = aiohttp.TCPConnector(ttl_dns_cache=300, limit=1)
        async with aiohttp.ClientSession(connector=connector) as http:
            async with http.ws_connect(
                settings.EVENTS_WS_URL,
                headers=headers,
                heartbeat=None,
                max_msg_size=0,
                compress=False,
                autoclose=True,
                autoping=False,
            ) as ws:
                self._reconnect_attempt = 0
                logger.info("EventsCollector connected ✓")
                await self._receive_loop(ws)

    async def _receive_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async for msg in ws:
            if not self._running:
                break
            recv_ts = time.monotonic()

            if msg.type == aiohttp.WSMsgType.TEXT:
                await self._on_text(msg.data, ws, recv_ts)
            elif msg.type == aiohttp.WSMsgType.BINARY:
                await self._on_binary(msg.data, recv_ts)
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                logger.warning("EventsCollector WS closed/error")
                break

    async def _on_text(self, data: str, ws: aiohttp.ClientWebSocketResponse, recv_ts: float) -> None:
        logger.debug("EVENTS TEXT ← %s", data[:200])

        if not data:
            return

        eio_type = data[0]

        if eio_type == "2":           # server ping
            await ws.send_str("3")
        elif eio_type == "0":         # handshake
            try:
                cfg = json.loads(data[1:])
                logger.info("EventsCollector EIO handshake — sid=%s", cfg.get("sid"))
            except Exception:
                pass
            await ws.send_str("40")   # SIO CONNECT
        elif eio_type == "4":
            sio = data[1:]
            if sio.startswith("0"):   # SIO CONNECT ack
                logger.info("EventsCollector SIO connected")
                # Try session token auth (same as demo-api server)
                if settings.SOCKET_SECRET:
                    auth = json.dumps(
                        ["auth", {
                            "sessionToken": settings.SOCKET_SECRET,
                            "uid": str(settings.SOCKET_USER_ID),
                            "lang": "ru",
                            "currentUrl": "cabinet/demo-quick-high-low",
                            "isChart": 1,
                        }],
                        separators=(",", ":"),
                    )
                    await ws.send_str(f"42{auth}")
                    logger.info("EventsCollector sent auth →")
            elif sio.startswith("5"):  # binary event announcement
                self._handle_binary_announcement(sio)
            else:
                logger.debug("EVENTS SIO: %s", sio[:100])

    def _handle_binary_announcement(self, data: str) -> None:
        try:
            dash_idx = data.index("-", 1)
            num = int(data[1:dash_idx])
            packet = json.loads(data[dash_idx + 1:])
            self._pending_event = packet[0] if packet else None
            self._pending_count = num
            self._pending_attachments = []
        except Exception as exc:
            logger.debug("Binary announcement parse error: %s", exc)

    async def _on_binary(self, data: bytes, recv_ts: float) -> None:
        self._pending_attachments.append(data)

        if len(self._pending_attachments) < self._pending_count:
            return

        event_name = self._pending_event
        attachments = self._pending_attachments[:]
        self._pending_event = None
        self._pending_count = 0
        self._pending_attachments = []

        if event_name != "updateStream":
            logger.debug("EVENTS binary event '%s' — skipped", event_name)
            return

        for raw in attachments:
            quote = self._decode_update_stream(raw)
            if quote:
                latency_ms = round((time.monotonic() - recv_ts) * 1000, 3)
                await self._redis.publish_quote(quote, latency_ms)
                logger.debug("EVENTS published %s @ %.5f", quote.symbol, quote.price)

    def _decode_update_stream(self, raw: bytes) -> Optional[Quote]:
        """
        Decode 39-byte updateStream binary payload from events-po.com.

        Format is being discovered. We try multiple decodings and log
        the raw hex at INFO level so Railway logs capture it for analysis.
        """
        now_ts = int(time.time())

        # Phase 1: log raw hex at INFO for format discovery
        logger.info(
            "[EVENTS BINARY] len=%d hex=%s",
            len(raw), raw.hex(),
        )

        # Attempt 1: msgpack
        try:
            import msgpack
            payload = msgpack.unpackb(raw, raw=False)
            logger.info("[EVENTS MSGPACK] %s", payload)
            if isinstance(payload, dict) and "symbol" in payload:
                return Quote(
                    symbol=str(payload["symbol"]),
                    price=float(payload.get("price", payload.get("value", 0))),
                    timestamp=int(payload.get("timestamp", now_ts)),
                )
        except Exception:
            pass

        # Attempt 2: 2-byte sym_len + sym_bytes + 8-byte double price + 8-byte int64 ts
        if len(raw) >= 18:
            try:
                sym_len = struct.unpack_from(">H", raw, 0)[0]
                if 2 <= sym_len <= 30 and len(raw) >= 2 + sym_len + 16:
                    sym = raw[2:2 + sym_len].decode("ascii")
                    price = struct.unpack_from(">d", raw, 2 + sym_len)[0]
                    ts = struct.unpack_from(">q", raw, 2 + sym_len + 8)[0]
                    if ts > 2_000_000_000:
                        ts //= 1000
                    if 0 < price < 1_000_000 and sym.replace("_", "").isalnum():
                        logger.info("[EVENTS STRUCT] symbol=%s price=%s", sym, price)
                        return Quote(symbol=sym, price=price, timestamp=ts)
            except Exception:
                pass

        # Attempt 3: little-endian variants
        if len(raw) >= 18:
            try:
                sym_len = struct.unpack_from("<H", raw, 0)[0]
                if 2 <= sym_len <= 30 and len(raw) >= 2 + sym_len + 16:
                    sym = raw[2:2 + sym_len].decode("ascii")
                    price = struct.unpack_from("<d", raw, 2 + sym_len)[0]
                    ts = struct.unpack_from("<q", raw, 2 + sym_len + 8)[0]
                    if ts > 2_000_000_000:
                        ts //= 1000
                    if 0 < price < 1_000_000 and sym.replace("_", "").isalnum():
                        logger.info("[EVENTS STRUCT LE] symbol=%s price=%s", sym, price)
                        return Quote(symbol=sym, price=price, timestamp=ts)
            except Exception:
                pass

        return None
