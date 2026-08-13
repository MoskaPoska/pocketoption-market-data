import asyncio
import json
import logging
import random
import time
from typing import Optional

import aiohttp

from app.config import settings
from app.session.manager import SessionManager

logger = logging.getLogger(__name__)


class BaseWebSocketClient:
    """
    Abstract Base Client for WebSockets.
    Handles connection, reconnection, exponential backoff, and session invalidation.
    """
    def __init__(self, url: str, origin: str, session_manager: SessionManager):
        self.url = url
        self.origin = origin
        self.session = session_manager
        self._running = False
        self._reconnect_attempt = 0
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.name = self.__class__.__name__

    @property
    def is_connected(self) -> bool:
        return self.ws is not None and not self.ws.closed

    async def start(self) -> None:
        self._running = True
        logger.info("%s starting → %s", self.name, self.url)
        while self._running:
            try:
                await self._connect_and_run()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("%s loop error: %s", self.name, exc, exc_info=True)
            
            if not self._running:
                break
            
            delay = self._backoff()
            logger.info("%s reconnecting in %.1fs … (attempt #%d)", self.name, delay, self._reconnect_attempt)
            await asyncio.sleep(delay)

    async def stop(self) -> None:
        self._running = False
        if self.ws and not self.ws.closed:
            await self.ws.close()
        logger.info("%s stopped.", self.name)

    async def _connect_and_run(self) -> None:
        cookies = self.session.get_cookie_header()
        headers = {
            "Origin": self.origin,
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
        }
        if cookies:
            headers["Cookie"] = cookies

        connector = aiohttp.TCPConnector(ttl_dns_cache=300, limit=1)
        async with aiohttp.ClientSession(connector=connector) as http:
            try:
                async with http.ws_connect(
                    self.url,
                    headers=headers,
                    heartbeat=None,
                    receive_timeout=30.0,
                    max_msg_size=0,
                    compress=False,
                    autoclose=True,
                    autoping=False,
                ) as ws:
                    self.ws = ws
                    self._reconnect_attempt = 0
                    logger.info("%s WebSocket connected ✓", self.name)
                    await self._receive_loop(ws)
            except aiohttp.WSServerHandshakeError as exc:
                self._reconnect_attempt += 1
                if exc.status in {401, 403, 502}:
                    logger.error("%s auth failure (HTTP %d). Reloading session …", self.name, exc.status)
                    try:
                        await self.session.reload()
                    except Exception as reload_exc:
                        logger.error("%s session reload failed: %s", self.name, reload_exc)
                else:
                    logger.error("%s WS handshake error: HTTP %d", self.name, exc.status)
                raise
            except aiohttp.ClientConnectorError as exc:
                self._reconnect_attempt += 1
                logger.error("%s Connection refused: %s", self.name, exc)
                raise
            except asyncio.TimeoutError:
                self._reconnect_attempt += 1
                logger.error("%s Connection timed out (no data for 30s)", self.name)
                raise
            finally:
                self.ws = None

    async def _receive_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async for msg in ws:
            if not self._running:
                break
            recv_ts = time.monotonic()
            if msg.type == aiohttp.WSMsgType.TEXT:
                await self.on_ws_text(msg.data, recv_ts)
            elif msg.type == aiohttp.WSMsgType.BINARY:
                await self.on_ws_binary(msg.data, recv_ts)
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                logger.warning("%s server closed WS or error", self.name)
                self._reconnect_attempt += 1
                break

    async def on_ws_text(self, data: str, recv_ts: float) -> None:
        """To be implemented by subclasses"""
        pass

    async def on_ws_binary(self, data: bytes, recv_ts: float) -> None:
        """To be implemented by subclasses"""
        pass

    def _backoff(self) -> float:
        raw = settings.RECONNECT_BASE_DELAY * (2 ** self._reconnect_attempt)
        clamped = min(raw, settings.RECONNECT_MAX_DELAY)
        return clamped + random.uniform(0, settings.RECONNECT_MAX_JITTER)


class SocketIOClient(BaseWebSocketClient):
    """
    Socket.IO / Engine.IO Protocol Client.
    Handles handshake, ping/pong, and multi-frame binary event buffering.
    """
    def __init__(self, url: str, origin: str, session_manager: SessionManager):
        super().__init__(url, origin, session_manager)
        self._pending_event_name: Optional[str] = None
        self._pending_count: int = 0
        self._pending_attachments: list[bytes] = []

    async def on_ws_text(self, data: str, recv_ts: float) -> None:
        if not data:
            return
        eio_type = data[0]

        if eio_type == "0":
            try:
                cfg = json.loads(data[1:])
                logger.info("%s EIO handshake — sid=%s", self.name, cfg.get("sid"))
            except Exception:
                pass
            if self.ws:
                await self.ws.send_str("40")
                logger.debug("%s Sent SIO CONNECT (40) →", self.name)

        elif eio_type == "2":
            if self.ws:
                await self.ws.send_str("3")
                logger.debug("%s PONG →", self.name)

        elif eio_type == "4":
            sio_payload = data[1:]
            if not sio_payload:
                return
            sio_type = sio_payload[0]
            
            if sio_type == "0":
                logger.info("%s SIO connected ✓", self.name)
                await self.on_sio_connect()
            elif sio_type == "2":
                try:
                    packet = json.loads(sio_payload[1:])
                    if isinstance(packet, list) and len(packet) > 0:
                        event_name = packet[0]
                        payload = packet[1] if len(packet) > 1 else None
                        await self.on_sio_text_event(event_name, payload, recv_ts)
                except Exception as exc:
                    logger.error("%s SIO text parse error: %s | data=%s", self.name, exc, data[:100])
            elif sio_type == "5":
                self._handle_binary_announcement(sio_payload)

    async def on_ws_binary(self, data: bytes, recv_ts: float) -> None:
        if self._pending_event_name is None:
            return
        self._pending_attachments.append(data)
        if len(self._pending_attachments) < self._pending_count:
            return
        
        event_name = self._pending_event_name
        attachments = self._pending_attachments[:]
        self._pending_event_name = None
        self._pending_count = 0
        self._pending_attachments = []

        await self.on_sio_binary_event(event_name, attachments, recv_ts)

    def _handle_binary_announcement(self, sio_payload: str) -> None:
        try:
            dash_idx = sio_payload.index("-", 1)
            num_attachments = int(sio_payload[1:dash_idx])
            json_str = sio_payload[dash_idx + 1:]
            packet = json.loads(json_str)
            if isinstance(packet, list) and len(packet) > 0:
                self._pending_event_name = packet[0]
                self._pending_count = num_attachments
                self._pending_attachments = []
        except Exception as exc:
            logger.error("%s Binary announcement error: %s | data=%s", self.name, exc, sio_payload[:100])

    async def on_sio_connect(self) -> None:
        """Triggered when Socket.IO connection is established."""
        pass

    async def on_sio_text_event(self, event: str, payload: any, recv_ts: float) -> None:
        """Triggered on text event e.g. 42['eventName', payload]"""
        pass

    async def on_sio_binary_event(self, event: str, attachments: list[bytes], recv_ts: float) -> None:
        """Triggered on binary event e.g. 451-['eventName'] + raw binary WS frames"""
        pass
