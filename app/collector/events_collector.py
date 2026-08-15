"""
Events Low-Latency Socket.IO Collector
======================================
Connects to demo-api-eu.po.market for high-frequency binary price feeds.
"""

import asyncio
import json
import logging
import time

from app.config import settings
from app.collector.client import SocketIOClient
from app.collector.parser import QuoteDecoder, Quote
from app.session.manager import SessionManager

logger = logging.getLogger(__name__)


class EventsCollector(SocketIOClient):
    """
    Business logic layer for the demo-api-eu.po.market high-frequency connection.
    """

    def __init__(
        self,
        session_manager: SessionManager,
        quote_queue: asyncio.Queue,
        symbol: str,
    ) -> None:
        super().__init__(
            url=settings.EVENTS_WS_URL,
            origin=settings.EVENTS_WS_ORIGIN,
            session_manager=session_manager
        )
        self.queue = quote_queue
        self.symbol = symbol
        self.name = f"EventsCollector[{symbol}]"
        self._decoder = QuoteDecoder()
        self._ping_task: Optional[asyncio.Task] = None

    async def stop(self) -> None:
        if getattr(self, '_ping_task', None) and not self._ping_task.done():
            self._ping_task.cancel()
        await super().stop()

    async def on_sio_disconnect(self) -> None:
        """Server kicked us — force session reload before next reconnect."""
        logger.warning("%s session rejected — forcing session reload", self.name)
        try:
            await self.session.reload()
            logger.info("%s session reloaded successfully", self.name)
        except Exception as exc:
            logger.error("%s session reload failed: %s", self.name, exc)

    async def on_sio_connect(self) -> None:
        """Triggered when Socket.IO is connected. Send auth.
        demo-api-eu.po.market uses a short session token.
        We extract session_id from the ci_session PHP-serialized cookie.
        """
        cookies = self.session.get_cookies_dict()
        ci_session = cookies.get("ci_session", "")
        session_token = self._parse_session_id(ci_session)
        uid = settings.SOCKET_USER_ID

        auth_payload = json.dumps(
            [
                "auth",
                {
                    "session": session_token,
                    "isDemo": 1 if settings.SUBSCRIBE_IS_DEMO else 0,
                    "uid": uid,
                    "platform": 2,
                    "isFastHistory": True,
                    "isOptimized": True,
                }
            ],
            separators=(",", ":")
        )
        if self.ws:
            await self.ws.send_str(f"42{auth_payload}")
            logger.info(
                "%s sent auth → uid=%s session='%s...' (len=%d)",
                self.name, uid, session_token[:8], len(session_token)
            )

    @staticmethod
    def _parse_session_id(ci_session: str) -> str:
        """Extract session_id from PHP-serialized ci_session cookie.

        ci_session URL-decoded format:
          a:4:{s:10:"session_id";s:32:"XXXXXXX";s:10:"ip_address";...}HMAC

        Returns the raw session_id string, or the raw ci_session as fallback.
        """
        import urllib.parse, re
        try:
            decoded = urllib.parse.unquote(ci_session)
            m = re.search(r's:10:"session_id";s:\d+:"([^"]+)"', decoded)
            if m:
                sid = m.group(1)
                logger.debug("Parsed session_id from ci_session: %s", sid)
                return sid
        except Exception as exc:
            logger.warning("Failed to parse ci_session: %s", exc)
        return ci_session

    async def _ping_loop(self) -> None:
        """Continuously send application-level pings to keep PO stream alive."""
        while self._running and self.ws and not self.ws.closed:
            try:
                if self.ws and not self.ws.closed:
                    await self.ws.send_str('42["ping-server"]')
                    logger.debug("%s sent ping-server", self.name)
                await asyncio.sleep(15.0)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("%s ping loop error: %s", self.name, exc)
                break

    async def on_sio_text_event(self, event: str, payload: any, recv_ts: float) -> None:
        # events-po.com sends text "auth/success", demo-api sends binary "successauth"
        if event in ("successauth", "auth/success"):
            logger.info("%s ← auth success ('%s')! Subscribing to %s …", self.name, event, self.symbol)
            await self._subscribe()
        elif event == "updateBalance":
            logger.debug("%s balance update received", self.name)
        else:
            quotes = self._decoder.decode_text_event(event, payload)
            for quote in quotes:
                await self._publish(quote, recv_ts)

    async def _subscribe(self) -> None:
        """Send changeSymbol + subfor AFTER successauth is confirmed.
        Browser order: changeSymbol first, then subfor.
        """
        if not self.ws or self.ws.closed:
            return
        # 1. changeSymbol first (browser does it this way)
        sub_msg = json.dumps(
            ["changeSymbol", {"asset": self.symbol, "period": 5}],
            separators=(",", ":")
        )
        await self.ws.send_str(f"42{sub_msg}")

        # 2. subfor second
        subfor_msg = json.dumps(["subfor", self.symbol], separators=(",", ":"))
        await self.ws.send_str(f"42{subfor_msg}")

        logger.info("%s sent changeSymbol + subfor → %s", self.name, self.symbol)

        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def on_sio_binary_event(self, event: str, attachments: list[bytes], recv_ts: float) -> None:
        # successauth comes as a BINARY event (451-), not text (42)
        if event == "successauth":
            logger.info("%s ← binary successauth! Subscribing to %s …", self.name, self.symbol)
            await self._subscribe()
            return
        quote = self._decoder.decode_binary_event(event, attachments)
        if quote:
            await self._publish(quote, recv_ts)

    async def _publish(self, quote: Quote, recv_ts: float) -> None:
        if quote.symbol != self.symbol:
            return
            
        latency_ms = round((time.monotonic() - recv_ts) * 1000, 3)
        try:
            self.queue.put_nowait((quote, latency_ms))
        except asyncio.QueueFull:
            logger.warning("%s dropped quote (queue full): %s", self.name, quote.symbol)
        else:
            logger.debug(
                "%s published %s @ %.5f | latency=%.3fms", self.name, quote.symbol, quote.price, latency_ms
            )
