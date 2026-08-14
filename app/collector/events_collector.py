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

    async def on_sio_connect(self) -> None:
        """Triggered when Socket.IO is connected. Send auth and subscribe."""
        cookies = self.session.get_cookies_dict()
        session_id = cookies.get("ci_session", "")
        uid = cookies.get("po_uuid", "")

        auth_payload = json.dumps(
            [
                "auth",
                {
                    "session": session_id,
                    "isDemo": 1 if settings.SUBSCRIBE_IS_DEMO else 0,
                    "uid": uid,
                    "platform": 1,
                    "isFastHistory": True,
                    "isOptimized": True
                }
            ],
            separators=(",", ":")
        )
        if self.ws:
            await self.ws.send_str(f"42{auth_payload}")
            logger.info("%s sent auth → uid=%s", self.name, uid)
            # NOTE: subfor + changeSymbol are sent ONLY after receiving auth/success

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
        if event == "successauth":
            logger.info("%s auth success! Subscribing to %s …", self.name, self.symbol)
            await self._subscribe()
        elif event == "updateBalance":
            logger.debug("%s balance update received", self.name)
        else:
            quotes = self._decoder.decode_text_event(event, payload)
            for quote in quotes:
                await self._publish(quote, recv_ts)

    async def _subscribe(self) -> None:
        """Send subfor + changeSymbol AFTER auth/success is confirmed."""
        if not self.ws or self.ws.closed:
            return
        subfor_msg = json.dumps(["subfor", self.symbol], separators=(",", ":"))
        await self.ws.send_str(f"42{subfor_msg}")

        sub_msg = json.dumps(
            ["changeSymbol", {"asset": self.symbol, "period": 5}],
            separators=(",", ":")
        )
        await self.ws.send_str(f"42{sub_msg}")
        logger.info("%s sent subfor + changeSymbol → %s", self.name, self.symbol)

        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def on_sio_binary_event(self, event: str, attachments: list[bytes], recv_ts: float) -> None:
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
