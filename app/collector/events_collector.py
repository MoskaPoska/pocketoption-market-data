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
    ) -> None:
        super().__init__(
            url=settings.EVENTS_WS_URL,
            origin=settings.EVENTS_WS_ORIGIN,
            session_manager=session_manager
        )
        self.queue = quote_queue
        self._decoder = QuoteDecoder()

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
            logger.info("EventsCollector sent auth → uid=%s", uid)

            # Keep sending heartbeats
            await self.ws.send_str('42["ping-server"]')
            
            for symbol in settings.subscribe_symbols_list:
                subfor_msg = json.dumps(["subfor", symbol], separators=(",", ":"))
                await self.ws.send_str(f"42{subfor_msg}")
                
                sub_msg = json.dumps(["changeSymbol", {"asset": symbol, "period": 5}], separators=(",", ":"))
                await self.ws.send_str(f"42{sub_msg}")
                logger.info("EventsCollector sent subfor and changeSymbol → %s", symbol)

    async def on_sio_text_event(self, event: str, payload: any, recv_ts: float) -> None:
        if event == "auth/success":
            logger.info("EventsCollector auth success!")
        else:
            quotes = self._decoder.decode_text_event(event, payload)
            for quote in quotes:
                await self._publish(quote, recv_ts)

    async def on_sio_binary_event(self, event: str, attachments: list[bytes], recv_ts: float) -> None:
        quote = self._decoder.decode_binary_event(event, attachments)
        if quote:
            await self._publish(quote, recv_ts)

    async def _publish(self, quote: Quote, recv_ts: float) -> None:
        latency_ms = round((time.monotonic() - recv_ts) * 1000, 3)
        try:
            self.queue.put_nowait((quote, latency_ms))
        except asyncio.QueueFull:
            logger.warning("EventsCollector dropped quote (queue full): %s", quote.symbol)
        else:
            logger.debug(
                "EventsCollector published %s @ %.5f | latency=%.3fms", quote.symbol, quote.price, latency_ms
            )
