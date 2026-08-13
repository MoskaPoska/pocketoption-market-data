"""
Direct Low-Latency Engine.IO / Socket.IO Collector
====================================================
Connects to chat-po.site to retrieve fallback data.
"""

import json
import logging
import time

from app.config import settings
from app.collector.client import SocketIOClient
from app.collector.parser import QuoteDecoder, Quote
from app.broker.redis_client import RedisClient
from app.session.manager import SessionManager

logger = logging.getLogger(__name__)


class DirectCollector(SocketIOClient):
    """
    Business logic layer for the chat-po.site fallback connection.
    """

    def __init__(
        self,
        session_manager: SessionManager,
        redis_client: RedisClient,
    ) -> None:
        super().__init__(
            url=settings.SOURCE_WS_URL,
            origin=settings.SOURCE_ORIGIN,
            session_manager=session_manager
        )
        self._redis = redis_client
        self._decoder = QuoteDecoder()

    async def on_sio_connect(self) -> None:
        """Triggered when Socket.IO is connected. Send initial auth."""
        auth_payload = json.dumps(
            ["user_init", {"id": settings.SOCKET_USER_ID, "secret": settings.SOCKET_SECRET}],
            separators=(",", ":"),
        )
        if self.ws:
            await self.ws.send_str(f"42{auth_payload}")
            logger.info("DirectCollector sent user_init (auth) → user_id=%s", settings.SOCKET_USER_ID)

    async def on_sio_text_event(self, event: str, payload: any, recv_ts: float) -> None:
        if event == "user_ready":
            if self.ws:
                await self.ws.send_str('42["chat_room_list"]')
                logger.info("DirectCollector sent chat_room_list (subscribe) →")
                
                symbol = settings.SUBSCRIBE_SYMBOL
                if symbol:
                    change_symbol = json.dumps(
                        ["changeSymbol", {
                            "asset":    symbol,
                            "isDemo":   settings.SUBSCRIBE_IS_DEMO,
                            "openType": "binary",
                            "period":   60,
                        }],
                        separators=(",", ":"),
                    )
                    await self.ws.send_str(f"42{change_symbol}")
                    logger.info("DirectCollector sent changeSymbol → %s", symbol)

        quotes = self._decoder.decode_text_event(event, payload)
        for quote in quotes:
            await self._publish(quote, recv_ts)

    async def on_sio_binary_event(self, event: str, attachments: list[bytes], recv_ts: float) -> None:
        quote = self._decoder.decode_binary_event(event, attachments)
        if quote:
            await self._publish(quote, recv_ts)

    async def _publish(self, quote: Quote, recv_ts: float) -> None:
        latency_ms = round((time.monotonic() - recv_ts) * 1000, 3)
        await self._redis.publish_quote(quote, latency_ms)
        logger.debug(
            "DirectCollector published %s @ %.5f | latency=%.3fms", quote.symbol, quote.price, latency_ms
        )
