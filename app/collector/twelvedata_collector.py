"""
Twelve Data WebSocket Collector
================================
Connects to Twelve Data's real-time WebSocket API and streams EURUSD quotes.
Publishes quotes to Redis in the same format as EventsCollector.

Free plan: https://twelvedata.com — sufficient for single-symbol real-time.

WebSocket protocol:
  Connect: wss://ws.twelvedata.com/v1/quotes/price?apikey=KEY
  Subscribe: {"action": "subscribe", "params": {"symbols": "EUR/USD"}}
  Message:  {"event": "price", "symbol": "EUR/USD", "price": 1.12345,
             "timestamp": 1700000000, ...}
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

import aiohttp

from app.collector.parser import Quote

logger = logging.getLogger(__name__)

# Map our internal symbol → Twelve Data symbol
SYMBOL_MAP = {
    "EURUSD_otc": "EUR/USD",
    "EURUSD":     "EUR/USD",
    "GBPUSD_otc": "GBP/USD",
    "GBPUSD":     "GBP/USD",
    "USDJPY_otc": "USD/JPY",
    "USDJPY":     "USD/JPY",
    "AUDUSD_otc": "AUD/USD",
    "AUDUSD":     "AUD/USD",
}

WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"


class TwelveDataCollector:
    """
    Subscribes to Twelve Data real-time WebSocket and publishes quotes to Redis.
    Drop-in replacement for EventsCollector — same queue interface.
    """

    def __init__(
        self,
        api_key: str,
        quote_queue: asyncio.Queue,
        symbol: str,
    ) -> None:
        self.api_key = api_key
        self.queue = quote_queue
        self.symbol = symbol                       # our internal symbol, e.g. EURUSD_otc
        self.td_symbol = SYMBOL_MAP.get(symbol, symbol.replace("_otc", "").replace("OTC", ""))
        self.name = f"TwelveDataCollector[{symbol}]"
        self._running = False
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None

    # ── Public API ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Connect and stream quotes. Reconnects automatically on failure."""
        self._running = True
        logger.info("%s starting → %s (td_symbol=%s)", self.name, WS_URL, self.td_symbol)

        backoff = 2.0
        while self._running:
            try:
                await self._run_connection()
                backoff = 2.0  # reset on clean exit
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("%s connection error: %s", self.name, exc)

            if not self._running:
                break
            logger.info("%s reconnecting in %.1fs …", self.name, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 60.0)

        logger.info("%s stopped.", self.name)

    async def stop(self) -> None:
        self._running = False
        if self._ws and not self._ws.closed:
            await self._ws.close()

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and not self._ws.closed

    # ── Internal ───────────────────────────────────────────────────────────────

    async def _run_connection(self) -> None:
        url = f"{WS_URL}?apikey={self.api_key}"
        timeout = aiohttp.ClientTimeout(total=None, connect=10)

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                url,
                timeout=timeout,
                heartbeat=30,
            ) as ws:
                self._ws = ws
                logger.info("%s WebSocket connected ✓", self.name)

                # Subscribe to the symbol
                subscribe_msg = json.dumps({
                    "action": "subscribe",
                    "params": {"symbols": self.td_symbol},
                })
                await ws.send_str(subscribe_msg)
                logger.info("%s subscribed to %s", self.name, self.td_symbol)

                async for msg in ws:
                    if not self._running:
                        break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._handle_message(msg.data)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        logger.warning("%s WS closed/error: %s", self.name, msg.data)
                        break

    async def _handle_message(self, raw: str) -> None:
        recv_ts = time.monotonic()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        event = data.get("event")

        if event == "subscribe-status":
            status = data.get("status")
            logger.info("%s subscribe-status: %s", self.name, status)
            return

        if event == "price":
            price = data.get("price")
            ts = data.get("timestamp")
            if price is None:
                return

            quote = Quote(
                symbol=self.symbol,
                price=float(price),
                timestamp=float(ts) if ts else time.time(),
            )
            latency_ms = round((time.monotonic() - recv_ts) * 1000, 3)
            try:
                self.queue.put_nowait((quote, latency_ms))
            except asyncio.QueueFull:
                logger.warning("%s dropped quote (queue full)", self.name)
            else:
                logger.debug(
                    "%s %s @ %.5f | latency=%.3fms",
                    self.name, self.symbol, price, latency_ms,
                )
            return

        if event == "heartbeat":
            logger.debug("%s heartbeat", self.name)
            return

        logger.debug("%s unhandled event '%s': %s", self.name, event, raw[:120])
