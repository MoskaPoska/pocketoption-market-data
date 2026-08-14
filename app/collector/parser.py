"""
Stateless Quote Decoder for Socket.IO Events
===========================================

Decodes SIO text and binary events into Quote objects.
"""

import json
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class Quote:
    symbol: str
    price: float
    timestamp: int          # Unix milliseconds


class QuoteDecoder:
    """
    Stateless decoder for Socket.IO payloads.
    Receives pre-parsed event names and payloads/attachments from SocketIOClient.
    """

    def __init__(self) -> None:
        self._successful_decoder: Optional[str] = None

    def decode_text_event(self, event_name: str, payload: any) -> list[Quote]:
        """Decode a regular Socket.IO text event."""
        match event_name:
            case "updateStream":
                q = self._extract_quote(payload)
                return [q] if q else []

            case "successauth":
                # auth handled by EventsCollector directly
                return []

            case _:
                logger.debug("SIO text event '%s' — ignored", event_name)
                return []

    def decode_binary_event(
        self, event_name: str, attachments: list[bytes]
    ) -> Optional[Quote]:
        """
        Decode binary Socket.IO attachment for `updateStream`.
        """
        import struct
        import time as _time

        for idx, raw in enumerate(attachments):
            logger.debug(
                "[BINARY] event='%s' len=%d hex=%s",
                event_name, len(raw), raw[:64].hex(),
            )

            # ── Attempt 1: MessagePack ────────────────────────────────────
            if self._successful_decoder in (None, "msgpack"):
                try:
                    import msgpack
                    payload = msgpack.unpackb(raw, raw=False)
                    logger.debug("[BINARY→MSGPACK] %s", payload)
                    # PO sends binary as list: ["updateStream", {asset, at, time, dir}]
                    if isinstance(payload, list) and len(payload) >= 2:
                        event_in_payload = payload[0]
                        data = payload[1]
                        if event_in_payload == "updateStream" and isinstance(data, dict):
                            q = self._extract_quote(data)
                            if q:
                                self._successful_decoder = "msgpack"
                                return q
                    # Also try dict directly
                    if isinstance(payload, dict):
                        q = self._extract_quote(payload)
                        if q:
                            self._successful_decoder = "msgpack"
                            return q
                except Exception:
                    pass

            # ── Attempt 2: UTF-8 JSON ─────────────────────────────────────
            if self._successful_decoder in (None, "json"):
                try:
                    text = raw.decode("utf-8")
                    payload = json.loads(text)
                    logger.debug("[BINARY→JSON] %s", payload)
                    if isinstance(payload, dict) and "symbol" in payload:
                        q = self._extract_quote(payload)
                        if q:
                            self._successful_decoder = "json"
                            return q
                except (UnicodeDecodeError, json.JSONDecodeError):
                    pass

            # ── Attempt 3: custom struct — try common layouts ─────────────
            if self._successful_decoder in (None, "struct_be"):
                if len(raw) >= 18:
                    try:
                        sym_len = struct.unpack_from(">H", raw, 0)[0]
                        if 2 <= sym_len <= 30 and len(raw) >= 2 + sym_len + 16:
                            sym = raw[2:2 + sym_len].decode("ascii")
                            price = struct.unpack_from(">d", raw, 2 + sym_len)[0]
                            ts = struct.unpack_from(">q", raw, 2 + sym_len + 8)[0]
                            if ts > 2_000_000_000:
                                ts = ts // 1000
                            if 0 < price < 1_000_000 and sym.replace("_", "").isalnum() and abs(ts - _time.time()) < 300:
                                logger.info("Signal extracted via struct BE: symbol=%s price=%s", sym, price)
                                self._successful_decoder = "struct_be"
                                return Quote(symbol=sym, price=price, timestamp=ts)
                    except Exception:
                        pass
                        
            if self._successful_decoder in (None, "struct_le"):
                if len(raw) >= 18:
                    try:
                        sym_len = struct.unpack_from("<H", raw, 0)[0]
                        if 2 <= sym_len <= 30 and len(raw) >= 2 + sym_len + 16:
                            sym = raw[2:2 + sym_len].decode("ascii")
                            price = struct.unpack_from("<d", raw, 2 + sym_len)[0]
                            ts = struct.unpack_from("<q", raw, 2 + sym_len + 8)[0]
                            if ts > 2_000_000_000:
                                ts = ts // 1000
                            if 0 < price < 1_000_000 and sym.replace("_", "").isalnum() and abs(ts - _time.time()) < 300:
                                logger.info("Signal extracted via struct LE: symbol=%s price=%s", sym, price)
                                self._successful_decoder = "struct_le"
                                return Quote(symbol=sym, price=price, timestamp=ts)
                    except Exception:
                        pass

            # Log raw bytes for manual format discovery
            if event_name in ("updateStream", "chafor"):
                logger.info(
                    "[BINARY UNDECODABLE] event='%s' len=%d hex=%s repr=%r",
                    event_name, len(raw), raw[:80].hex(), raw[:80],
                )

        return None

    def _extract_quote(self, payload: dict) -> Optional[Quote]:
        """Extract Quote from PO's updateStream payload.
        
        PO field names: 'asset' (symbol), 'at' (price), 'time' (unix ts), 'dir' (direction)
        Fallback to legacy: 'symbol', 'price', 'timestamp'
        """
        import time
        try:
            # Symbol: PO uses 'asset', fallback to 'symbol' or 'active'
            symbol = (
                payload.get("asset")
                or payload.get("symbol")
                or str(payload.get("active", ""))
            )
            if not symbol:
                return None

            # Price: PO uses 'at', fallback to 'price', 'value', 'p'
            price = (
                payload.get("at")
                or payload.get("price")
                or payload.get("value")
                or payload.get("p")
            )
            if price is None:
                return None

            # Timestamp: PO uses 'time', fallback to 'timestamp'
            ts = int(payload.get("time") or payload.get("timestamp") or 0)
            if ts <= 0:
                ts = int(time.time())
            elif ts > 2_000_000_000:
                ts = ts // 1000

            if abs(ts - time.time()) > 300:
                logger.warning("Quote timestamp %d out of bounds, dropping.", ts)
                return None

            q = Quote(symbol=str(symbol), price=float(price), timestamp=ts)
            logger.info("Quote decoded: %s @ %.5f (ts=%d)", q.symbol, q.price, q.timestamp)
            return q

        except (KeyError, TypeError, ValueError) as exc:
            logger.warning(
                "Quote extraction failed: %s | payload=%s", exc, str(payload)[:200]
            )
            return None
