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

            case "user_ready":
                logger.info("SIO ← user_ready | user_id=%s", payload.get("id") if isinstance(payload, dict) else "?")
                return []

            case "chat_room_list":
                # Initial snapshot: extract ALL signals from ALL rooms
                logger.info("SIO ← chat_room_list (snapshot received)")
                quotes = []
                if isinstance(payload, dict) and "list" in payload:
                    for room in payload["list"]:
                        q = self._extract_signal_from_room(room)
                        if q:
                            quotes.append(q)
                if quotes:
                    logger.info("chat_room_list snapshot: extracted %d quotes", len(quotes))
                return quotes

            case "chat_room_list_update":
                # Real-time update for one room
                logger.debug("SIO ← chat_room_list_update")
                q = self._extract_signal_from_room(payload)
                return [q] if q else []

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
                    if isinstance(payload, dict) and "symbol" in payload:
                        q = self._extract_quote(payload)
                        if q:
                            self._successful_decoder = "msgpack"
                            return q
                    if isinstance(payload, (list, tuple)) and len(payload) >= 2:
                        sym = str(payload[0])
                        price = float(payload[1])
                        ts = int(payload[2]) if len(payload) > 2 else int(_time.time())
                        if ts > 2_000_000_000:
                            ts = ts // 1000
                        if abs(ts - _time.time()) < 300:
                            logger.info("Signal extracted via msgpack: symbol=%s price=%s", sym, price)
                            self._successful_decoder = "msgpack"
                            return Quote(symbol=sym, price=price, timestamp=ts)
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
            if event_name in ("updateStream", "updateAssets", "chafor"):
                logger.info(
                    "[BINARY UNDECODABLE] event='%s' len=%d hex=%s repr=%r",
                    event_name, len(raw), raw[:80].hex(), raw[:80],
                )

        return None

    def _extract_signal_from_room(self, room: dict | None) -> Optional[Quote]:
        if not isinstance(room, dict):
            return None

        logger.debug("chat_room payload: %s", str(room)[:400])

        try:
            content = room.get("message_content")
            if isinstance(content, str):
                import json as _json
                content = _json.loads(content)

            if isinstance(content, dict):
                signal = content.get("signal") or {}
            else:
                msg = room.get("message") or {}
                if isinstance(msg, str):
                    import json as _json
                    msg = _json.loads(msg)
                content2 = msg.get("message_content") if isinstance(msg, dict) else {}
                if isinstance(content2, str):
                    import json as _json
                    content2 = _json.loads(content2)
                signal = (content2 or {}).get("signal") or {} if isinstance(content2, dict) else {}

            if not signal:
                return None

            logger.info(
                "Signal extracted: symbol=%s price=%s",
                signal.get("symbol"), signal.get("price", signal.get("value")),
            )
            ts = int(signal.get("timestamp", signal.get("time", 0)))
            if ts <= 0:
                import time
                ts = int(time.time())
            elif ts > 2000000000:
                ts = int(ts / 1000)
                
            import time
            if abs(ts - time.time()) > 300:
                logger.warning("Signal timestamp %d out of bounds, dropping.", ts)
                return None

            return Quote(
                symbol=str(signal["symbol"]),
                price=float(signal.get("price", signal.get("value", 0))),
                timestamp=ts,
            )
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            logger.debug("Signal extraction failed: %s | room=%s", exc, str(room)[:300])
            return None

    def _extract_quote(self, payload: dict) -> Optional[Quote]:
        import time
        try:
            ts = int(payload.get("timestamp", 0))
            if ts <= 0:
                ts = int(time.time())
            elif ts > 2000000000:
                ts = int(ts / 1000)

            if abs(ts - time.time()) > 300:
                logger.warning("Quote timestamp %d out of bounds, dropping.", ts)
                return None

            return Quote(
                symbol=str(payload["symbol"]),
                price=float(payload["price"]),
                timestamp=ts,
            )
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning(
                "Quote extraction failed: %s | payload=%s", exc, str(payload)[:200]
            )
            return None
