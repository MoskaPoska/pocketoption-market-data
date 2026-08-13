"""
Engine.IO v4 / Socket.IO v5 Frame Parser
=========================================

Engine.IO frame types (first byte of text frame):
  "0"  open      — server handshake JSON
  "1"  close     — server closing
  "2"  ping      — server heartbeat (we must reply "3")
  "3"  pong      — server ack of our ping
  "4"  message   — contains Socket.IO payload
  "6"  noop      — ignore

Socket.IO packet types (second byte, inside EIO "4" frames):
  "0"  connect
  "1"  disconnect
  "2"  event         → "42["eventName", payload]"
  "3"  ack
  "4"  connect_error
  "5"  binary_event  → "45{N}-["eventName", {"_placeholder":true,"num":0}]"
                        followed by N raw binary WebSocket frames
  "6"  binary_ack

Binary event sequence:
  1. TEXT  "451-["updateStream",{"_placeholder":true,"num":0}]"
  2. BIN   <raw bytes>  ← attachment #0

  The text frame announces N attachments; the next N binary WS frames
  are the actual data payloads.  We buffer the announcement and
  collect binaries until we have all N attachments.
"""

import json
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


# ── Domain model ─────────────────────────────────────────────────────────────

@dataclass(slots=True)
class Quote:
    symbol: str
    price: float
    timestamp: int          # Unix milliseconds


# ── Parser ───────────────────────────────────────────────────────────────────

class EngineIOParser:
    """
    Stateful parser for Engine.IO / Socket.IO frames.

    Maintains a small internal buffer for the binary-event multi-frame
    sequence (text announcement + N binary attachments).
    """

    def __init__(self) -> None:
        self._pending_event_name: Optional[str] = None
        self._pending_count: int = 0                    # expected attachments
        self._pending_attachments: list[bytes] = []     # collected so far

    # ── Public API ────────────────────────────────────────────────────────────

    def parse_text_frame(self, data: str) -> tuple[str, list["Quote"]]:
        """
        Parse one text WebSocket frame.

        Returns:
            (action, quotes)
            action ∈ {"ping", "pong", "open", "close", "message", "noop", "unknown"}
            quotes — list of Quote instances decoded from this frame (may be empty).

        The caller must handle action == "ping" by sending "3" over the wire.
        """
        if not data:
            return "unknown", []

        eio_type = data[0]

        match eio_type:
            case "2":
                logger.debug("EIO ← PING (server-initiated)")
                return "ping", []

            case "3":
                logger.debug("EIO ← PONG")
                return "pong", []

            case "0":
                self._log_handshake(data[1:])
                return "open", []

            case "1":
                logger.warning("EIO ← CLOSE")
                return "close", []

            case "6":
                return "noop", []

            case "4":
                quotes = self._parse_socketio(data[1:])
                return "message", quotes

            case _:
                logger.warning("Unknown EIO frame type '%s': %s", eio_type, data[:120])
                return "unknown", []

    def parse_binary_frame(self, data: bytes) -> Optional[Quote]:
        """
        Process one binary WebSocket frame.

        ┌─────────────────────────────────────────────────────────────────┐
        │  STUB — binary format not yet known.                            │
        │  Raw bytes are logged for manual inspection.                    │
        │  Replace _decode_binary_event() once format is confirmed.       │
        └─────────────────────────────────────────────────────────────────┘
        """
        # ── RAW LOG (Phase 1 — format discovery) ─────────────────────────
        logger.info(
            "[RAW BINARY] len=%d | hex_head=%s | repr_head=%r",
            len(data),
            data[:64].hex(),
            data[:64],
        )
        # ─────────────────────────────────────────────────────────────────

        if self._pending_event_name is None:
            logger.debug("Binary frame received with no pending event — logged above.")
            return None

        self._pending_attachments.append(data)

        if len(self._pending_attachments) < self._pending_count:
            logger.debug(
                "Buffered attachment %d/%d for event '%s'",
                len(self._pending_attachments),
                self._pending_count,
                self._pending_event_name,
            )
            return None

        # All attachments received → decode
        event_name = self._pending_event_name
        attachments = self._pending_attachments[:]
        self._reset_pending()

        return self._decode_binary_event(event_name, attachments)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _parse_socketio(self, data: str) -> list["Quote"]:
        """Route to the correct Socket.IO handler by packet type."""
        if not data:
            return []

        sio_type = data[0]

        match sio_type:
            case "0":
                logger.info("SIO ← CONNECT: %s", data[1:80])
                return []

            case "1":
                logger.warning("SIO ← DISCONNECT: %s", data[1:80])
                return []

            case "2":
                # Regular text event: '2["eventName", payload]'
                return self._parse_text_event(data[1:])

            case "5":
                # Binary event announcement: '5{N}-["eventName", {...}]'
                self._handle_binary_announcement(data)
                return []

            case _:
                logger.debug("SIO packet type '%s' — not handled", sio_type)
                return []

    def _parse_text_event(self, json_str: str) -> list["Quote"]:
        """Parse a regular Socket.IO event (SIO type 2)."""
        try:
            packet = json.loads(json_str)
        except json.JSONDecodeError as exc:
            logger.error("SIO event JSON decode error: %s | data=%s", exc, json_str[:200])
            return []

        if not isinstance(packet, list) or len(packet) < 1:
            return []

        event_name = packet[0]
        payload = packet[1] if len(packet) > 1 else None

        match event_name:
            case "updateStream":
                q = self._extract_quote(payload)
                return [q] if q else []

            case "user_ready":
                logger.info("SIO ← user_ready | user_id=%s", payload.get("id") if payload else "?")
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

    def _handle_binary_announcement(self, data: str) -> None:
        """
        Parse a Socket.IO binary event announcement (SIO type 5).

        Format:  "5{num_attachments}-["eventName", {"_placeholder":true,"num":0}]"
        Example: "451-["updateStream",{"_placeholder":true,"num":0}]"
        """
        try:
            dash_idx = data.index("-", 1)           # skip SIO type byte
            num_attachments = int(data[1:dash_idx])
            json_str = data[dash_idx + 1:]
            packet = json.loads(json_str)
        except (ValueError, json.JSONDecodeError) as exc:
            logger.error("Binary announcement parse error: %s | raw=%s", exc, data[:200])
            return

        if not isinstance(packet, list) or not packet:
            return

        event_name = packet[0]
        self._pending_event_name = event_name
        self._pending_count = num_attachments
        self._pending_attachments = []

        logger.debug(
            "Binary event '%s' announced — waiting for %d attachment(s)",
            event_name,
            num_attachments,
        )

    def _decode_binary_event(
        self, event_name: str, attachments: list[bytes]
    ) -> Optional[Quote]:
        """
        Decode binary Socket.IO attachment for `updateStream`.

        PocketOption sends real-time price ticks as binary attachments.
        We attempt three decoders in order:
          1. MessagePack — most common in modern real-time APIs
          2. Custom struct — 4-byte symbol_id + 8-byte double price + 8-byte int64 ts
          3. UTF-8 JSON — fallback

        All raw bytes are also logged at DEBUG level for format discovery.
        """
        import struct
        import time as _time

        for idx, raw in enumerate(attachments):
            logger.debug(
                "[BINARY] event='%s' len=%d hex=%s",
                event_name, len(raw), raw[:64].hex(),
            )

            # ── Attempt 1: MessagePack ────────────────────────────────────
            try:
                import msgpack
                payload = msgpack.unpackb(raw, raw=False)
                logger.debug("[BINARY→MSGPACK] %s", payload)
                if isinstance(payload, dict) and "symbol" in payload:
                    return self._extract_quote(payload)
                if isinstance(payload, (list, tuple)) and len(payload) >= 2:
                    # [symbol, price] or [symbol, price, timestamp]
                    sym = str(payload[0])
                    price = float(payload[1])
                    ts = int(payload[2]) if len(payload) > 2 else int(_time.time())
                    if ts > 2_000_000_000:
                        ts = ts // 1000
                    logger.info("Signal extracted via msgpack: symbol=%s price=%s", sym, price)
                    return Quote(symbol=sym, price=price, timestamp=ts)
            except Exception:
                pass

            # ── Attempt 2: UTF-8 JSON ─────────────────────────────────────
            try:
                text = raw.decode("utf-8")
                payload = json.loads(text)
                logger.debug("[BINARY→JSON] %s", payload)
                if isinstance(payload, dict) and "symbol" in payload:
                    return self._extract_quote(payload)
            except (UnicodeDecodeError, json.JSONDecodeError):
                pass

            # ── Attempt 3: custom struct — try common layouts ─────────────
            # Layout A: 2-byte sym_len, N-byte sym, 8-byte double price, 8-byte int64 ts
            if len(raw) >= 18:
                try:
                    sym_len = struct.unpack_from(">H", raw, 0)[0]
                    if 2 <= sym_len <= 30 and len(raw) >= 2 + sym_len + 16:
                        sym = raw[2:2 + sym_len].decode("ascii")
                        price = struct.unpack_from(">d", raw, 2 + sym_len)[0]
                        ts = struct.unpack_from(">q", raw, 2 + sym_len + 8)[0]
                        if ts > 2_000_000_000:
                            ts = ts // 1000
                        if 0 < price < 1_000_000 and sym.replace("_", "").isalnum():
                            logger.info("Signal extracted via struct: symbol=%s price=%s", sym, price)
                            return Quote(symbol=sym, price=price, timestamp=ts)
                except Exception:
                    pass

            # Log raw bytes for manual format discovery
            logger.info(
                "[BINARY UNDECODABLE] event='%s' len=%d hex=%s repr=%r",
                event_name, len(raw), raw[:80].hex(), raw[:80],
            )

        return None

    def _extract_signal_from_room(self, room: dict | None) -> Optional[Quote]:
        """
        Extract a Quote from a chat_room_list / chat_room_list_update room dict.

        DevTools observation — room payload contains:
        {
          "room_id": 224730975,
          "message": {
            "message_id": 625961232,
            "message_content": {
              "signal": {
                "symbol": "ETHUSD_otc",
                "type": "cryptocurr",
                ...
              }
            }
          }
        }

        Log the full room payload at DEBUG level so we can refine the path.
        """
        if not isinstance(room, dict):
            return None

        logger.debug("chat_room payload: %s", str(room)[:400])

        # The signal can be at different nesting levels depending on event type.
        # Try all known variants:
        #   1. room["message_content"]["signal"]  (chat_room_list_update)
        #   2. room["message"]["message_content"]["signal"]  (alternate)
        try:
            # Variant 1: message_content directly on room dict
            content = room.get("message_content")
            if isinstance(content, str):
                import json as _json
                content = _json.loads(content)

            if isinstance(content, dict):
                signal = content.get("signal") or {}
            else:
                # Variant 2: drill through message sub-dict
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

            return Quote(
                symbol=str(signal["symbol"]),
                price=float(signal.get("price", signal.get("value", 0))),
                timestamp=ts,
            )
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            logger.debug("Signal extraction failed: %s | room=%s", exc, str(room)[:300])
            return None

    def _extract_quote(self, payload: dict) -> Optional[Quote]:
        """Extract a Quote from an updateStream payload dict."""
        import time
        try:
            ts = int(payload.get("timestamp", 0))
            if ts <= 0:
                ts = int(time.time())
            elif ts > 2000000000:
                ts = int(ts / 1000) # Convert ms to s

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

    def _reset_pending(self) -> None:
        self._pending_event_name = None
        self._pending_count = 0
        self._pending_attachments = []

    @staticmethod
    def _log_handshake(json_str: str) -> None:
        try:
            cfg = json.loads(json_str)
            logger.info(
                "EIO handshake — sid=%s pingInterval=%s pingTimeout=%s",
                cfg.get("sid"),
                cfg.get("pingInterval"),
                cfg.get("pingTimeout"),
            )
        except json.JSONDecodeError:
            logger.info("EIO handshake (raw): %s", json_str[:200])
