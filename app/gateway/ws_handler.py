"""
FastAPI WebSocket Gateway
==========================
Each client connects to  ws://host:8000/ws/{symbol}
The handler subscribes to Redis  quotes:{symbol}  and forwards
every quote message as JSON until the client disconnects.
"""

import asyncio
import logging

from fastapi import WebSocket, WebSocketDisconnect

from app.broker.redis_client import RedisClient

logger = logging.getLogger(__name__)


class WebSocketGateway:
    """
    Manages client WebSocket connections.
    Thread-safe (asyncio single-threaded model — no locks needed).
    """

    def __init__(self, redis_client: RedisClient) -> None:
        self._redis = redis_client
        # symbol → list of active sockets (for monitoring only)
        self._connections: dict[str, list[WebSocket]] = {}

    async def handle(self, websocket: WebSocket, symbol: str) -> None:
        """
        Accept a WebSocket connection and stream quotes for `symbol`.

        The Redis subscription is held for the lifetime of the connection
        and released automatically when the client disconnects.
        """
        await websocket.accept()
        client = websocket.client
        logger.info("Client connected: %s → symbol=%s", client, symbol)

        self._connections.setdefault(symbol, []).append(websocket)

        try:
            async for quote in self._redis.subscribe(symbol):
                try:
                    # send_json is non-blocking and queues internally
                    await websocket.send_json(quote)
                except (WebSocketDisconnect, RuntimeError):
                    # Client closed the connection
                    break
                except Exception as exc:
                    logger.error("Send error to %s: %s", client, exc)
                    break

        except WebSocketDisconnect:
            pass

        except asyncio.CancelledError:
            pass

        finally:
            self._remove(symbol, websocket)
            logger.info("Client disconnected: %s", client)
            try:
                await websocket.close()
            except Exception:
                pass

    @property
    def connection_count(self) -> int:
        return sum(len(v) for v in self._connections.values())

    def _remove(self, symbol: str, ws: WebSocket) -> None:
        bucket = self._connections.get(symbol, [])
        try:
            bucket.remove(ws)
        except ValueError:
            pass
