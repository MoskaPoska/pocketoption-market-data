from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── Source WebSocket ──────────────────────────────────────────────────────
    SOURCE_WS_URL: str = (
        "wss://chat-po.site/cabinet-client/socket.io/?EIO=4&transport=websocket"
    )
    SOURCE_ORIGIN: str = "https://chat-po.site"

    # Real-time price feed — demo-api-eu is the correct server for updateStream
    EVENTS_WS_URL: str = "wss://demo-api-eu.po.market/socket.io/?EIO=4&transport=websocket"
    EVENTS_WS_ORIGIN: str = "https://pocketoption.com"

    # ── Session ───────────────────────────────────────────────────────────────
    # Path to Playwright storage_state.json produced externally
    STORAGE_STATE_PATH: Path = Path("storage_state.json")

    # Socket.IO auth credentials (from browser DevTools → user_init event)
    SOCKET_USER_ID: int = 0          # your numeric user ID
    SOCKET_SECRET: str = ""          # session secret from user_init message

    # Short session token for demo-api-eu.po.market auth.
    # Get it from: Browser DevTools → Network → WS → demo-api-eu.po.market
    # → Messages → first ↑ message starting with 42["auth" → copy "session" value
    PO_SESSION_TOKEN: str = ""

    # Symbol to subscribe to for continuous price ticks (changeSymbol event)
    # Set to empty string to skip — only chat_room_list will be used.
    # Can be a comma-separated list of symbols: "EURUSD_otc,GBPUSD_otc"
    SUBSCRIBE_SYMBOL: str = "EURUSD_otc"
    SUBSCRIBE_IS_DEMO: int = 1        # 1 = demo, 0 = real

    @property
    def subscribe_symbols_list(self) -> list[str]:
        """Returns SUBSCRIBE_SYMBOL as a list of strings."""
        if not self.SUBSCRIBE_SYMBOL:
            return []
        return [sym.strip() for sym in self.SUBSCRIBE_SYMBOL.split(",") if sym.strip()]

    # ── Redis ─────────────────────────────────────────────────────────────────
    REDIS_URL: str = "redis://redis:6379/0"

    # ── Reconnect / Heartbeat ─────────────────────────────────────────────────
    RECONNECT_BASE_DELAY: float = 1.0    # seconds
    RECONNECT_MAX_DELAY: float = 30.0   # seconds
    RECONNECT_MAX_JITTER: float = 1.0   # seconds

    # Engine.IO heartbeat — send "2" (ping) every N seconds
    PING_INTERVAL: float = 20.0

    # ── FastAPI ───────────────────────────────────────────────────────────────
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    DEBUG: bool = False

    # ── Telegram Admin Alerts ─────────────────────────────────────────────────
    # Bot token from @BotFather. Leave empty to disable alerts.
    TELEGRAM_BOT_TOKEN: str = ""
    # Comma-separated list of admin Telegram user IDs (e.g. "123456789,987654321")
    TELEGRAM_ADMIN_IDS: list[int] = []
    # Warn this many days before cookie expiry
    COOKIE_WARN_DAYS: int = 7


# Module-level singleton — imported everywhere
settings = Settings()
