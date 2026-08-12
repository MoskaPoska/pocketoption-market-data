"""
Cookie Expiry Watcher
=====================
Runs as a background asyncio task.
Every 12 hours it checks cookie expiration dates from storage_state.json.
If any auth cookie expires within WARN_DAYS (default 7), it sends a
Telegram alert to all configured admin IDs.

Only admins listed in TELEGRAM_ADMIN_IDS receive the message.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Sequence

import aiohttp

from app.config import settings

logger = logging.getLogger(__name__)

# How many seconds between checks (12 hours)
CHECK_INTERVAL = 12 * 3600

# Auth cookies we care about
AUTH_COOKIE_NAMES = {"ci_session", "autologin", "loggedIn", "po_uuid"}


class CookieWatcher:
    """Monitors cookie expiry and notifies Telegram admins."""

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None

    # ── Public API ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if not settings.TELEGRAM_BOT_TOKEN:
            logger.warning("CookieWatcher disabled — TELEGRAM_BOT_TOKEN not set")
            return
        if not settings.TELEGRAM_ADMIN_IDS:
            logger.warning("CookieWatcher disabled — TELEGRAM_ADMIN_IDS not set")
            return

        logger.info(
            "CookieWatcher started — checking every 12h | admins=%s",
            settings.TELEGRAM_ADMIN_IDS,
        )
        self._task = asyncio.create_task(self._run_loop(), name="cookie-watcher")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("CookieWatcher stopped.")

    # ── Internal loop ──────────────────────────────────────────────────────────

    async def _run_loop(self) -> None:
        # Run first check shortly after startup (60 s delay)
        await asyncio.sleep(60)
        while True:
            try:
                await self._check_and_notify()
            except Exception as exc:
                logger.error("CookieWatcher error: %s", exc, exc_info=True)
            await asyncio.sleep(CHECK_INTERVAL)

    async def _check_and_notify(self) -> None:
        expiring = self._find_expiring_cookies(warn_days=settings.COOKIE_WARN_DAYS)
        if not expiring:
            logger.debug("CookieWatcher: all cookies OK")
            return

        msg = self._build_message(expiring)
        logger.warning("CookieWatcher: expiring cookies detected — notifying admins")
        await self._broadcast(msg)

    # ── Cookie inspection ──────────────────────────────────────────────────────

    def _find_expiring_cookies(self, warn_days: int) -> list[dict]:
        """Return list of auth cookies expiring within warn_days."""
        path = Path(settings.STORAGE_STATE_PATH)
        if not path.exists():
            return []

        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("CookieWatcher: cannot read %s: %s", path, exc)
            return []

        # Support both Playwright storage_state and raw Chrome JSON array
        if isinstance(raw, dict):
            cookies: list = raw.get("cookies", [])
        elif isinstance(raw, list):
            cookies = raw
        else:
            return []

        now = time.time()
        threshold = now + warn_days * 86400
        expiring = []

        for c in cookies:
            name = c.get("name", "")
            if name not in AUTH_COOKIE_NAMES:
                continue
            exp = c.get("expirationDate") or c.get("expires", 0)
            if exp and exp < threshold:
                days_left = max(0, int((exp - now) / 86400))
                expiring.append({"name": name, "days_left": days_left, "expires": int(exp)})

        return expiring

    @staticmethod
    def _build_message(expiring: list[dict]) -> str:
        # Find the minimum days left across all expiring cookies
        min_days = min(c["days_left"] for c in expiring)

        if min_days == 0:
            urgency = "🔴 Куки уже истекли!"
        elif min_days <= 3:
            urgency = f"🔴 Куки истекают через {min_days} дн."
        else:
            urgency = f"🟡 Куки истекают через {min_days} дн."

        return (
            f"{urgency}\n\n"
            "Нужно обновить куки в боте, иначе котировки перестанут поступать."
        )

    # ── Telegram dispatch ──────────────────────────────────────────────────────

    async def _broadcast(self, text: str) -> None:
        """Send message to every admin ID via Telegram Bot API."""
        admin_ids: Sequence[int] = settings.TELEGRAM_ADMIN_IDS
        async with aiohttp.ClientSession() as session:
            for admin_id in admin_ids:
                ok = await self._send_message(session, admin_id, text)
                if ok:
                    logger.info("Telegram alert sent to admin %d", admin_id)
                else:
                    logger.error("Failed to send Telegram alert to admin %d", admin_id)

    async def _send_message(
        self,
        session: aiohttp.ClientSession,
        chat_id: int,
        text: str,
    ) -> bool:
        url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }
        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return True
                body = await resp.text()
                logger.error(
                    "Telegram API error: status=%d body=%s", resp.status, body[:200]
                )
                return False
        except Exception as exc:
            logger.error("Telegram send error: %s", exc)
            return False
