"""
Session Manager
===============
Reads a cookie file and extracts cookies for direct use in aiohttp
WebSocket connections.

Supported file formats (auto-detected):
  1. Playwright storage_state.json  — {"cookies": [...], "origins": [...]}
  2. Chrome extension export        — [...] (bare JSON array of cookie objects)

Cookie object fields used:
  - name, value           (required)
  - expires / expirationDate  (float Unix timestamp; -1 = session cookie)
  - httpOnly, secure, domain, path  (informational only — not enforced here)
"""

import json
import logging
import time
import urllib.parse
from pathlib import Path

import aiohttp

logger = logging.getLogger(__name__)

# Cookies whose presence confirms a live authenticated session
AUTH_SIGNAL_COOKIES = {"ci_session", "loggedIn", "autologin", "po_uuid"}


import os
from typing import Optional

from app.broker.redis_client import RedisClient

class SessionManager:
    """
    Loads and exposes session cookies from a Playwright or Chrome-extension
    cookie file, Redis, or environment variables.
    """

    def __init__(self, storage_state_path: Path, redis_client: Optional[RedisClient] = None) -> None:
        self._path = storage_state_path
        self._redis = redis_client
        self._cookies: dict[str, str] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    async def load(self) -> None:
        """
        Load session cookies.
        Priority:
          1. Redis key 'po:cookies' (if redis_client provided)
          2. PO_COOKIES_JSON env var
          3. storage_state.json file
          4. PO_SESSION_TOKEN env var (cookieless auth)
        """
        env_cookies = os.environ.get("PO_COOKIES_JSON")
        session_token = os.environ.get("PO_SESSION_TOKEN", "")
        
        raw_cookies_str = None
        
        if self._redis and getattr(self._redis, '_redis', None):
            try:
                redis_data = await self._redis._redis.get("po:cookies")
                if redis_data:
                    raw_cookies_str = redis_data.decode("utf-8")
                    logger.info("Loaded cookies from Redis (po:cookies)")
            except Exception as exc:
                logger.warning("Failed to read from Redis po:cookies: %s", exc)
                
        if not raw_cookies_str and env_cookies:
            raw_cookies_str = env_cookies
            logger.info("Loaded cookies from PO_COOKIES_JSON env var")

        if raw_cookies_str:
            with open(self._path, "w", encoding="utf-8") as f:
                f.write(raw_cookies_str)

        if self._path.exists():
            with self._path.open("r", encoding="utf-8") as fh:
                raw = json.load(fh)
            cookie_list = self._parse_raw(raw)
            now = time.time()
            loaded: dict[str, str] = {}
            expired: list[str] = []
            for c in cookie_list:
                name = c.get("name", "")
                value = c.get("value", "")
                if not name:
                    continue
                exp = c.get("expires", c.get("expirationDate", -1))
                if exp != -1 and exp < now:
                    expired.append(name)
                    continue
                loaded[name] = value
            self._cookies = loaded
            if expired:
                logger.warning("Skipped %d expired cookies: %s", len(expired), expired)
            auth_present = AUTH_SIGNAL_COOKIES & set(self._cookies)
            logger.info("Session loaded — total=%d cookies | auth=%s",
                        len(self._cookies), sorted(auth_present))
        elif session_token:
            # Cookieless mode: build minimal header from session token
            logger.info("No cookie source found — using PO_SESSION_TOKEN for cookieless auth")
            self._cookies = {"ci_session": session_token, "loggedIn": "1"}
        else:
            raise FileNotFoundError(
                "No cookie source found. Set po:cookies in Redis, or PO_COOKIES_JSON/PO_SESSION_TOKEN env vars."
            )

    def get_cookie_header(self) -> str:
        """
        Return all cookies as a single Cookie HTTP header string.
        Example: "ci_session=abc...; loggedIn=1"
        """
        if not self._cookies:
            raise RuntimeError("Session not loaded. Call SessionManager.load() first.")
        return "; ".join(f"{k}={v}" for k, v in self._cookies.items())

    def get_cookies_dict(self) -> dict[str, str]:
        """Return a copy of the cookies dict (name → value)."""
        return dict(self._cookies)

    async def reload(self) -> None:
        """Reload cookies from disk/Redis (call after external session refresh)."""
        logger.info("Reloading session …")
        await self.load()

    async def auto_refresh_session(self) -> bool:
        """Use the autologin cookie to obtain a fresh ci_session tied to THIS server's IP.

        Flow:
          1. GET pocketoption.com with ONLY the autologin cookie (not ci_session)
          2. PO logs us in automatically → returns new ci_session for current IP
          3. Store the new ci_session in self._cookies

        This solves the IP-lock problem: the new ci_session will have Railway's IP,
        so demo-api-eu.po.market will accept our auth.
        """
        autologin = self._cookies.get("autologin", "")
        if not autologin:
            logger.warning("auto_refresh_session: no autologin cookie available")
            return False

        url = "https://pocketoption.com/en/cabinet/demo-quick-high-low/"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        # Send all cookies EXCEPT ci_session (which has the wrong IP and causes conflict).
        # PO might require po_uuid or other cookies to process the autologin.
        cookies = {k: v for k, v in self._cookies.items() if k != "ci_session"}
        if "autologin" not in cookies:
            logger.warning("auto_refresh_session: no autologin cookie available")
            return False

        try:
            from curl_cffi.requests import AsyncSession
            
            async with AsyncSession(impersonate="chrome110", headers=headers, timeout=15) as s:
                resp = await s.get(
                    url,
                    cookies=cookies,
                    allow_redirects=True,
                )
                logger.info(
                    "auto_refresh_session: GET %s → status=%d url=%s",
                    url, resp.status_code, str(resp.url)[:80],
                )
                # Extract ci_session from response cookies
                new_ci = resp.cookies.get("ci_session")
                if new_ci:
                    self._cookies["ci_session"] = new_ci
                    # Try to extract and log the embedded IP to verify
                    try:
                        decoded = urllib.parse.unquote(new_ci)
                        import re
                        m = re.search(r'"ip_address";s:\d+:"([^"]+)"', decoded)
                        ip = m.group(1) if m else "unknown"
                    except Exception:
                        ip = "?"
                    logger.info(
                        "auto_refresh_session ✓ new ci_session obtained | ip_in_session=%s",
                        ip,
                    )
                    return True
                logger.warning(
                    "auto_refresh_session: no ci_session in response cookies. "
                    "autologin cookie may be expired or invalid."
                )
                return False
        except Exception as exc:
            logger.error("auto_refresh_session failed: %s", exc)
            return False

    @property
    def is_loaded(self) -> bool:
        return bool(self._cookies)

    # ── Format detection ──────────────────────────────────────────────────────

    @staticmethod
    def _parse_raw(raw: list | dict) -> list[dict]:
        """
        Normalise cookie data to a flat list of cookie dicts.

        Supported inputs:
          - list  → Chrome extension export (raw array of cookie objects)
          - dict  → Playwright storage_state  {"cookies": [...], "origins": [...]}
        """
        if isinstance(raw, list):
            logger.debug("Detected Chrome extension cookie format (JSON array)")
            return raw

        if isinstance(raw, dict):
            if "cookies" in raw:
                logger.debug("Detected Playwright storage_state format")
                return raw["cookies"]
            # Fallback: maybe it's a single cookie wrapped in a dict?
            logger.warning("Unknown dict format — treating as single cookie entry")
            return [raw]

        raise ValueError(
            f"Unsupported cookie file format: expected list or dict, got {type(raw).__name__}"
        )
