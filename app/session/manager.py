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
from pathlib import Path

logger = logging.getLogger(__name__)

# Cookies whose presence confirms a live authenticated session
AUTH_SIGNAL_COOKIES = {"ci_session", "loggedIn", "autologin", "po_uuid"}


class SessionManager:
    """
    Loads and exposes session cookies from a Playwright or Chrome-extension
    cookie file.

    Usage::

        sm = SessionManager(Path("storage_state.json"))
        sm.load()
        headers = {"Cookie": sm.get_cookie_header()}
    """

    def __init__(self, storage_state_path: Path) -> None:
        self._path = storage_state_path
        self._cookies: dict[str, str] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    def load(self) -> None:
        """
        Load session cookies.
        Priority:
          1. PO_COOKIES_JSON env var (full cookie export JSON)
          2. storage_state.json file
          3. Fallback: build minimal cookie from PO_SESSION_TOKEN (no file needed)
        """
        import os

        env_cookies = os.environ.get("PO_COOKIES_JSON")
        session_token = os.environ.get("PO_SESSION_TOKEN", "")

        if env_cookies:
            # Write to file so existing parse logic works
            with open(self._path, "w", encoding="utf-8") as f:
                f.write(env_cookies)

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
            logger.info("Session loaded from file — total=%d cookies | auth=%s",
                        len(self._cookies), sorted(auth_present))
        elif session_token:
            # Cookieless mode: build minimal header from session token
            logger.info("No cookie file found — using PO_SESSION_TOKEN for cookieless auth")
            self._cookies = {"ci_session": session_token, "loggedIn": "1"}
        else:
            raise FileNotFoundError(
                "No cookie source found. Set PO_COOKIES_JSON or PO_SESSION_TOKEN env var."
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

    def reload(self) -> None:
        """Reload cookies from disk (call after external session refresh)."""
        logger.info("Reloading session from disk …")
        self.load()

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
