#!/usr/bin/env python3
"""
scripts/import_cookies.py
==========================
Converts a Chrome-extension cookie export (JSON array) into a
Playwright-compatible storage_state.json.

Usage:
    python scripts/import_cookies.py chrome_cookies.json
    python scripts/import_cookies.py chrome_cookies.json --out storage_state.json

The input file should be a JSON array exported from:
  - EditThisCookie
  - Cookie-Editor
  - Any extension that exports cookies as a JSON array
"""

import argparse
import json
import sys
import time
from pathlib import Path


def convert(chrome_cookies: list[dict]) -> dict:
    """Convert Chrome extension cookie list → Playwright storage_state dict."""
    now = time.time()
    playwright_cookies = []
    skipped = []

    for c in chrome_cookies:
        name = c.get("name", "")
        if not name:
            continue

        exp = c.get("expirationDate", c.get("expires", -1))
        if exp != -1 and exp < now:
            skipped.append(name)
            continue

        playwright_cookies.append(
            {
                "name": name,
                "value": c.get("value", ""),
                "domain": c.get("domain", ""),
                "path": c.get("path", "/"),
                # Playwright uses int; -1 = session cookie
                "expires": int(exp) if exp != -1 else -1,
                "httpOnly": bool(c.get("httpOnly", False)),
                "secure": bool(c.get("secure", False)),
                # Chrome null → Playwright "None"
                "sameSite": c.get("sameSite") or "None",
            }
        )

    if skipped:
        print(f"[!] Skipped {len(skipped)} expired cookies: {skipped}", file=sys.stderr)

    return {"cookies": playwright_cookies, "origins": []}


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert Chrome cookies → storage_state.json")
    parser.add_argument("input", type=Path, help="Chrome extension cookie export (JSON array)")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("storage_state.json"),
        help="Output path (default: storage_state.json)",
    )
    args = parser.parse_args()

    if not args.input.exists():
        print(f"[ERROR] Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    with args.input.open("r", encoding="utf-8") as fh:
        raw = json.load(fh)

    if not isinstance(raw, list):
        print("[ERROR] Input must be a JSON array of cookie objects.", file=sys.stderr)
        sys.exit(1)

    state = convert(raw)
    total = len(state["cookies"])

    with args.out.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, ensure_ascii=False)

    print(f"[OK] Wrote {total} cookies to '{args.out}'")
    print("     Auth cookies present:",
          [c["name"] for c in state["cookies"]
           if c["name"] in {"ci_session", "loggedIn", "autologin", "po_uuid"}])


if __name__ == "__main__":
    main()
