"""
Quick test: connect to PocketOption via pocketoptionapi-async
and fetch candles for EURUSD_otc.

Run: .venv\Scripts\python test_po_api.py
"""
import asyncio
import os
from pocketoptionapi_async import AsyncPocketOptionClient

# Your sessionToken from DevTools (Network → WS → Messages → 42["auth",...])
# Copy the value of "sessionToken" field
SESSION_TOKEN = os.environ.get("PO_SESSION_TOKEN", "dc33191b679196bb478761af76596626")
UID = int(os.environ.get("PO_UID", "129671966"))
IS_DEMO = True

# Build SSID in the format the library expects
SSID = f'42["auth",{{"session":"{SESSION_TOKEN}","isDemo":{1 if IS_DEMO else 0},"uid":{UID},"platform":1}}]'
print(f"Using SSID: {SSID[:60]}...")

SYMBOL = "EURUSD_otc"

async def on_candle_update(data):
    """Called every time a new candle/tick arrives."""
    print(f"[TICK] {data}")

async def main():
    print(f"Connecting to PocketOption (demo={IS_DEMO})...")
    client = AsyncPocketOptionClient(
        ssid=SSID,
        is_demo=IS_DEMO,
        auto_reconnect=True,
        enable_logging=True,
    )

    # Register real-time callback
    client.add_event_callback("candles", on_candle_update)

    # Connect
    connected = await client.connect()
    if not connected:
        print("❌ Connection failed!")
        return

    print("✅ Connected!")

    # Fetch historical candles (last 100, 1-minute)
    print(f"Fetching last 100 candles for {SYMBOL}...")
    try:
        candles = await client.get_candles(asset=SYMBOL, timeframe=60, count=100)
        print(f"✅ Got {len(candles)} candles!")
        if candles:
            last = candles[-1]
            print(f"  Last candle: O={last.open} H={last.high} L={last.low} C={last.close} T={last.time}")
    except Exception as e:
        print(f"❌ get_candles error: {e}")

    # Listen for real-time updates for 30 seconds
    print("Listening for real-time ticks for 30 seconds...")
    await asyncio.sleep(30)

    await client.disconnect()
    print("Done.")

if __name__ == "__main__":
    asyncio.run(main())
