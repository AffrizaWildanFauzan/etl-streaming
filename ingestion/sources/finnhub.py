"""EXTRACT: stream trade saham US real-time dari Finnhub WebSocket (butuh API key gratis)."""

import json
import logging
from collections.abc import AsyncIterator

import websockets

log = logging.getLogger(__name__)

FINNHUB_WS_URL = "wss://ws.finnhub.io"
PING_INTERVAL_SECONDS = 20
OPEN_TIMEOUT_SECONDS = 15


async def stream_finnhub(api_key: str, symbols: tuple[str, ...]) -> AsyncIterator[dict]:
    log.info("Connecting ke Finnhub untuk %s", ", ".join(symbols))
    async with websockets.connect(
        f"{FINNHUB_WS_URL}?token={api_key}",
        ping_interval=PING_INTERVAL_SECONDS,
        open_timeout=OPEN_TIMEOUT_SECONDS,
    ) as ws:
        for symbol in symbols:
            await ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        log.info("Terhubung ke Finnhub, subscribe %d simbol", len(symbols))

        async for raw in ws:
            message = json.loads(raw)
            message_type = message.get("type")
            if message_type == "trade":
                # Satu pesan Finnhub bisa berisi banyak trade -> dipecah per trade
                for trade in message.get("data", []):
                    yield trade
            elif message_type == "error":
                log.error("Finnhub error: %s", message.get("msg"))
