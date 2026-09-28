"""EXTRACT: stream trade real-time dari Binance Public WebSocket (tanpa API key)."""

import json
import logging
from collections.abc import AsyncIterator

import websockets

log = logging.getLogger(__name__)

PING_INTERVAL_SECONDS = 20
OPEN_TIMEOUT_SECONDS = 15


def build_stream_url(base_url: str, symbols: tuple[str, ...]) -> str:
    """Combined stream, contoh: .../stream?streams=btcusdt@trade/ethusdt@trade"""
    streams = "/".join(f"{symbol.lower()}@trade" for symbol in symbols)
    return f"{base_url}?streams={streams}"


async def stream_binance(base_url: str, symbols: tuple[str, ...]) -> AsyncIterator[dict]:
    url = build_stream_url(base_url, symbols)
    log.info("Connecting ke Binance: %s", url)
    async with websockets.connect(
        url, ping_interval=PING_INTERVAL_SECONDS, open_timeout=OPEN_TIMEOUT_SECONDS
    ) as ws:
        log.info("Terhubung ke Binance, %d simbol", len(symbols))
        async for raw in ws:
            message = json.loads(raw)
            payload = message.get("data", {})
            if payload.get("e") == "trade":
                yield payload
