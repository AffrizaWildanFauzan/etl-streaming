"""Producer: EXTRACT data dari WebSocket lalu publish mentah ke Kafka.

Jalankan:  SOURCE=binance python -m ingestion.producer
           SOURCE=finnhub FINNHUB_API_KEY=xxx python -m ingestion.producer
"""

import asyncio
import json
import logging
import signal
import time
from collections.abc import AsyncIterator, Callable

import websockets
from confluent_kafka import KafkaError, Message, Producer

from common.config import env, env_list, kafka_config
from ingestion.sources.binance import stream_binance
from ingestion.sources.finnhub import stream_finnhub

log = logging.getLogger("producer")

INITIAL_BACKOFF_SECONDS = 1
MAX_BACKOFF_SECONDS = 60
STATS_EVERY_N_MESSAGES = 1000
FLUSH_TIMEOUT_SECONDS = 10
DEFAULT_BINANCE_URL = "wss://data-stream.binance.vision/stream"
DEFAULT_BINANCE_SYMBOLS = "btcusdt,ethusdt,bnbusdt,solusdt,xrpusdt"
DEFAULT_FINNHUB_SYMBOLS = "AAPL,MSFT,NVDA,TSLA,AMZN,BINANCE:BTCUSDT"

StreamFactory = Callable[[], AsyncIterator[dict]]


def build_source(name: str) -> StreamFactory:
    if name == "binance":
        url = env("BINANCE_WS_URL", DEFAULT_BINANCE_URL)
        symbols = env_list("BINANCE_SYMBOLS", DEFAULT_BINANCE_SYMBOLS)
        return lambda: stream_binance(url, symbols)
    if name == "finnhub":
        api_key = env("FINNHUB_API_KEY", required=True)
        symbols = env_list("FINNHUB_SYMBOLS", DEFAULT_FINNHUB_SYMBOLS)
        return lambda: stream_finnhub(api_key, symbols)
    raise RuntimeError(f"SOURCE tidak dikenal: {name!r} (pilih binance / finnhub)")


def build_envelope(source: str, payload: dict) -> bytes:
    """Data mentah dibungkus metadata ingestion, tanpa diubah isinya."""
    envelope = {"source": source, "ingested_at": int(time.time() * 1000), "payload": payload}
    return json.dumps(envelope, separators=(",", ":")).encode()


def _on_delivery(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        log.error("Gagal kirim ke Kafka (%s): %s", msg.topic(), err)


def publish(producer: Producer, topic: str, source: str, payload: dict) -> None:
    key = str(payload.get("s", "")).encode()  # key = symbol -> urutan per simbol terjaga
    value = build_envelope(source, payload)
    try:
        producer.produce(topic, key=key, value=value, on_delivery=_on_delivery)
    except BufferError:
        log.warning("Buffer producer penuh, menunggu flush...")
        producer.poll(1)
        producer.produce(topic, key=key, value=value, on_delivery=_on_delivery)
    producer.poll(0)


async def run_forever(source: str, factory: StreamFactory, producer: Producer, topic: str):
    backoff = INITIAL_BACKOFF_SECONDS
    sent = 0
    while True:
        try:
            async for payload in factory():
                publish(producer, topic, source, payload)
                sent += 1
                backoff = INITIAL_BACKOFF_SECONDS
                if sent % STATS_EVERY_N_MESSAGES == 0:
                    log.info("%s: %d pesan terkirim ke %s", source, sent, topic)
            log.warning("Stream %s ditutup server", source)
        except (websockets.WebSocketException, OSError, asyncio.TimeoutError) as exc:
            log.warning("Koneksi %s terputus: %s: %s", source, type(exc).__name__, exc)
        except json.JSONDecodeError as exc:
            log.error("Pesan WebSocket bukan JSON: %s", exc)
        log.info("Reconnect %s dalam %ds", source, backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    source = env("SOURCE", "binance")
    kafka = kafka_config()
    factory = build_source(source)
    producer = Producer({
        "bootstrap.servers": kafka.bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 50,
        "compression.type": "lz4",
        "client.id": f"producer-{source}",
    })

    task = asyncio.create_task(run_forever(source, factory, producer, kafka.raw_topic))
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, task.cancel)
    try:
        await task
    except asyncio.CancelledError:
        log.info("Shutdown, flush pesan tersisa...")
    finally:
        remaining = producer.flush(FLUSH_TIMEOUT_SECONDS)
        if remaining:
            log.error("%d pesan belum terkirim saat shutdown", remaining)


if __name__ == "__main__":
    asyncio.run(main())
