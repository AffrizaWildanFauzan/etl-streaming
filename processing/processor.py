"""Processor: consume Kafka -> TRANSFORM -> LOAD ke PostgreSQL (micro-batch).

Semantik: at-least-once dari Kafka + load idempotent = hasil akhir tanpa duplikat.
Offset baru di-commit SETELAH transaksi database berhasil. Jika database gagal,
proses berhenti (exit != 0) dan Docker me-restart-nya dari offset terakhir.
"""

import json
import logging
import signal
from dataclasses import dataclass

import psycopg2
from confluent_kafka import Consumer, KafkaException, Message, Producer

from processing.aggregate import aggregate_candles
from common.config import env_int, kafka_config, postgres_config
from storage.load import insert_trades, upsert_candles
from processing.transform import InvalidRecord, Trade, transform_message

log = logging.getLogger("processor")


@dataclass(frozen=True)
class BatchResult:
    received: int
    loaded: int
    duplicates: int
    rejected: int
    candles: int


class Shutdown:
    requested = False

    def request(self, *_):
        log.info("Sinyal shutdown diterima")
        self.requested = True


def send_to_dlq(dlq: Producer, topic: str, msg: Message, reason: str) -> None:
    record = {
        "reason": reason,
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "raw": (msg.value() or b"").decode("utf-8", errors="replace"),
    }
    dlq.produce(topic, key=msg.key(), value=json.dumps(record).encode())


def split_valid(messages: list[Message], dlq: Producer, dlq_topic: str) -> tuple[list[Trade], int]:
    trades, rejected = [], 0
    for msg in messages:
        if msg.error():
            raise KafkaException(msg.error())
        try:
            trades.append(transform_message(msg.value()))
        except InvalidRecord as exc:
            rejected += 1
            log.warning("Record ditolak (offset %s): %s", msg.offset(), exc)
            send_to_dlq(dlq, dlq_topic, msg, str(exc))
    dlq.flush()
    return trades, rejected


def process_batch(messages, conn, dlq: Producer, dlq_topic: str) -> BatchResult:
    trades, rejected = split_valid(messages, dlq, dlq_topic)
    try:
        new_trades = insert_trades(conn, trades)
        candles = aggregate_candles(new_trades)
        upsert_candles(conn, candles)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return BatchResult(
        received=len(messages),
        loaded=len(new_trades),
        duplicates=len(trades) - len(new_trades),
        rejected=rejected,
        candles=len(candles),
    )


def run(consumer: Consumer, conn, dlq: Producer, dlq_topic: str, shutdown: Shutdown):
    batch_size = env_int("BATCH_SIZE", 500)
    batch_timeout = env_int("BATCH_TIMEOUT_MS", 1000) / 1000
    while not shutdown.requested:
        messages = consumer.consume(num_messages=batch_size, timeout=batch_timeout)
        if not messages:
            continue
        result = process_batch(messages, conn, dlq, dlq_topic)
        consumer.commit(asynchronous=False)
        log.info(
            "batch: diterima=%d dimuat=%d duplikat=%d ditolak=%d candle=%d",
            result.received, result.loaded, result.duplicates, result.rejected, result.candles,
        )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    kafka = kafka_config()
    shutdown = Shutdown()
    signal.signal(signal.SIGINT, shutdown.request)
    signal.signal(signal.SIGTERM, shutdown.request)

    consumer = Consumer({
        "bootstrap.servers": kafka.bootstrap,
        "group.id": kafka.group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    dlq = Producer({"bootstrap.servers": kafka.bootstrap, "client.id": "processor-dlq"})
    conn = psycopg2.connect(postgres_config().dsn)
    consumer.subscribe([kafka.raw_topic])
    log.info("Processor berjalan: %s -> PostgreSQL", kafka.raw_topic)
    try:
        run(consumer, conn, dlq, kafka.dlq_topic, shutdown)
    finally:
        consumer.close()
        conn.close()
        log.info("Processor berhenti")


main()
