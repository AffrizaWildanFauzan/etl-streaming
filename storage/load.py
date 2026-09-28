"""LOAD: menulis trade bersih dan candle OHLCV ke PostgreSQL secara idempotent.

- trades   : INSERT ... ON CONFLICT DO NOTHING (dedup berdasarkan PK)
- ohlcv_1m : UPSERT yang menggabungkan candle parsial dengan candle di database

Hanya trade yang benar-benar baru (hasil RETURNING) yang diagregasi, sehingga
replay pesan Kafka (at-least-once) tidak menggandakan volume.
"""

from collections.abc import Sequence

from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values

from processing.aggregate import Candle
from processing.transform import Trade

INSERT_TRADES_SQL = """
INSERT INTO trades (source, symbol, trade_id, price, quantity, notional, side,
                    trade_time, ingested_at)
VALUES %s
ON CONFLICT (source, symbol, trade_id) DO NOTHING
RETURNING source, symbol, trade_id
"""

UPSERT_CANDLES_SQL = """
INSERT INTO ohlcv_1m AS o (source, symbol, bucket_start, open, high, low, close,
                           volume, notional, trade_count, first_trade_time, last_trade_time)
VALUES %s
ON CONFLICT (source, symbol, bucket_start) DO UPDATE SET
    open  = CASE WHEN EXCLUDED.first_trade_time < o.first_trade_time
                 THEN EXCLUDED.open ELSE o.open END,
    close = CASE WHEN EXCLUDED.last_trade_time >= o.last_trade_time
                 THEN EXCLUDED.close ELSE o.close END,
    high  = GREATEST(o.high, EXCLUDED.high),
    low   = LEAST(o.low, EXCLUDED.low),
    volume      = o.volume + EXCLUDED.volume,
    notional    = o.notional + EXCLUDED.notional,
    trade_count = o.trade_count + EXCLUDED.trade_count,
    first_trade_time = LEAST(o.first_trade_time, EXCLUDED.first_trade_time),
    last_trade_time  = GREATEST(o.last_trade_time, EXCLUDED.last_trade_time),
    updated_at = now()
"""


def _dedupe(trades: Sequence[Trade]) -> list[Trade]:
    """Hilangkan duplikat di dalam batch (Postgres menolak konflik ganda di satu INSERT)."""
    unique = {(t.source, t.symbol, t.trade_id): t for t in trades}
    return list(unique.values())


def insert_trades(conn: PgConnection, trades: Sequence[Trade]) -> list[Trade]:
    """Insert trade; kembalikan hanya trade yang baru (belum pernah dimuat)."""
    unique = _dedupe(trades)
    if not unique:
        return []
    rows = [
        (t.source, t.symbol, t.trade_id, t.price, t.quantity, t.notional, t.side,
         t.trade_time, t.ingested_at)
        for t in unique
    ]
    with conn.cursor() as cur:
        inserted_keys = set(execute_values(cur, INSERT_TRADES_SQL, rows, fetch=True))
    return [t for t in unique if (t.source, t.symbol, t.trade_id) in inserted_keys]


def upsert_candles(conn: PgConnection, candles: Sequence[Candle]) -> None:
    if not candles:
        return
    rows = [
        (c.source, c.symbol, c.bucket_start, c.open, c.high, c.low, c.close, c.volume,
         c.notional, c.trade_count, c.first_trade_time, c.last_trade_time)
        for c in candles
    ]
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_CANDLES_SQL, rows)
