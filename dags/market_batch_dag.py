
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator

CONN_ID = "market_db"
INTERVAL = "bucket_start >= '{{ data_interval_start }}' AND bucket_start < '{{ data_interval_end }}'"

FRESHNESS_SQL = """
SELECT count(*) > 0
FROM trades
WHERE loaded_at > now() - make_interval(mins => {{ params.freshness_minutes }})
"""

# Candle valid: low <= open/close <= high, volume > 0. COALESCE -> jam tanpa data dianggap lolos.
CANDLE_INTEGRITY_SQL = f"""
SELECT COALESCE(bool_and(
           low <= high AND open BETWEEN low AND high AND close BETWEEN low AND high
           AND volume > 0 AND trade_count > 0), true)
FROM ohlcv_1m
WHERE {INTERVAL}
"""

# Total volume & jumlah trade di tabel agregat harus sama dengan tabel detail.
TRADES_VS_CANDLES_SQL = f"""
WITH t AS (
    SELECT source, symbol, sum(quantity) AS volume, count(*) AS n
    FROM trades
    WHERE trade_time >= '{{{{ data_interval_start }}}}' AND trade_time < '{{{{ data_interval_end }}}}'
    GROUP BY 1, 2
), c AS (
    SELECT source, symbol, sum(volume) AS volume, sum(trade_count) AS n
    FROM ohlcv_1m
    WHERE {INTERVAL}
    GROUP BY 1, 2
)
-- IS NOT DISTINCT FROM: simbol yang hanya ada di salah satu sisi (NULL) dihitung GAGAL
SELECT COALESCE(bool_and(t.volume IS NOT DISTINCT FROM c.volume
                         AND t.n IS NOT DISTINCT FROM c.n), true)
FROM t FULL JOIN c USING (source, symbol)
"""

ROLLUP_SQL = f"""
INSERT INTO ohlcv_1h (source, symbol, bucket_start, open, high, low, close,
                      volume, notional, trade_count, vwap)
SELECT source, symbol, date_trunc('hour', bucket_start),
       (array_agg(open  ORDER BY bucket_start))[1],
       max(high), min(low),
       (array_agg(close ORDER BY bucket_start DESC))[1],
       sum(volume), sum(notional), sum(trade_count),
       sum(notional) / NULLIF(sum(volume), 0)
FROM ohlcv_1m
WHERE {INTERVAL}
GROUP BY source, symbol, date_trunc('hour', bucket_start)
ON CONFLICT (source, symbol, bucket_start) DO UPDATE SET
    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
    volume = EXCLUDED.volume, notional = EXCLUDED.notional,
    trade_count = EXCLUDED.trade_count, vwap = EXCLUDED.vwap, rolled_up_at = now()
"""

VERIFY_ROLLUP_SQL = f"""
SELECT COALESCE(bool_and(h.volume IS NOT DISTINCT FROM m.volume
                         AND h.trade_count IS NOT DISTINCT FROM m.n), true)
FROM (
    SELECT source, symbol, sum(volume) AS volume, sum(trade_count) AS n
    FROM ohlcv_1m WHERE {INTERVAL} GROUP BY 1, 2
) m
LEFT JOIN ohlcv_1h h
       ON h.source = m.source AND h.symbol = m.symbol
      AND h.bucket_start = '{{{{ data_interval_start }}}}'
"""

PURGE_SQL = """
DELETE FROM trades
WHERE trade_time < now() - make_interval(days => {{ params.retention_days }})
"""

with DAG(
    dag_id="market_batch_layer",
    description="Data quality, rollup OHLCV 1 jam, dan retensi untuk pipeline streaming pasar",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 9, 27, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    params={"freshness_minutes": 5, "retention_days": 7},
    tags=["market", "streaming", "batch-layer"],
) as dag:
    check_stream_freshness = SQLCheckOperator(
        task_id="check_stream_freshness", conn_id=CONN_ID, sql=FRESHNESS_SQL
    )
    check_candle_integrity = SQLCheckOperator(
        task_id="check_candle_integrity", conn_id=CONN_ID, sql=CANDLE_INTEGRITY_SQL
    )
    check_trades_vs_candles = SQLCheckOperator(
        task_id="check_trades_vs_candles", conn_id=CONN_ID, sql=TRADES_VS_CANDLES_SQL
    )
    rollup_ohlcv_1h = SQLExecuteQueryOperator(
        task_id="rollup_ohlcv_1h", conn_id=CONN_ID, sql=ROLLUP_SQL
    )
    verify_rollup = SQLCheckOperator(
        task_id="verify_rollup", conn_id=CONN_ID, sql=VERIFY_ROLLUP_SQL
    )
    purge_old_trades = SQLExecuteQueryOperator(
        task_id="purge_old_trades", conn_id=CONN_ID, sql=PURGE_SQL
    )

    check_stream_freshness >> [check_candle_integrity, check_trades_vs_candles]
    [check_candle_integrity, check_trades_vs_candles] >> rollup_ohlcv_1h
    rollup_ohlcv_1h >> verify_rollup >> purge_old_trades
