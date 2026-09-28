-- Skema data warehouse untuk pipeline ETL streaming pasar finansial.

CREATE TABLE IF NOT EXISTS trades (
    source       TEXT           NOT NULL,
    symbol       TEXT           NOT NULL,
    trade_id     TEXT           NOT NULL,
    price        NUMERIC(28,10) NOT NULL CHECK (price > 0),
    quantity     NUMERIC(28,10) NOT NULL CHECK (quantity > 0),
    notional     NUMERIC(38,10) NOT NULL,
    side         TEXT           CHECK (side IN ('BUY', 'SELL')),
    trade_time   TIMESTAMPTZ    NOT NULL,   -- waktu transaksi di bursa
    ingested_at  TIMESTAMPTZ    NOT NULL,   -- waktu diterima producer
    loaded_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),  -- waktu masuk database
    PRIMARY KEY (source, symbol, trade_id)
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON trades (symbol, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_loaded_at   ON trades (loaded_at DESC);

CREATE TABLE IF NOT EXISTS ohlcv_1m (
    source           TEXT           NOT NULL,
    symbol           TEXT           NOT NULL,
    bucket_start     TIMESTAMPTZ    NOT NULL,
    open             NUMERIC(28,10) NOT NULL,
    high             NUMERIC(28,10) NOT NULL,
    low              NUMERIC(28,10) NOT NULL,
    close            NUMERIC(28,10) NOT NULL,
    volume           NUMERIC(38,10) NOT NULL,
    notional         NUMERIC(38,10) NOT NULL,
    trade_count      BIGINT         NOT NULL,
    first_trade_time TIMESTAMPTZ    NOT NULL,
    last_trade_time  TIMESTAMPTZ    NOT NULL,
    updated_at       TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (source, symbol, bucket_start)
);

-- VWAP (Volume Weighted Average Price) = total nilai transaksi / total volume
CREATE OR REPLACE VIEW v_ohlcv_1m AS
SELECT o.*, notional / NULLIF(volume, 0) AS vwap
FROM ohlcv_1m o;

-- Ringkasan terbaru per simbol
CREATE OR REPLACE VIEW v_latest_price AS
SELECT DISTINCT ON (source, symbol)
       source, symbol, price, side, trade_time
FROM trades
ORDER BY source, symbol, trade_time DESC;

-- Diisi oleh DAG Airflow `market_batch_layer` (rollup per jam dari ohlcv_1m)
CREATE TABLE IF NOT EXISTS ohlcv_1h (
    source       TEXT           NOT NULL,
    symbol       TEXT           NOT NULL,
    bucket_start TIMESTAMPTZ    NOT NULL,
    open         NUMERIC(28,10) NOT NULL,
    high         NUMERIC(28,10) NOT NULL,
    low          NUMERIC(28,10) NOT NULL,
    close        NUMERIC(28,10) NOT NULL,
    volume       NUMERIC(38,10) NOT NULL,
    notional     NUMERIC(38,10) NOT NULL,
    trade_count  BIGINT         NOT NULL,
    vwap         NUMERIC(28,10),
    rolled_up_at TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (source, symbol, bucket_start)
);
