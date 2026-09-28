# Real-time Market ETL Streaming

Pipeline **ETL streaming** untuk data pasar finansial secara real-time:
trade kripto dari **Binance Public Stream** (tanpa API key) dan, opsional, trade
**saham US** dari **Finnhub** diambil lewat WebSocket, dialirkan melalui **Apache Kafka**,
dibersihkan dan diagregasi oleh stream processor Python, disimpan ke **PostgreSQL**,
diorkestrasi tugas batch-nya oleh **Apache Airflow**, lalu divisualisasikan di **Streamlit**.

## Struktur project

```
├── ingestion/                 # EXTRACT: WebSocket → Kafka
│   ├── sources/binance.py     #   Binance Public Stream (kripto)
│   ├── sources/finnhub.py     #   Finnhub (saham US, opsional)
│   └── producer.py            #   publish data mentah ke topic market.trades.raw
├── processing/                # TRANSFORM (stream processing)
│   ├── transform.py           #   parsing, validasi, normalisasi, enrichment
│   ├── aggregate.py           #   tumbling window 1 menit → candle OHLCV
│   └── processor.py           #   Kafka consumer micro-batch → storage
├── storage/                   # LOAD
│   ├── load.py                #   insert idempotent + upsert candle
│   └── schema/                #   DDL PostgreSQL (dijalankan otomatis saat init)
│       ├── 01_schema.sql      #     trades, ohlcv_1m, ohlcv_1h, view VWAP
│       └── 02_airflow_db.sql  #     database metadata Airflow
├── dags/
│   └── market_batch_dag.py    # DAG Airflow: data quality, rollup 1 jam, retensi
├── infrastructure/
│   ├── docker-compose.yml     # Kafka, PostgreSQL, Airflow, producer, processor, dashboard
│   ├── Dockerfile             # image Python untuk ingestion/processing/dashboard
│   └── .env.example           # template konfigurasi
├── common/config.py           # konfigurasi dari environment variable
├── dashboard/app.py           # visualisasi real-time
├── tests/                     # unit test
└── Makefile                   # perintah singkat (make up, make test, ...)
```

## Arsitektur

```
 INGESTION                 BUFFER                PROCESSING                    STORAGE              SERVING
┌───────────────┐      ┌──────────────┐      ┌────────────────────────┐     ┌──────────────┐     ┌───────────┐
│ Binance WS    │──┐   │ Kafka        │      │ processor              │     │ PostgreSQL   │     │ Streamlit │
│ (kripto 24/7) │  ├──▶│ market.      │─────▶│ • validasi & normalisasi│────▶│ trades       │────▶│ :8501     │
├───────────────┤  │   │ trades.raw   │      │ • enrichment           │     │ ohlcv_1m     │     └───────────┘
│ Finnhub WS    │──┘   │ (3 partisi)  │      │ • window OHLCV 1 menit │     │ ohlcv_1h ◀─┐ │
│ (saham, opsi) │      └──────────────┘      └───────────┬────────────┘     └────────────┼─┘
└───────────────┘                                       ▼                               │
                                             market.trades.dlq             ┌────────────┴──────────┐
                                             (record tidak valid)          │ Airflow DAG (per jam) │
                                                                           │ quality check, rollup,│
                                                                           │ retensi   :8081       │
                                                                           └───────────────────────┘
```

**Speed layer** (streaming, detik): ingestion → Kafka → processing → `trades` & `ohlcv_1m`.
**Batch layer** (Airflow, per jam): validasi kualitas data, rollup ke `ohlcv_1h`, dan retensi.

## Detail setiap tahap

### 1. Ingestion (Extract)
- Binance: combined stream `<symbol>@trade` untuk BTC, ETH, BNB, SOL, XRP (bisa diubah di `.env`).
  Endpoint yang dipakai `data-stream.binance.vision`, karena `stream.binance.com` diblokir sebagian ISP di Indonesia.
- Finnhub: subscribe simbol saham (AAPL, MSFT, NVDA, TSLA, AMZN). Satu pesan bisa berisi banyak trade, jadi dipecah per trade.
- Setiap pesan dibungkus envelope `{source, ingested_at, payload}` dan dikirim ke Kafka dengan
  **key = simbol**, sehingga urutan per simbol terjaga di partisi yang sama.
- Producer memakai `acks=all` + idempotence, dan **reconnect otomatis** dengan exponential backoff.

### 2. Processing (Transform)
- **Validasi:** JSON valid, field wajib ada, `price > 0`, `quantity > 0`, simbol tidak kosong,
  timestamp tidak di masa depan. Record yang gagal dikirim ke **Dead Letter Queue** beserta alasannya.
- **Normalisasi:** dua format sumber diubah ke satu skema `Trade`
  (harga dan kuantitas memakai `Decimal` agar presisi finansial terjaga, waktu dalam UTC).
- **Enrichment:** `notional = price × quantity`, dan `side` (BUY/SELL aggressor) dari flag `m` Binance.
  Finnhub tidak punya trade id, jadi dibuat id deterministik (hash) untuk deduplikasi.
- **Agregasi:** tumbling window **1 menit** berbasis *event time* → OHLCV, jumlah trade, notional, dan **VWAP**.

### 3. Storage (Load)
- `trades`: `INSERT ... ON CONFLICT DO NOTHING` dengan PK `(source, symbol, trade_id)`, sehingga duplikat terbuang.
- `ohlcv_1m`: **upsert** yang menggabungkan candle parsial tiap batch.
  Hanya trade yang benar-benar baru (hasil `RETURNING`) yang diagregasi.
- Offset Kafka di-commit **setelah** transaksi database sukses.
  Hasilnya: **at-least-once + load idempotent = efektif exactly-once**.

### 4. Orkestrasi (DAG Airflow `market_batch_layer`, tiap jam)

```
check_stream_freshness ──┬── check_candle_integrity  ──┬── rollup_ohlcv_1h ── verify_rollup ── purge_old_trades
                         └── check_trades_vs_candles ──┘
```

| Task | Fungsi |
|---|---|
| `check_stream_freshness` | Gagal jika tidak ada trade masuk dalam 5 menit terakhir (pipeline streaming mati) |
| `check_candle_integrity` | Setiap candle: `low ≤ open, close ≤ high`, volume > 0 |
| `check_trades_vs_candles` | Total volume dan jumlah trade di `ohlcv_1m` sama dengan di `trades` |
| `rollup_ohlcv_1h` | Agregasi candle 1 menit → 1 jam (upsert, aman di-rerun/backfill) |
| `verify_rollup` | Hasil rollup konsisten dengan sumbernya |
| `purge_old_trades` | Retensi: hapus trade mentah > 7 hari (candle tetap disimpan) |

Semua query memakai `data_interval_start/end` Airflow, jadi idempotent dan bisa di-backfill.
Parameter `freshness_minutes` dan `retention_days` bisa diubah saat trigger DAG.

## Cara menjalankan

Prasyarat: Docker Desktop.

```bash
make env        # membuat infrastructure/.env → ganti POSTGRES_PASSWORD & AIRFLOW_ADMIN_PASSWORD
make up         # build & jalankan seluruh stack
```

| Layanan | URL |
|---|---|
| Dashboard | http://localhost:8501 |
| Airflow (user `admin`, password dari `.env`) | http://localhost:8081 |
| Kafka UI (lihat topic & pesan) | http://localhost:8080 |
| PostgreSQL | `localhost:5434` (user/db sesuai `.env`) |

Perintah lain:

```bash
make ps                   # status service
make logs s=processor     # log satu service
make dag-test             # jalankan DAG sekali untuk jam berjalan
make up-stocks            # + producer saham US (isi FINNHUB_API_KEY dulu)
make down                 # stop (data tetap ada)
make clean                # stop + hapus semua data
```

> Finnhub: daftar API key gratis di https://finnhub.io/register. Trade saham US hanya muncul
> saat bursa buka (± 20.30–03.00 WIB). Di luar jam itu, simbol `BINANCE:BTCUSDT` tetap mengalir.

## Pengujian

```bash
pip install -r requirements-dev.txt
make test
```

Unit test mencakup normalisasi kedua sumber, aturan validasi, dan perhitungan OHLCV
(termasuk trade yang datang tidak berurutan). Coverage: transform 95%, aggregate 100%.

Pengujian end-to-end yang sudah dilakukan:

1. **Alur data:** ± 1.100 trade/menit dari 5 simbol masuk ke PostgreSQL dengan latency end-to-end rata-rata ± 650 ms.
2. **Konsistensi:** total volume dan jumlah trade di `ohlcv_1m` sama persis dengan di `trades`.
3. **Dead Letter Queue:** pesan bukan JSON dan harga negatif ditolak, lalu muncul di `market.trades.dlq` beserta alasannya.
4. **Idempotensi:** offset consumer di-reset sehingga semua pesan di-replay. Hasilnya 0 baris baru dan candle tetap konsisten.
5. **DAG Airflow:** keenam task sukses, dan hasil rollup `ohlcv_1h` cocok dengan `ohlcv_1m`.
   Uji negatif: candle rusak (high < low) membuat `check_candle_integrity` gagal, dan candle tanpa trade
   membuat `check_trades_vs_candles` gagal.
