"""Dashboard real-time hasil ETL streaming (Streamlit)."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text

from common.config import postgres_config

st.set_page_config(page_title="Market Stream ETL", layout="wide")

KPI_SQL = text("""
SELECT
  (SELECT count(*) FROM trades)                                          AS total_trades,
  (SELECT count(*) FROM trades WHERE loaded_at > now() - interval '60 seconds') AS trades_60s,
  (SELECT avg(extract(epoch FROM loaded_at - trade_time)) * 1000
     FROM trades WHERE loaded_at > now() - interval '60 seconds')       AS latency_ms,
  (SELECT count(DISTINCT symbol) FROM trades)                            AS symbols
""")

SYMBOLS_SQL = text("SELECT DISTINCT source, symbol FROM ohlcv_1m ORDER BY source, symbol")

CANDLES_SQL = text("""
SELECT bucket_start, open, high, low, close, volume, vwap, trade_count
FROM v_ohlcv_1m
WHERE source = :source AND symbol = :symbol
  AND bucket_start > now() - make_interval(mins => :minutes)
ORDER BY bucket_start
""")

SUMMARY_SQL = text("""
SELECT l.source, l.symbol, l.price AS last_price,
       round(100 * (l.price - f.open) / f.open, 3) AS change_pct,
       f.volume, f.trades
FROM v_latest_price l
JOIN LATERAL (
    SELECT (array_agg(open ORDER BY bucket_start))[1] AS open,
           sum(volume) AS volume, sum(trade_count) AS trades
    FROM ohlcv_1m o
    WHERE o.source = l.source AND o.symbol = l.symbol
      AND o.bucket_start > now() - make_interval(mins => :minutes)
) f ON true
ORDER BY l.source, l.symbol
""")

RECENT_SQL = text("""
SELECT trade_time, source, symbol, side, price, quantity, notional,
       round(extract(epoch FROM loaded_at - trade_time) * 1000) AS latency_ms
FROM trades ORDER BY loaded_at DESC LIMIT 25
""")


@st.cache_resource
def get_engine():
    return create_engine(postgres_config().sqlalchemy_url, pool_pre_ping=True)


def query(sql, **params) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn, params=params)


def candle_chart(df: pd.DataFrame, title: str) -> go.Figure:
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.75, 0.25],
                        vertical_spacing=0.03)
    fig.add_trace(go.Candlestick(x=df.bucket_start, open=df.open, high=df.high, low=df.low,
                                 close=df.close, name="OHLC"), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.bucket_start, y=df.vwap, name="VWAP",
                             line={"width": 1.5, "dash": "dot"}), row=1, col=1)
    fig.add_trace(go.Bar(x=df.bucket_start, y=df.volume, name="Volume"), row=2, col=1)
    fig.update_layout(title=title, height=560, xaxis_rangeslider_visible=False,
                      margin={"l": 10, "r": 10, "t": 40, "b": 10})
    return fig


def render_kpis() -> None:
    kpi = query(KPI_SQL).iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total trade dimuat", f"{int(kpi.total_trades):,}")
    c2.metric("Throughput (60 dtk)", f"{int(kpi.trades_60s):,} trade")
    latency = "-" if pd.isna(kpi.latency_ms) else f"{kpi.latency_ms:,.0f} ms"
    c3.metric("Latency end-to-end", latency)
    c4.metric("Jumlah simbol", int(kpi.symbols))


def render_market(minutes: int) -> None:
    pairs = query(SYMBOLS_SQL)
    if pairs.empty:
        st.info("Belum ada data. Tunggu producer & processor berjalan beberapa detik...")
        return
    labels = [f"{r.source}:{r.symbol}" for r in pairs.itertuples()]
    choice = st.selectbox("Simbol", labels, key="symbol")
    source, symbol = choice.split(":", 1)

    candles = query(CANDLES_SQL, source=source, symbol=symbol, minutes=minutes)
    if not candles.empty:
        st.plotly_chart(candle_chart(candles, f"{symbol} - candle 1 menit"),
                        use_container_width=True)

    left, right = st.columns([1, 1])
    left.subheader(f"Ringkasan {minutes} menit terakhir")
    left.dataframe(query(SUMMARY_SQL, minutes=minutes), hide_index=True,
                   use_container_width=True)
    right.subheader("Trade terbaru")
    right.dataframe(query(RECENT_SQL), hide_index=True, use_container_width=True)


st.title("Real-time Market ETL Streaming")
st.caption("Binance / Finnhub WebSocket → Kafka → Processor → PostgreSQL → Dashboard")

with st.sidebar:
    window_minutes = st.slider("Jendela waktu (menit)", 5, 240, 30)
    refresh_seconds = st.slider("Auto-refresh (detik)", 2, 30, 5)


@st.fragment(run_every=refresh_seconds)
def live_view() -> None:
    try:
        render_kpis()
        render_market(window_minutes)
    except Exception as exc:  # koneksi DB putus dsb. -> tampilkan, jangan crash
        st.error(f"Gagal membaca database: {exc}")


live_view()
