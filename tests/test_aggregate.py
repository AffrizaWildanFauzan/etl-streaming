from datetime import UTC, datetime, timedelta
from decimal import Decimal

from processing.aggregate import aggregate_candles, bucket_start
from processing.transform import Trade

BASE = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)


def trade(symbol: str, seconds: float, price: str, qty: str, trade_id: str) -> Trade:
    p, q = Decimal(price), Decimal(qty)
    return Trade(
        source="binance", symbol=symbol, trade_id=trade_id, price=p, quantity=q,
        notional=p * q, side="BUY", trade_time=BASE + timedelta(seconds=seconds),
        ingested_at=BASE,
    )


def test_bucket_start_floors_to_minute():
    ts = datetime(2026, 9, 27, 12, 3, 59, 999000, tzinfo=UTC)
    assert bucket_start(ts) == datetime(2026, 9, 27, 12, 3, tzinfo=UTC)


def test_empty_input_gives_no_candles():
    assert aggregate_candles([]) == []


def test_ohlcv_is_computed_by_trade_time_not_arrival_order():
    trades = [
        trade("BTCUSDT", 30, "102", "1", "c"),
        trade("BTCUSDT", 5, "100", "2", "a"),   # datang belakangan tapi paling awal
        trade("BTCUSDT", 50, "99", "1", "d"),
        trade("BTCUSDT", 10, "105", "1", "b"),
    ]

    [candle] = aggregate_candles(trades)

    assert candle.bucket_start == BASE
    assert candle.open == Decimal("100")
    assert candle.high == Decimal("105")
    assert candle.low == Decimal("99")
    assert candle.close == Decimal("99")
    assert candle.volume == Decimal("5")
    assert candle.notional == Decimal("506")
    assert candle.trade_count == 4
    assert candle.first_trade_time == BASE + timedelta(seconds=5)
    assert candle.last_trade_time == BASE + timedelta(seconds=50)


def test_trades_are_split_per_symbol_and_minute():
    trades = [
        trade("BTCUSDT", 1, "100", "1", "1"),
        trade("BTCUSDT", 61, "101", "1", "2"),
        trade("ETHUSDT", 2, "10", "1", "3"),
    ]

    candles = aggregate_candles(trades)
    keys = sorted((c.symbol, c.bucket_start) for c in candles)

    assert keys == [
        ("BTCUSDT", BASE),
        ("BTCUSDT", BASE + timedelta(minutes=1)),
        ("ETHUSDT", BASE),
    ]
