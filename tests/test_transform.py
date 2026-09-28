import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from processing.transform import InvalidRecord, transform_message

NOW = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)
NOW_MS = int(NOW.timestamp() * 1000)


def envelope(source: str, payload: dict) -> bytes:
    return json.dumps({"source": source, "ingested_at": NOW_MS, "payload": payload}).encode()


def binance_payload(**overrides) -> dict:
    base = {"e": "trade", "s": "BTCUSDT", "t": 42, "p": "84564.10", "q": "0.5",
            "T": NOW_MS - 1000, "m": True}
    return {**base, **overrides}


def finnhub_payload(**overrides) -> dict:
    base = {"s": "AAPL", "p": 230.25, "t": NOW_MS - 500, "v": 10, "c": ["1"]}
    return {**base, **overrides}


def test_binance_trade_is_normalized():
    trade = transform_message(envelope("binance", binance_payload()), now=NOW)

    assert trade.source == "binance"
    assert trade.symbol == "BTCUSDT"
    assert trade.trade_id == "42"
    assert trade.price == Decimal("84564.10")
    assert trade.quantity == Decimal("0.5")
    assert trade.notional == Decimal("42282.050")
    assert trade.trade_time == datetime(2026, 9, 27, 11, 59, 59, tzinfo=UTC)
    assert trade.ingested_at == NOW


def test_binance_buyer_maker_means_sell_side():
    assert transform_message(envelope("binance", binance_payload(m=True)), now=NOW).side == "SELL"
    assert transform_message(envelope("binance", binance_payload(m=False)), now=NOW).side == "BUY"


def test_finnhub_trade_is_normalized_with_deterministic_id():
    raw = envelope("finnhub", finnhub_payload())

    first = transform_message(raw, now=NOW)
    second = transform_message(raw, now=NOW)

    assert first.symbol == "AAPL"
    assert first.price == Decimal("230.25")
    assert first.quantity == Decimal("10")
    assert first.side is None
    assert first.trade_id == second.trade_id


def test_finnhub_exchange_prefix_is_kept_in_symbol():
    trade = transform_message(envelope("finnhub", finnhub_payload(s="BINANCE:BTCUSDT")), now=NOW)
    assert trade.symbol == "BINANCE:BTCUSDT"


@pytest.mark.parametrize("raw", [b"not json", b"[]", b'{"source": "binance"}'])
def test_malformed_envelope_is_rejected(raw):
    with pytest.raises(InvalidRecord):
        transform_message(raw, now=NOW)


def test_unknown_source_is_rejected():
    with pytest.raises(InvalidRecord, match="source"):
        transform_message(envelope("nyse", binance_payload()), now=NOW)


@pytest.mark.parametrize("overrides, reason", [
    ({"p": "0"}, "price"),
    ({"p": "-1"}, "price"),
    ({"q": "0"}, "quantity"),
    ({"p": "abc"}, "price"),
    ({"s": ""}, "symbol"),
    ({"T": NOW_MS + 10 * 60 * 1000}, "future"),
])
def test_invalid_binance_values_are_rejected(overrides, reason):
    with pytest.raises(InvalidRecord, match=reason):
        transform_message(envelope("binance", binance_payload(**overrides)), now=NOW)


def test_missing_field_is_rejected():
    payload = binance_payload()
    del payload["p"]
    with pytest.raises(InvalidRecord, match="p"):
        transform_message(envelope("binance", payload), now=NOW)


def test_finnhub_zero_volume_is_rejected():
    with pytest.raises(InvalidRecord, match="quantity"):
        transform_message(envelope("finnhub", finnhub_payload(v=0)), now=NOW)
