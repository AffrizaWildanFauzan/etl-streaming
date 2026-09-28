"""TRANSFORM (agregasi): tumbling window 1 menit -> candle OHLCV per simbol.

Hasil agregasi satu micro-batch bersifat parsial; penggabungan dengan candle
yang sudah ada di database dilakukan oleh upsert di `load.py`.
"""

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from processing.transform import Trade

WINDOW = timedelta(minutes=1)


@dataclass(frozen=True)
class Candle:
    source: str
    symbol: str
    bucket_start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    notional: Decimal
    trade_count: int
    first_trade_time: datetime
    last_trade_time: datetime


def bucket_start(ts: datetime, window: timedelta = WINDOW) -> datetime:
    epoch = datetime(1970, 1, 1, tzinfo=ts.tzinfo)
    offset = (ts - epoch) // window * window
    return epoch + offset


def _build_candle(key: tuple[str, str, datetime], trades: list[Trade]) -> Candle:
    ordered = sorted(trades, key=lambda t: (t.trade_time, t.trade_id))
    source, symbol, start = key
    return Candle(
        source=source,
        symbol=symbol,
        bucket_start=start,
        open=ordered[0].price,
        high=max(t.price for t in ordered),
        low=min(t.price for t in ordered),
        close=ordered[-1].price,
        volume=sum((t.quantity for t in ordered), Decimal(0)),
        notional=sum((t.notional for t in ordered), Decimal(0)),
        trade_count=len(ordered),
        first_trade_time=ordered[0].trade_time,
        last_trade_time=ordered[-1].trade_time,
    )


def aggregate_candles(trades: Iterable[Trade]) -> list[Candle]:
    groups: dict[tuple[str, str, datetime], list[Trade]] = defaultdict(list)
    for trade in trades:
        groups[(trade.source, trade.symbol, bucket_start(trade.trade_time))].append(trade)
    return [_build_candle(key, group) for key, group in groups.items()]
