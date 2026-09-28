"""TRANSFORM: parsing, validasi, normalisasi, dan enrichment data trade mentah.

Setiap sumber punya format berbeda; semuanya diubah ke satu skema `Trade`.
Record yang tidak lolos validasi melempar `InvalidRecord` -> dikirim ke DLQ.
"""

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation

MAX_CLOCK_SKEW = timedelta(minutes=5)
MIN_VALID_TIME = datetime(2010, 1, 1, tzinfo=UTC)
FINNHUB_ID_LENGTH = 24


class InvalidRecord(ValueError):
    """Record gagal validasi; pesan error menjelaskan alasannya."""


@dataclass(frozen=True)
class Trade:
    source: str
    symbol: str
    trade_id: str
    price: Decimal
    quantity: Decimal
    notional: Decimal
    side: str | None  # BUY / SELL (aggressor), None jika sumber tidak menyediakan
    trade_time: datetime
    ingested_at: datetime


def _field(payload: dict, key: str):
    if key not in payload:
        raise InvalidRecord(f"field wajib '{key}' tidak ada")
    return payload[key]


def _positive_decimal(value, name: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise InvalidRecord(f"{name} bukan angka: {value!r}") from exc
    if not number.is_finite() or number <= 0:
        raise InvalidRecord(f"{name} harus > 0, didapat {value!r}")
    return number


def _epoch_ms_to_datetime(value, name: str) -> datetime:
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=UTC)
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        raise InvalidRecord(f"{name} bukan epoch ms valid: {value!r}") from exc


def _symbol(value) -> str:
    symbol = str(value).strip().upper()
    if not symbol:
        raise InvalidRecord("symbol kosong")
    return symbol


def _validate_time(trade_time: datetime, now: datetime) -> None:
    if trade_time > now + MAX_CLOCK_SKEW:
        raise InvalidRecord(f"trade_time di masa depan (future): {trade_time.isoformat()}")
    if trade_time < MIN_VALID_TIME:
        raise InvalidRecord(f"trade_time terlalu lama: {trade_time.isoformat()}")


def _build_trade(source, symbol, trade_id, price, quantity, side, trade_time, ingested_at, now):
    _validate_time(trade_time, now)
    return Trade(
        source=source,
        symbol=symbol,
        trade_id=trade_id,
        price=price,
        quantity=quantity,
        notional=price * quantity,
        side=side,
        trade_time=trade_time,
        ingested_at=ingested_at,
    )


def parse_binance(payload: dict, ingested_at: datetime, now: datetime) -> Trade:
    # m = true -> buyer adalah maker -> trade dipicu oleh seller (SELL aggressor)
    return _build_trade(
        source="binance",
        symbol=_symbol(_field(payload, "s")),
        trade_id=str(_field(payload, "t")),
        price=_positive_decimal(_field(payload, "p"), "price"),
        quantity=_positive_decimal(_field(payload, "q"), "quantity"),
        side="SELL" if _field(payload, "m") else "BUY",
        trade_time=_epoch_ms_to_datetime(_field(payload, "T"), "T"),
        ingested_at=ingested_at,
        now=now,
    )


def _finnhub_trade_id(payload: dict) -> str:
    """Finnhub tidak memberi trade id -> buat id deterministik agar deduplikasi tetap jalan."""
    key = f"{payload['s']}|{payload['t']}|{payload['p']}|{payload['v']}"
    return hashlib.sha1(key.encode()).hexdigest()[:FINNHUB_ID_LENGTH]


def parse_finnhub(payload: dict, ingested_at: datetime, now: datetime) -> Trade:
    for key in ("s", "p", "t", "v"):
        _field(payload, key)
    return _build_trade(
        source="finnhub",
        symbol=_symbol(payload["s"]),
        trade_id=_finnhub_trade_id(payload),
        price=_positive_decimal(payload["p"], "price"),
        quantity=_positive_decimal(payload["v"], "quantity"),
        side=None,
        trade_time=_epoch_ms_to_datetime(payload["t"], "t"),
        ingested_at=ingested_at,
        now=now,
    )


PARSERS: dict[str, Callable[[dict, datetime, datetime], Trade]] = {
    "binance": parse_binance,
    "finnhub": parse_finnhub,
}


def _decode_envelope(raw: bytes) -> dict:
    try:
        envelope = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidRecord(f"bukan JSON valid: {exc}") from exc
    if not isinstance(envelope, dict):
        raise InvalidRecord("envelope harus berupa object JSON")
    for key in ("source", "ingested_at", "payload"):
        _field(envelope, key)
    if not isinstance(envelope["payload"], dict):
        raise InvalidRecord("payload harus berupa object JSON")
    return envelope


def transform_message(raw: bytes, now: datetime | None = None) -> Trade:
    """Ubah satu pesan Kafka (bytes) menjadi `Trade` yang sudah bersih."""
    envelope = _decode_envelope(raw)
    parser = PARSERS.get(envelope["source"])
    if parser is None:
        raise InvalidRecord(f"source tidak dikenal: {envelope['source']!r}")
    ingested_at = _epoch_ms_to_datetime(envelope["ingested_at"], "ingested_at")
    return parser(envelope["payload"], ingested_at, now or datetime.now(UTC))
