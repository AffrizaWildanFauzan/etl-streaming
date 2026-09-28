"""Konfigurasi pipeline, dibaca dari environment variable."""

import os
from dataclasses import dataclass


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Environment variable {name} wajib diisi")
    return value or ""


def env_list(name: str, default: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in env(name, default).split(",") if item.strip())


def env_int(name: str, default: int) -> int:
    raw = env(name, str(default))
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} harus berupa angka, didapat: {raw!r}") from exc


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap: str
    raw_topic: str
    dlq_topic: str
    group_id: str


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    db: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.db} "
            f"user={self.user} password={self.password}"
        )

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.db}"
        )


def kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap=env("KAFKA_BOOTSTRAP", "localhost:29092"),
        raw_topic=env("RAW_TOPIC", "market.trades.raw"),
        dlq_topic=env("DLQ_TOPIC", "market.trades.dlq"),
        group_id=env("PROCESSOR_GROUP_ID", "trade-processor"),
    )


def postgres_config() -> PostgresConfig:
    return PostgresConfig(
        host=env("POSTGRES_HOST", "localhost"),
        port=env_int("POSTGRES_PORT", 5434),
        db=env("POSTGRES_DB", "market"),
        user=env("POSTGRES_USER", "etl"),
        password=env("POSTGRES_PASSWORD", required=True),
    )
