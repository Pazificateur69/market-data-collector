"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

KafkaAcks = Literal[0, 1, -1, "all"]

# NoDecode skips pydantic-settings' default JSON parsing for list[str] env vars so we
# can accept comma-separated strings via our own validator.
SymbolList = Annotated[list[str], NoDecode]


class Settings(BaseSettings):
    """Application settings. All values can be overridden via environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="MDC_",
        case_sensitive=False,
        extra="ignore",
    )

    # Symbols traded on each exchange. Format follows the exchange's native convention.
    binance_symbols: SymbolList = Field(default=["btcusdt", "ethusdt"])
    coinbase_symbols: SymbolList = Field(default=["BTC-USD", "ETH-USD"])
    kraken_symbols: SymbolList = Field(default=["XBT/USD", "ETH/USD"])

    # Kafka
    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    kafka_topic: str = Field(default="market.ticks")
    kafka_client_id: str = Field(default="market-data-collector")
    kafka_linger_ms: int = Field(default=5)
    kafka_compression: str = Field(default="lz4")
    kafka_acks: KafkaAcks = Field(default=1)

    # Backoff policy for WebSocket reconnects.
    backoff_initial_seconds: float = Field(default=1.0, ge=0.1)
    backoff_max_seconds: float = Field(default=60.0, ge=1.0)
    backoff_multiplier: float = Field(default=2.0, ge=1.0)

    # In-memory queue between WS adapters and Kafka producer (backpressure isolation).
    queue_max_size: int = Field(default=10_000, ge=1)

    # Metrics
    metrics_enabled: bool = Field(default=True)
    metrics_port: int = Field(default=9100, ge=1, le=65_535)

    # Health & misc
    log_level: str = Field(default="INFO")
    metrics_log_interval_seconds: float = Field(default=30.0, ge=1.0)

    @field_validator("binance_symbols", "coinbase_symbols", "kraken_symbols", mode="before")
    @classmethod
    def _split_csv(cls, value: object) -> object:
        if isinstance(value, str):
            return [s.strip() for s in value.split(",") if s.strip()]
        return value

    @field_validator("kafka_acks", mode="before")
    @classmethod
    def _coerce_acks(cls, value: object) -> object:
        """Accept the broker-side string forms ("0", "1", "-1", "all") from env."""
        if isinstance(value, str):
            stripped = value.strip().lower()
            if stripped == "all":
                return "all"
            try:
                return int(stripped)
            except ValueError:
                pass
        return value
