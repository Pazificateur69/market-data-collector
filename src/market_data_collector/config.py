"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    binance_symbols: list[str] = Field(default=["btcusdt", "ethusdt"])
    coinbase_symbols: list[str] = Field(default=["BTC-USD", "ETH-USD"])

    # Kafka
    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    kafka_topic: str = Field(default="market.ticks")
    kafka_client_id: str = Field(default="market-data-collector")
    kafka_linger_ms: int = Field(default=5)
    kafka_compression: str = Field(default="lz4")
    kafka_acks: str = Field(default="1")

    # Backoff policy for WebSocket reconnects.
    backoff_initial_seconds: float = Field(default=1.0, ge=0.1)
    backoff_max_seconds: float = Field(default=60.0, ge=1.0)
    backoff_multiplier: float = Field(default=2.0, ge=1.0)

    # Health & misc
    log_level: str = Field(default="INFO")
    metrics_log_interval_seconds: float = Field(default=30.0, ge=1.0)

    @field_validator("binance_symbols", "coinbase_symbols", mode="before")
    @classmethod
    def _split_csv(cls, value: object) -> object:
        if isinstance(value, str):
            return [s.strip() for s in value.split(",") if s.strip()]
        return value
