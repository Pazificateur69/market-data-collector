"""Normalized data models shared across exchange adapters."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class Exchange(StrEnum):
    BINANCE = "binance"
    COINBASE = "coinbase"


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class Tick(BaseModel):
    """Normalized trade tick produced by every exchange adapter."""

    model_config = ConfigDict(frozen=True)

    exchange: Exchange
    symbol: str = Field(description="Normalized symbol, e.g. BTC-USDT")
    price: Decimal
    quantity: Decimal
    side: Side | None = None
    trade_id: str
    event_time: datetime = Field(description="Exchange-provided event timestamp (UTC)")
    received_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Wall-clock receive time on the collector (UTC)",
    )

    def kafka_key(self) -> bytes:
        """Partition key: group ticks per (exchange, symbol) for ordering guarantees."""
        return f"{self.exchange.value}:{self.symbol}".encode()
