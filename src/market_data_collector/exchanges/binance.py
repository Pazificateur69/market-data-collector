"""Binance trade-stream adapter.

Doc: https://binance-docs.github.io/apidocs/spot/en/#trade-streams
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from ..models import Exchange, Side, Tick
from .base import ExchangeAdapter


class BinanceAdapter(ExchangeAdapter):
    exchange = Exchange.BINANCE
    _BASE_URL = "wss://stream.binance.com:9443/stream"

    def url(self) -> str:
        streams = "/".join(f"{s.lower()}@trade" for s in self._symbols)
        return f"{self._BASE_URL}?streams={streams}"

    def subscribe_payload(self) -> dict[str, Any] | None:
        # Binance accepts the combined-stream URL directly; no explicit SUBSCRIBE needed.
        return None

    def parse(self, raw: dict[str, Any]) -> Tick | None:
        data = raw.get("data") if "data" in raw else raw
        if not isinstance(data, dict) or data.get("e") != "trade":
            return None

        symbol_raw = str(data["s"]).upper()
        symbol = _binance_to_normalized(symbol_raw)
        # Binance flag: m == True means the buyer is the market maker -> taker SOLD.
        side = Side.SELL if bool(data.get("m")) else Side.BUY
        event_ms = int(data["T"])

        return Tick(
            exchange=self.exchange,
            symbol=symbol,
            price=Decimal(str(data["p"])),
            quantity=Decimal(str(data["q"])),
            side=side,
            trade_id=str(data["t"]),
            event_time=datetime.fromtimestamp(event_ms / 1000, tz=UTC),
        )


_QUOTE_CURRENCIES = ("USDT", "USDC", "BUSD", "BTC", "ETH", "EUR", "USD", "TRY", "FDUSD")


def _binance_to_normalized(symbol: str) -> str:
    """Split a concatenated Binance symbol into BASE-QUOTE form."""
    for quote in _QUOTE_CURRENCIES:
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return f"{symbol[: -len(quote)]}-{quote}"
    return symbol
