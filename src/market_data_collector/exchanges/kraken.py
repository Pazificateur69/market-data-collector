"""Kraken v2 trade-channel adapter.

Doc: https://docs.kraken.com/api/docs/websocket-v2/trade
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from ..models import Exchange, Side, Tick
from .base import ExchangeAdapter


class KrakenAdapter(ExchangeAdapter):
    exchange = Exchange.KRAKEN
    _URL = "wss://ws.kraken.com/v2"

    def url(self) -> str:
        return self._URL

    def subscribe_payload(self) -> dict[str, Any] | None:
        return {
            "method": "subscribe",
            "params": {"channel": "trade", "symbol": list(self._symbols)},
        }

    def parse(self, raw: dict[str, Any]) -> Tick | None:
        if raw.get("channel") != "trade" or raw.get("type") not in ("update", "snapshot"):
            return None
        trades = raw.get("data", [])
        if not trades:
            return None
        # Kraken delivers an array of trades per frame; the most recent is what we want.
        trade = trades[-1]
        side_str = str(trade.get("side", "")).lower()
        side = Side.BUY if side_str == "buy" else Side.SELL if side_str == "sell" else None
        return Tick(
            exchange=self.exchange,
            symbol=str(trade["symbol"]),
            price=Decimal(str(trade["price"])),
            quantity=Decimal(str(trade["qty"])),
            side=side,
            trade_id=str(trade["trade_id"]),
            event_time=datetime.fromisoformat(str(trade["timestamp"]).replace("Z", "+00:00")),
        )
