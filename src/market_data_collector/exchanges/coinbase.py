"""Coinbase Advanced Trade market-data adapter (public match channel).

Doc: https://docs.cdp.coinbase.com/advanced-trade/docs/ws-channels
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from ..models import Exchange, Side, Tick
from .base import ExchangeAdapter


class CoinbaseAdapter(ExchangeAdapter):
    exchange = Exchange.COINBASE
    _URL = "wss://advanced-trade-ws.coinbase.com"

    def url(self) -> str:
        return self._URL

    def subscribe_payload(self) -> dict[str, Any] | None:
        return {
            "type": "subscribe",
            "product_ids": list(self._symbols),
            "channel": "market_trades",
        }

    def parse(self, raw: dict[str, Any]) -> Tick | None:
        if raw.get("channel") != "market_trades":
            return None

        for event in raw.get("events", []):
            if event.get("type") not in ("snapshot", "update"):
                continue
            trades = event.get("trades", [])
            if not trades:
                continue
            # The newest trade is what we want; older snapshot entries are dropped.
            trade = trades[0]
            return _trade_to_tick(trade)
        return None


def _trade_to_tick(trade: dict[str, Any]) -> Tick:
    side_str = str(trade.get("side", "")).upper()
    side = Side.BUY if side_str == "BUY" else Side.SELL if side_str == "SELL" else None
    return Tick(
        exchange=Exchange.COINBASE,
        symbol=str(trade["product_id"]),
        price=Decimal(str(trade["price"])),
        quantity=Decimal(str(trade["size"])),
        side=side,
        trade_id=str(trade["trade_id"]),
        event_time=datetime.fromisoformat(str(trade["time"]).replace("Z", "+00:00")),
    )
