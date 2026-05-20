"""Unit tests for exchange-specific message parsing."""

from __future__ import annotations

from decimal import Decimal

from market_data_collector.config import Settings
from market_data_collector.exchanges.binance import BinanceAdapter, _binance_to_normalized
from market_data_collector.exchanges.coinbase import CoinbaseAdapter
from market_data_collector.models import Exchange, Side


def _settings() -> Settings:
    return Settings(_env_file=None)  # type: ignore[call-arg]


def test_binance_symbol_split() -> None:
    assert _binance_to_normalized("BTCUSDT") == "BTC-USDT"
    assert _binance_to_normalized("ETHBTC") == "ETH-BTC"
    assert _binance_to_normalized("SOLEUR") == "SOL-EUR"


def test_binance_parses_trade_message() -> None:
    adapter = BinanceAdapter(["btcusdt"], _settings())
    raw = {
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade",
            "E": 1_700_000_000_000,
            "s": "BTCUSDT",
            "t": 12345,
            "p": "42000.50",
            "q": "0.0123",
            "T": 1_700_000_000_000,
            "m": True,
        },
    }
    tick = adapter.parse(raw)
    assert tick is not None
    assert tick.exchange is Exchange.BINANCE
    assert tick.symbol == "BTC-USDT"
    assert tick.price == Decimal("42000.50")
    assert tick.quantity == Decimal("0.0123")
    assert tick.side is Side.SELL
    assert tick.trade_id == "12345"


def test_binance_skips_non_trade_messages() -> None:
    adapter = BinanceAdapter(["btcusdt"], _settings())
    assert adapter.parse({"result": None, "id": 1}) is None
    assert adapter.parse({"data": {"e": "kline"}}) is None


def test_coinbase_parses_match_message() -> None:
    adapter = CoinbaseAdapter(["BTC-USD"], _settings())
    raw = {
        "channel": "market_trades",
        "client_id": "",
        "timestamp": "2024-01-01T00:00:00Z",
        "sequence_num": 1,
        "events": [
            {
                "type": "update",
                "trades": [
                    {
                        "trade_id": "777",
                        "product_id": "BTC-USD",
                        "price": "40000.10",
                        "size": "0.5",
                        "side": "BUY",
                        "time": "2024-01-01T00:00:00Z",
                    }
                ],
            }
        ],
    }
    tick = adapter.parse(raw)
    assert tick is not None
    assert tick.exchange is Exchange.COINBASE
    assert tick.symbol == "BTC-USD"
    assert tick.price == Decimal("40000.10")
    assert tick.side is Side.BUY
    assert tick.trade_id == "777"


def test_coinbase_skips_unrelated_channels() -> None:
    adapter = CoinbaseAdapter(["BTC-USD"], _settings())
    assert adapter.parse({"channel": "heartbeat"}) is None


def test_tick_kafka_key_groups_by_exchange_and_symbol() -> None:
    adapter = BinanceAdapter(["btcusdt"], _settings())
    raw = {
        "data": {
            "e": "trade",
            "s": "BTCUSDT",
            "t": 1,
            "p": "1",
            "q": "1",
            "T": 1_700_000_000_000,
            "m": False,
        }
    }
    tick = adapter.parse(raw)
    assert tick is not None
    assert tick.kafka_key() == b"binance:BTC-USDT"
