"""Tests for the bounded-queue backpressure behavior in Collector._ingest."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from market_data_collector.collector import Collector
from market_data_collector.config import Settings
from market_data_collector.exchanges.base import ExchangeAdapter
from market_data_collector.models import Exchange, Tick


class _FixedStreamAdapter(ExchangeAdapter):
    """Adapter that yields a fixed list of ticks then hangs forever."""

    exchange = Exchange.BINANCE

    def __init__(self, ticks: list[Tick]) -> None:
        self._ticks = ticks
        self._symbols = ["btcusdt"]

    def url(self) -> str:
        return ""

    def subscribe_payload(self) -> dict[str, Any] | None:
        return None

    def parse(self, raw: dict[str, Any]) -> Tick | None:
        return None

    async def stream(self) -> AsyncIterator[Tick]:
        for tick in self._ticks:
            yield tick
        # Hold the task open so the queue isn't drained by task completion.
        await asyncio.Event().wait()


def _make_tick(i: int) -> Tick:
    return Tick(
        exchange=Exchange.BINANCE,
        symbol="BTC-USDT",
        price=Decimal("1"),
        quantity=Decimal("1"),
        trade_id=str(i),
        event_time=datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_ingest_drops_oldest_when_queue_full() -> None:
    """When the queue is full, _ingest drops the oldest entry (FIFO eviction)."""
    settings = Settings(_env_file=None, queue_max_size=3)  # type: ignore[call-arg]
    collector = Collector.__new__(Collector)
    collector._settings = settings  # type: ignore[attr-defined]
    collector._stop = asyncio.Event()  # type: ignore[attr-defined]
    collector._queue = asyncio.Queue(maxsize=3)  # type: ignore[attr-defined]
    from market_data_collector.collector import _Stats

    collector._stats = _Stats()  # type: ignore[attr-defined]

    ticks = [_make_tick(i) for i in range(10)]
    adapter = _FixedStreamAdapter(ticks)

    task = asyncio.create_task(collector._ingest(adapter))  # type: ignore[arg-type]
    # Yield enough times for the adapter to push all 10 ticks through the queue.
    for _ in range(50):
        await asyncio.sleep(0)

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Queue holds the 3 most recent ticks; 7 oldest were dropped.
    assert collector._queue.qsize() == 3  # type: ignore[attr-defined]
    assert collector._stats.dropped == 7  # type: ignore[attr-defined]
    remaining = []
    while not collector._queue.empty():  # type: ignore[attr-defined]
        remaining.append(collector._queue.get_nowait().trade_id)  # type: ignore[attr-defined]
    assert remaining == ["7", "8", "9"]
