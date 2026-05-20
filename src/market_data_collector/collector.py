"""Top-level orchestrator: fans exchange streams into a shared Kafka producer."""

from __future__ import annotations

import asyncio
import signal
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import structlog

from .config import Settings
from .exchanges import BinanceAdapter, CoinbaseAdapter, ExchangeAdapter
from .kafka_producer import KafkaTickProducer
from .models import Tick

logger = structlog.get_logger(__name__)


@dataclass
class _Stats:
    started_at: float = field(default_factory=time.monotonic)
    published: int = 0
    failed: int = 0

    def snapshot(self) -> dict[str, float | int]:
        elapsed = max(time.monotonic() - self.started_at, 1e-9)
        return {
            "published": self.published,
            "failed": self.failed,
            "rate_per_sec": round(self.published / elapsed, 2),
            "elapsed_sec": round(elapsed, 1),
        }


class Collector:
    """Runs N exchange adapters and one Kafka producer until shutdown is signaled."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._stats = _Stats()
        self._stop = asyncio.Event()
        self._adapters: list[ExchangeAdapter] = self._build_adapters()

    def _build_adapters(self) -> list[ExchangeAdapter]:
        adapters: list[ExchangeAdapter] = []
        if self._settings.binance_symbols:
            adapters.append(BinanceAdapter(self._settings.binance_symbols, self._settings))
        if self._settings.coinbase_symbols:
            adapters.append(CoinbaseAdapter(self._settings.coinbase_symbols, self._settings))
        if not adapters:
            raise ValueError("No exchange symbols configured")
        return adapters

    async def run(self) -> None:
        self._install_signal_handlers()
        async with KafkaTickProducer(self._settings) as producer:
            tasks = [
                asyncio.create_task(self._pump(adapter, producer), name=f"pump-{adapter.exchange}")
                for adapter in self._adapters
            ]
            tasks.append(asyncio.create_task(self._report_loop(), name="metrics"))
            stopper = asyncio.create_task(self._stop.wait(), name="stopper")
            try:
                done, _ = await asyncio.wait([*tasks, stopper], return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if task is stopper:
                        continue
                    if (exc := task.exception()) is not None:
                        logger.error("pump.crashed", task=task.get_name(), error=str(exc))
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                stopper.cancel()
                logger.info("collector.shutdown", **self._stats.snapshot())

    async def _pump(self, adapter: ExchangeAdapter, producer: KafkaTickProducer) -> None:
        stream: AsyncIterator[Tick] = adapter.stream()
        async for tick in stream:
            if self._stop.is_set():
                return
            try:
                await producer.publish(tick)
                self._stats.published += 1
            except Exception as exc:
                self._stats.failed += 1
                logger.warning(
                    "publish.failed",
                    exchange=tick.exchange.value,
                    symbol=tick.symbol,
                    error=str(exc),
                )

    async def _report_loop(self) -> None:
        interval = self._settings.metrics_log_interval_seconds
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except TimeoutError:
                logger.info("collector.stats", **self._stats.snapshot())

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except NotImplementedError:
                # Windows / restricted env: rely on KeyboardInterrupt propagation.
                pass
