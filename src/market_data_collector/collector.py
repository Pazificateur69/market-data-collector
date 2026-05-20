"""Top-level orchestrator.

Architecture:

    [BinanceAdapter]  ─┐
    [CoinbaseAdapter] ─┼─▶ asyncio.Queue (bounded) ─▶ [KafkaTickProducer]
    [KrakenAdapter]   ─┘

The bounded queue is the key resilience primitive: WebSocket consumers cannot stall
the event loop if Kafka is slow, and we never accumulate unbounded memory.
On overflow the OLDEST tick is dropped and `mdc_ticks_dropped_total` is incremented.
"""

from __future__ import annotations

import asyncio
import signal
import time
from dataclasses import dataclass, field

import structlog

from . import metrics as m
from .config import Settings
from .exchanges import (
    BinanceAdapter,
    CoinbaseAdapter,
    ExchangeAdapter,
    KrakenAdapter,
)
from .kafka_producer import KafkaTickProducer
from .models import Tick

logger = structlog.get_logger(__name__)


@dataclass
class _Stats:
    started_at: float = field(default_factory=time.monotonic)
    published: int = 0
    failed: int = 0
    dropped: int = 0

    def snapshot(self) -> dict[str, float | int]:
        elapsed = max(time.monotonic() - self.started_at, 1e-9)
        return {
            "published": self.published,
            "failed": self.failed,
            "dropped": self.dropped,
            "rate_per_sec": round(self.published / elapsed, 2),
            "elapsed_sec": round(elapsed, 1),
        }


class Collector:
    """Runs N exchange adapters and one Kafka producer until shutdown is signaled."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._stats = _Stats()
        self._stop = asyncio.Event()
        self._queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=settings.queue_max_size)
        self._adapters: list[ExchangeAdapter] = self._build_adapters()

    def _build_adapters(self) -> list[ExchangeAdapter]:
        adapters: list[ExchangeAdapter] = []
        if self._settings.binance_symbols:
            adapters.append(BinanceAdapter(self._settings.binance_symbols, self._settings))
        if self._settings.coinbase_symbols:
            adapters.append(CoinbaseAdapter(self._settings.coinbase_symbols, self._settings))
        if self._settings.kraken_symbols:
            adapters.append(KrakenAdapter(self._settings.kraken_symbols, self._settings))
        if not adapters:
            raise ValueError("No exchange symbols configured")
        return adapters

    async def run(self) -> None:
        self._install_signal_handlers()
        async with KafkaTickProducer(self._settings) as producer:
            tasks = [
                asyncio.create_task(self._ingest(adapter), name=f"ingest-{adapter.exchange}")
                for adapter in self._adapters
            ]
            tasks.append(asyncio.create_task(self._drain(producer), name="drain"))
            tasks.append(asyncio.create_task(self._report_loop(), name="metrics"))
            stopper = asyncio.create_task(self._stop.wait(), name="stopper")

            try:
                done, _ = await asyncio.wait([*tasks, stopper], return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if task is stopper:
                        continue
                    if (exc := task.exception()) is not None:
                        logger.error("task.crashed", task=task.get_name(), error=str(exc))
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                stopper.cancel()
                logger.info("collector.shutdown", **self._stats.snapshot())

    async def _ingest(self, adapter: ExchangeAdapter) -> None:
        """Pull ticks from one adapter and shove them into the shared queue.

        On overflow we drop the OLDEST tick (head) rather than the new one — newer
        ticks are more useful to downstream consumers than stale ones.
        """
        exchange_label = adapter.exchange.value
        async for tick in adapter.stream():
            if self._stop.is_set():
                return
            while True:
                try:
                    self._queue.put_nowait(tick)
                    m.QUEUE_DEPTH.set(self._queue.qsize())
                    break
                except asyncio.QueueFull:
                    try:
                        self._queue.get_nowait()
                        self._queue.task_done()
                        self._stats.dropped += 1
                        m.TICKS_DROPPED.labels(exchange_label).inc()
                    except asyncio.QueueEmpty:
                        # Race: someone else drained it. Loop and try put_nowait again.
                        continue

    async def _drain(self, producer: KafkaTickProducer) -> None:
        """Single consumer of the queue: serializes all writes to one producer."""
        while not self._stop.is_set():
            tick = await self._queue.get()
            try:
                await producer.publish(tick)
                self._stats.published += 1
                m.TICKS_PUBLISHED.labels(tick.exchange.value, tick.symbol).inc()
            except Exception as exc:
                self._stats.failed += 1
                m.TICKS_FAILED.labels(tick.exchange.value).inc()
                logger.warning(
                    "publish.failed",
                    exchange=tick.exchange.value,
                    symbol=tick.symbol,
                    error=str(exc),
                )
            finally:
                self._queue.task_done()
                m.QUEUE_DEPTH.set(self._queue.qsize())

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
