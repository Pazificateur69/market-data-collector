"""Async Kafka producer wrapper that serializes Tick models via orjson."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from types import TracebackType
from typing import Any, Self

import orjson
import structlog
from aiokafka import AIOKafkaProducer

from . import metrics as m
from .config import Settings
from .models import Tick

logger = structlog.get_logger(__name__)


def _default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"unsupported type: {type(obj).__name__}")


def _serialize(tick: Tick) -> bytes:
    return orjson.dumps(tick.model_dump(mode="json"), default=_default)


class KafkaTickProducer:
    """Thin async wrapper around aiokafka with explicit lifecycle management."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer: AIOKafkaProducer | None = None

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._producer is not None:
            return
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            client_id=self._settings.kafka_client_id,
            linger_ms=self._settings.kafka_linger_ms,
            compression_type=self._settings.kafka_compression,
            acks=self._settings.kafka_acks,
            enable_idempotence=False,
        )
        await self._producer.start()
        logger.info(
            "kafka.producer.started",
            bootstrap=self._settings.kafka_bootstrap_servers,
            topic=self._settings.kafka_topic,
        )

    async def stop(self) -> None:
        if self._producer is None:
            return
        try:
            await self._producer.flush()
        finally:
            await self._producer.stop()
            self._producer = None
            logger.info("kafka.producer.stopped")

    async def publish(self, tick: Tick) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaTickProducer not started")
        start = time.perf_counter()
        try:
            await self._producer.send_and_wait(
                self._settings.kafka_topic,
                value=_serialize(tick),
                key=tick.kafka_key(),
            )
        finally:
            elapsed = time.perf_counter() - start
            m.PUBLISH_LATENCY.observe(elapsed)
        # End-to-end latency: exchange event_time -> publish ack on our side.
        e2e = (datetime.now(UTC) - tick.event_time).total_seconds()
        if e2e >= 0:  # guard against clock skew producing negative values
            m.E2E_LATENCY.labels(tick.exchange.value).observe(e2e)
