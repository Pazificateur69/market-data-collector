"""Entry point: configure logging, start metrics server, run the collector."""

from __future__ import annotations

import asyncio

import structlog

from .collector import Collector
from .config import Settings
from .logging_config import configure_logging
from .metrics import start_metrics_server


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    log = structlog.get_logger(__name__)

    if settings.metrics_enabled:
        start_metrics_server(settings.metrics_port)
        log.info("metrics.started", port=settings.metrics_port)

    log.info(
        "collector.starting",
        binance=settings.binance_symbols,
        coinbase=settings.coinbase_symbols,
        kraken=settings.kraken_symbols,
        kafka_topic=settings.kafka_topic,
        queue_max_size=settings.queue_max_size,
    )
    try:
        asyncio.run(Collector(settings).run())
    except KeyboardInterrupt:
        log.info("collector.interrupted")


if __name__ == "__main__":
    main()
