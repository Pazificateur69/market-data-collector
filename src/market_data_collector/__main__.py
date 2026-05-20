"""Entry point: configure logging, load settings, run the collector."""

from __future__ import annotations

import asyncio

import structlog

from .collector import Collector
from .config import Settings
from .logging_config import configure_logging


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    log = structlog.get_logger(__name__)
    log.info(
        "collector.starting",
        binance=settings.binance_symbols,
        coinbase=settings.coinbase_symbols,
        kafka_topic=settings.kafka_topic,
    )
    try:
        asyncio.run(Collector(settings).run())
    except KeyboardInterrupt:
        log.info("collector.interrupted")


if __name__ == "__main__":
    main()
