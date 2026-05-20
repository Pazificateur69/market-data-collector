"""Base class implementing the resilient WebSocket loop shared across exchanges."""

from __future__ import annotations

import asyncio
import random
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterable
from typing import Any

import orjson
import structlog
import websockets
from websockets.asyncio.client import ClientConnection, connect

from ..config import Settings
from ..models import Exchange, Tick

logger = structlog.get_logger(__name__)


class ExchangeAdapter(ABC):
    """Connects to an exchange WebSocket, yields normalized Ticks indefinitely.

    Subclasses implement three small hooks: URL building, subscription payload, and
    raw-message parsing. The reconnection / backoff loop lives here so it stays
    consistent across exchanges.
    """

    exchange: Exchange

    def __init__(self, symbols: Iterable[str], settings: Settings) -> None:
        self._symbols = list(symbols)
        self._settings = settings
        self._log = logger.bind(exchange=self.exchange.value)

    @abstractmethod
    def url(self) -> str: ...

    @abstractmethod
    def subscribe_payload(self) -> dict[str, Any] | None:
        """Optional JSON payload sent immediately after connecting."""

    @abstractmethod
    def parse(self, raw: dict[str, Any]) -> Tick | None:
        """Convert a raw WebSocket message into a Tick. Return None to skip."""

    async def stream(self) -> AsyncIterator[Tick]:
        """Yield ticks forever, reconnecting with exponential backoff + jitter."""
        delay = self._settings.backoff_initial_seconds
        while True:
            try:
                async for tick in self._one_connection():
                    delay = self._settings.backoff_initial_seconds  # reset on success
                    yield tick
            except asyncio.CancelledError:
                raise
            except (websockets.ConnectionClosed, OSError) as exc:
                self._log.warning("ws.disconnect", reason=str(exc), retry_in=round(delay, 2))
            except Exception as exc:
                self._log.exception("ws.unexpected_error", error=str(exc))

            jitter = random.uniform(0, delay * 0.25)
            await asyncio.sleep(delay + jitter)
            delay = min(
                delay * self._settings.backoff_multiplier,
                self._settings.backoff_max_seconds,
            )

    async def _one_connection(self) -> AsyncIterator[Tick]:
        async with connect(
            self.url(),
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
            max_size=2**20,
        ) as ws:
            self._log.info("ws.connected", url=self.url(), symbols=self._symbols)
            await self._send_subscription(ws)
            async for message in ws:
                tick = self._safe_parse(message)
                if tick is not None:
                    yield tick

    async def _send_subscription(self, ws: ClientConnection) -> None:
        payload = self.subscribe_payload()
        if payload is None:
            return
        await ws.send(orjson.dumps(payload).decode())
        self._log.info("ws.subscribed", channels=payload.get("params") or payload)

    def _safe_parse(self, message: str | bytes) -> Tick | None:
        try:
            raw = orjson.loads(message)
        except orjson.JSONDecodeError:
            self._log.warning("ws.invalid_json", preview=str(message)[:120])
            return None
        if not isinstance(raw, dict):
            return None
        try:
            return self.parse(raw)
        except Exception as exc:
            self._log.warning("parse.error", error=str(exc), payload=raw)
            return None
