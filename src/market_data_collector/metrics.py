"""Prometheus metrics + minimal HTTP server.

We use prometheus_client's official asyncio-compatible exposition: a tiny aiohttp-free
HTTP handler running in a background thread (prometheus_client.start_http_server).
That keeps the dependency surface small and the event loop unblocked.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

# A dedicated registry keeps test isolation easy and avoids polluting the default one.
REGISTRY = CollectorRegistry(auto_describe=True)

TICKS_PUBLISHED = Counter(
    "mdc_ticks_published_total",
    "Total ticks successfully published to Kafka.",
    labelnames=("exchange", "symbol"),
    registry=REGISTRY,
)

TICKS_FAILED = Counter(
    "mdc_ticks_failed_total",
    "Total ticks that failed to publish to Kafka.",
    labelnames=("exchange",),
    registry=REGISTRY,
)

TICKS_DROPPED = Counter(
    "mdc_ticks_dropped_total",
    "Total ticks dropped due to bounded queue overflow (backpressure).",
    labelnames=("exchange",),
    registry=REGISTRY,
)

WS_CONNECTED = Gauge(
    "mdc_ws_connected",
    "1 if the exchange WebSocket is currently connected, else 0.",
    labelnames=("exchange",),
    registry=REGISTRY,
)

WS_RECONNECTS = Counter(
    "mdc_ws_reconnects_total",
    "Total WebSocket reconnection attempts.",
    labelnames=("exchange",),
    registry=REGISTRY,
)

QUEUE_DEPTH = Gauge(
    "mdc_queue_depth",
    "Current size of the in-memory tick queue.",
    registry=REGISTRY,
)

# Latency buckets in seconds — tuned for crypto WS feeds (typical 10ms-1s).
_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

PUBLISH_LATENCY = Histogram(
    "mdc_kafka_publish_latency_seconds",
    "Time spent publishing a tick to Kafka (send_and_wait).",
    buckets=_LATENCY_BUCKETS,
    registry=REGISTRY,
)

E2E_LATENCY = Histogram(
    "mdc_e2e_latency_seconds",
    "End-to-end latency: exchange event_time -> Kafka publish ack.",
    labelnames=("exchange",),
    buckets=_LATENCY_BUCKETS,
    registry=REGISTRY,
)


def start_metrics_server(port: int) -> None:
    """Start a background HTTP server exposing /metrics on the given port."""
    start_http_server(port, registry=REGISTRY)
