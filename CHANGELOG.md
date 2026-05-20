# Changelog

All notable changes to this project will be documented in this file.
Format loosely follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.2.1] — 2026-05-20

### Fixed
- **Dockerfile**: switched the build stage to an isolated venv so transitive
  dependencies (e.g. `packaging`) are reliably copied into the runtime stage.
- **Dockerfile**: `.dockerignore` no longer excludes `README.md` (required by
  `pyproject.toml`'s `readme` field at install time) — unblocks the CI docker job.
- **Config**: `MDC_*_SYMBOLS` env vars are parsed as comma-separated strings instead
  of pydantic-settings' default JSON list parsing (via `NoDecode`).
- **Config**: `MDC_KAFKA_ACKS` accepts `"0"`, `"1"`, `"-1"`, or `"all"` from env;
  invalid string was crashing aiokafka at boot.

### Added
- **Kafka producer**: retries broker bootstrap with backoff so a slow-starting
  broker no longer crashes the whole service at boot.
- `cramjam` declared as an explicit dependency — required for `lz4` compression
  in aiokafka ≥ 0.14.

### Changed
- Bumped Kafka in `docker-compose.yml` to `apache/kafka:4.0.2` (replaces the
  deprecated `bitnami/kafka` image).
- Prometheus is now opt-in via the `observability` Compose profile:
  `docker compose --profile observability up`.

### Verified
- End-to-end smoke test against live Binance + Coinbase + Kraken WebSockets
  with a real Kafka broker: 2,242 ticks published in 30s, queue depth = 0,
  publish-latency mean ≈ 8.8 ms.

## [0.2.0] — 2026-05-20

### Added
- **Kraken** WebSocket v2 adapter (trade channel) — third exchange supported.
- **Prometheus metrics** exposed on `/metrics`:
  counters (`mdc_ticks_published_total`, `mdc_ticks_failed_total`,
  `mdc_ticks_dropped_total`, `mdc_ws_reconnects_total`),
  gauges (`mdc_ws_connected`, `mdc_queue_depth`),
  histograms (`mdc_kafka_publish_latency_seconds`, `mdc_e2e_latency_seconds`).
- **Bounded `asyncio.Queue`** between exchange adapters and the Kafka producer.
  When the queue fills, the oldest tick is dropped (FIFO eviction) and a counter
  is incremented — the WebSocket read loop is never blocked by a slow broker.
- **Prometheus** service pre-wired in `docker-compose.yml` (port 9090) with a
  ready-to-use scrape config under `deploy/prometheus.yml`.
- HTTP-based Docker healthcheck against `/metrics` (replaces the prior TCP probe).
- Tests for the Kraken parser and the queue-overflow eviction path.

### Changed
- `Collector` is now a producer/consumer pipeline: one `_ingest` task per
  exchange feeds the shared queue, one `_drain` task publishes to Kafka.
- Kafka producer records publish latency and `event_time → ack` end-to-end
  latency on every send.

## [0.1.0] — 2026-05-20

### Added
- Initial release: Binance + Coinbase WebSocket adapters, normalized `Tick`
  model, aiokafka producer, exponential-backoff reconnection, structlog JSON
  output, multi-stage Dockerfile, docker-compose dev stack, GitHub Actions CI.
