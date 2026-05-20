# market-data-collector

Async Python service that streams crypto trades from **Binance** and **Coinbase Advanced Trade** WebSocket feeds, normalizes them into a uniform `Tick` model, and publishes them to **Kafka**.

Designed as the ingestion layer of a small market-data microservice mesh:

```
                 ┌────────────────────┐
  Binance WS ───▶│                    │
                 │ market-data-       │──▶ Kafka topic: market.ticks ──▶ downstream:
  Coinbase WS ──▶│ collector          │                                  - feature builder
                 │ (this repo)        │                                  - storage / OLAP
                 └────────────────────┘                                  - alpha strategies
```

## What it shows
- `asyncio` orchestration of multiple long-lived WebSocket streams
- The `websockets` library with ping/pong, timeouts, and clean reconnects
- An `aiokafka` producer with batching (`linger_ms`) and `lz4` compression
- **Strict typing** (`mypy --strict`, Pydantic v2 models, `StrEnum`)
- Exponential backoff + jitter for resilient reconnection
- A small, well-isolated `ExchangeAdapter` interface — adding a third exchange is ~40 lines
- Multi-stage **Dockerfile**, non-root user, healthcheck, `docker compose` dev stack
- CI: ruff, mypy, pytest, docker build (GitHub Actions)

## Quick start (Docker Compose)

```bash
docker compose up --build
```

Open another terminal and tail the topic:

```bash
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic market.ticks --from-beginning
```

Expected payload (one line per trade):

```json
{
  "exchange": "binance",
  "symbol": "BTC-USDT",
  "price": "42000.50",
  "quantity": "0.0123",
  "side": "sell",
  "trade_id": "12345",
  "event_time": "2024-01-01T00:00:00+00:00",
  "received_at": "2024-01-01T00:00:00.123456+00:00"
}
```

## Local development

```bash
make install         # pip install -e ".[dev]"
make check           # ruff + mypy + pytest
make run             # requires a Kafka broker reachable at MDC_KAFKA_BOOTSTRAP_SERVERS
```

## Configuration

Every setting is an environment variable prefixed with `MDC_`. See [`.env.example`](.env.example) for the full list. Highlights:

| Variable | Default | Purpose |
|---|---|---|
| `MDC_BINANCE_SYMBOLS` | `btcusdt,ethusdt` | Comma-separated Binance symbols (lowercase) |
| `MDC_COINBASE_SYMBOLS` | `BTC-USD,ETH-USD` | Comma-separated Coinbase product IDs |
| `MDC_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `MDC_KAFKA_TOPIC` | `market.ticks` | Destination topic |
| `MDC_BACKOFF_INITIAL_SECONDS` | `1.0` | Initial reconnect delay |
| `MDC_BACKOFF_MAX_SECONDS` | `60.0` | Cap on reconnect delay |

Disable an exchange by passing an empty list (`MDC_BINANCE_SYMBOLS=`).

## Project layout

```
src/market_data_collector/
├── __main__.py          # CLI entry point
├── collector.py         # orchestrator: fans adapters into the producer
├── config.py            # pydantic-settings
├── kafka_producer.py    # aiokafka wrapper + orjson serialization
├── logging_config.py    # structlog JSON output
├── models.py            # Tick, Exchange, Side
└── exchanges/
    ├── base.py          # reconnection loop + parse hooks
    ├── binance.py       # combined-stream trade adapter
    └── coinbase.py      # market_trades channel adapter
```

## Adding a new exchange

Subclass `ExchangeAdapter` with three methods — `url()`, `subscribe_payload()`, `parse()` — register it in `Collector._build_adapters`, done. The reconnection, backoff, JSON parsing, and Kafka publishing are inherited.

## Operational notes

- **Partition key** = `"{exchange}:{symbol}"`. Trades for a given symbol on a given exchange land on the same partition → preserved ordering downstream.
- **At-least-once** delivery (`acks=1`, `enable_idempotence=False`). For a trading system you'd flip this to `acks=all` + idempotence; the default favors throughput for analytics.
- The Docker healthcheck pings the configured Kafka broker — the producer is the bottleneck, not the WebSocket.
- Graceful shutdown on SIGINT/SIGTERM: in-flight messages are flushed before exit.

## License

MIT — see [LICENSE](LICENSE).
