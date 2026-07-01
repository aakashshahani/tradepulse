<p align="center">
  <img src="assets/banner.svg" alt="TradePulse" width="100%">
</p>

# TradePulse

A real-time streaming data pipeline for crypto market data. The full flow is
Coinbase WebSocket to Kafka to Spark structured streaming to Postgres to a
Streamlit dashboard, built in phases.

## Status

- **Phase 1: infrastructure and schema.** Kafka (KRaft) plus Postgres 18 via
  Docker Compose, with the pipeline schema created on first Postgres startup.
- **Phase 2: Coinbase bridge producer.** A Python service that republishes live
  Coinbase trades onto Kafka.
- **Phase 3: candle aggregation.** A PySpark Structured Streaming job that turns
  `trades.raw` into 1-minute OHLC candles and writes them to Postgres.
- **Phase 4: volatility alerts.** The same job flags candles whose open-to-close
  move exceeds a threshold and writes them to the `alerts` table. (current)

The Streamlit dashboard is a later phase, not built yet.

## Project layout

```
tradepulse/
├── docker-compose.yml   # kafka, kafka-init, postgres, producer
├── .env.example         # copy to .env
├── assets/
│   └── banner.svg
├── sql/
│   └── init.sql         # schema loaded on first Postgres startup
├── schemas/
│   └── trades.raw.schema.json   # JSON Schema contract for the trades.raw topic
├── producer/            # Phase 2: Coinbase to Kafka producer
│   ├── producer.py
│   ├── requirements.txt
│   ├── Dockerfile
│   └── tests/           # unit tests (transform_match)
├── spark_job/           # Phase 3: Spark structured streaming (candles)
│   ├── streaming_job.py
│   ├── requirements.txt
│   ├── Dockerfile
│   └── tests/           # unit tests (aggregate_candles OHLC logic)
└── dashboard/           # Phase 5+: Streamlit dashboard (placeholder)
```

## Services

| Service      | Image                | Purpose                                                        |
|--------------|----------------------|----------------------------------------------------------------|
| `kafka`      | `apache/kafka:4.3.1` | Single-node KRaft broker (broker and controller, no ZooKeeper).|
| `kafka-init` | `apache/kafka:4.3.1` | One-shot: creates `trades.raw` with 6 partitions, then exits.  |
| `postgres`   | `postgres:18`        | Pipeline schema, persistent named volume.                      |
| `producer`   | built locally        | Coinbase Exchange WS to `trades.raw`.                          |
| `spark_job`  | built locally        | Structured Streaming: `trades.raw` to 1-minute candles in Postgres. |

### Kafka listeners (host vs. in-network)

The broker advertises two addresses because a single one cannot serve both
paths. The address a client is told to reconnect on must be resolvable from
that client.

- **In-network containers** use `kafka:9092` (the `INTERNAL` listener). This is
  what the producer connects to.
- **The host machine** uses `localhost:29092` (the `HOST` listener). Note the
  port is 29092, not 9092.

`docker compose exec kafka ...` runs inside the broker container, so it uses
`localhost:9092`. That is fine and unaffected by the above.

## Getting started

```bash
cp .env.example .env     # adjust POSTGRES_PASSWORD etc. if you like
docker compose up -d --build
docker compose ps        # kafka/postgres healthy, kafka-init exited 0, producer up
```

## Verifying Phase 3 (candles)

The Spark job reads `trades.raw` from the latest offset, computes 1-minute OHLC
candles per symbol, and writes each finalized window to the `candles` table. It
uses a 1-minute watermark, so the first candle for a window lands roughly two to
three minutes after startup (window length plus watermark).

Watch it write candles:

```bash
docker compose logs -f spark_job     # look for "wrote N candle(s) to postgres"
```

Query the candles once a couple of minutes have passed:

```bash
docker compose exec postgres psql -U tradepulse -d tradepulse -c \
  "SELECT symbol, window_start, window_end, open, high, low, close, round(volume,4) AS volume
     FROM candles ORDER BY window_start DESC, symbol LIMIT 12;"
```

How OHLC is computed: `open` and `close` are the prices of the earliest and
latest trade in the window, ordered by `(ts, trade_id)`; `high`/`low`/`volume`
are `max`/`min`/`sum`. This runs as a windowed aggregation in the streaming
query so each candle is complete before it is written, and `foreachBatch` is the
JDBC sink (append/INSERT). Malformed records are dropped and counted per batch
(grep the logs for `dropped`).

Run the OHLC unit tests:

```bash
docker compose run --rm --no-deps -v "$PWD/spark_job:/app" -w /app \
  --entrypoint python spark_job -m pytest -q
```

## Verifying Phase 4 (alerts)

In the same `foreachBatch` sink, each finalized candle's move is computed as
`pct_change = (close - open) / open * 100`. If `abs(pct_change)` exceeds
`ALERT_THRESHOLD_PCT` (default `0.3`), an `alerts` row is written with the close
price and a message like `BTC-USD moved +0.42% in the 14:32-14:33 window`.

Both the candle and alert inserts use `ON CONFLICT DO NOTHING` (keyed on
`(symbol, window_start)` and `(symbol, ts)`), so a checkpoint-recovery replay is
a safe no-op rather than a duplicate-key error.

Lower the threshold to see alerts quickly (crypto rarely moves 0.3% in a single
minute):

```bash
echo "ALERT_THRESHOLD_PCT=0.02" >> .env
docker compose up -d spark_job

docker compose exec postgres psql -U tradepulse -d tradepulse -c \
  "SELECT symbol, ts, round(pct_change,3) AS pct, message FROM alerts ORDER BY ts DESC LIMIT 10;"
```

## Verifying Phase 2 (producer)

Watch the producer bridge live trades onto Kafka:

```bash
docker compose logs -f producer
```

You should see `connected`, `subscription confirmed`, and a throughput line
roughly every 10s, for example:

```
throughput (10s window): BTC-USD=5.49/s ETH-USD=9.88/s SOL-USD=1.50/s | total=16.87/s
```

Confirm `trades.raw` has 6 partitions:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic trades.raw
```

Consume a few real messages (keyed by symbol):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic trades.raw \
  --property print.key=true --property key.separator=' | ' \
  --from-beginning --max-messages 5 --timeout-ms 15000
```

Each value matches `schemas/trades.raw.schema.json`, for example:

```json
{"symbol":"BTC-USD","price":58699.73,"size":8e-08,"side":"buy","trade_id":1047769595,"ts":"2026-07-01T10:12:39.996920Z"}
```

Check the producer's health. It refreshes a heartbeat file on every message from
the feed and goes `unhealthy` if the feed stalls:

```bash
docker compose ps producer      # STATUS shows (healthy)
```

### Running the producer unit tests

Tests are not shipped in the runtime image. Run them in the producer image with
the source mounted:

```bash
docker compose run --rm --no-deps -v "$PWD/producer:/app" -w /app \
  --entrypoint python producer -m unittest discover -t . -s tests -p 'test_*.py'
```

## Verifying Phase 1 (infra and schema)

### Kafka: produce and consume a test topic

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic test --partitions 1 --replication-factor 1

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

docker compose exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic test        # type lines, Ctrl-C

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic test --from-beginning --timeout-ms 5000

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic test
```

### Postgres: confirm the three tables exist

```bash
docker compose exec postgres psql -U tradepulse -d tradepulse -c "\dt"
docker compose exec postgres psql -U tradepulse -d tradepulse -c "\d candles"
```

You should see `candles`, `alerts`, and `raw_trades`.

> If you changed `POSTGRES_USER` or `POSTGRES_DB` in `.env`, substitute them above.

## Tearing down

```bash
docker compose down          # stop containers, keep data volume
docker compose down -v       # also remove the Postgres volume (wipes data and
                             # re-runs init.sql on next startup)
```

> `sql/init.sql` runs only when the Postgres data volume is empty. If you change
> the schema, run `docker compose down -v` to re-initialize.
>
> Kafka has no persistent volume, so `trades.raw` is recreated by `kafka-init`
> on each `up`.
