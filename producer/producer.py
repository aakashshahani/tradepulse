"""TradePulse Coinbase bridge producer (Phase 2).

Connects to Coinbase's public Exchange WebSocket feed, transforms each
`match` (trade) event into our own JSON schema, and republishes it to the
Kafka topic `trades.raw`. Survives disconnects via exponential backoff with
jitter, and shuts down cleanly on SIGINT/SIGTERM.

No producer/dashboard/Spark logic here; this phase is the bridge only.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import signal
import time
from collections import defaultdict

import websockets
from confluent_kafka import Producer
from prometheus_client import Counter, start_http_server
from websockets.asyncio.client import connect

# --- Configuration (env-driven, with sensible local defaults) --------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trades.raw")
COINBASE_WS_URL = os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
PRODUCT_IDS = [
    p.strip()
    for p in os.getenv("PRODUCT_IDS", "BTC-USD,ETH-USD,SOL-USD").split(",")
    if p.strip()
]
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Liveness file: touched on connect and on every message received from the
# feed. The container HEALTHCHECK marks the producer unhealthy if this file
# goes stale (feed silent / disconnected). Coinbase's heartbeat channel emits
# ~1 msg/s per product, so a healthy connection refreshes this continuously
# even when a symbol isn't trading.
HEARTBEAT_FILE = os.getenv("HEARTBEAT_FILE", "/tmp/tradepulse_heartbeat")

# Reconnect backoff parameters.
BACKOFF_BASE_S = 1.0
BACKOFF_CAP_S = 30.0

# How often to emit a throughput line.
REPORT_INTERVAL_S = 10.0

# Prometheus metrics (scraped by Prometheus at producer:METRICS_PORT/metrics).
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
TRADES_PUBLISHED = Counter(
    "tradepulse_trades_total", "Trades published to Kafka", ["symbol"]
)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-7s %(message)s",
)
log = logging.getLogger("tradepulse.producer")


class ThroughputMeter:
    """Counts published trades per symbol and logs a rate line periodically."""

    def __init__(self) -> None:
        self._counts: dict[str, int] = defaultdict(int)
        self._window_start = time.monotonic()

    def record(self, symbol: str) -> None:
        self._counts[symbol] += 1

    def report(self) -> None:
        now = time.monotonic()
        elapsed = max(now - self._window_start, 1e-9)
        # Always show every configured symbol, even at 0/s, for liveness.
        parts = []
        total = 0
        for symbol in PRODUCT_IDS:
            count = self._counts.get(symbol, 0)
            total += count
            parts.append(f"{symbol}={count / elapsed:.2f}/s")
        log.info(
            "throughput (%.0fs window): %s | total=%.2f/s",
            elapsed,
            " ".join(parts),
            total / elapsed,
        )
        self._counts.clear()
        self._window_start = now


def touch_heartbeat() -> None:
    """Refresh the liveness file's mtime for the container HEALTHCHECK."""
    try:
        with open(HEARTBEAT_FILE, "w") as f:
            f.write(str(time.time()))
    except OSError as exc:
        log.warning("could not update heartbeat file %s: %s", HEARTBEAT_FILE, exc)


def build_producer() -> Producer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        # Idempotence guarantees no duplicate/reordered writes per partition;
        # librdkafka forces acks=all and bounded in-flight to make this safe.
        "enable.idempotence": True,
        "client.id": "tradepulse-producer",
        "linger.ms": 50,
    }
    log.info("creating Kafka producer -> %s (topic=%s)", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    return Producer(conf)


def _on_delivery(err, msg) -> None:
    if err is not None:
        log.error("delivery failed key=%s: %s", msg.key(), err)


def publish_trade(producer: Producer, record: dict) -> None:
    """Publish one trade, keyed by symbol so per-symbol order is preserved."""
    payload = json.dumps(record).encode("utf-8")
    key = record["symbol"].encode("utf-8")
    while True:
        try:
            producer.produce(
                KAFKA_TOPIC, key=key, value=payload, on_delivery=_on_delivery
            )
            return
        except BufferError:
            # Local queue is full; serve callbacks to drain, then retry.
            producer.poll(0.5)


def transform_match(msg: dict) -> dict:
    """Map a Coinbase `match` message to our trades.raw schema."""
    return {
        "symbol": msg["product_id"],
        "price": float(msg["price"]),
        "size": float(msg["size"]),
        "side": msg["side"],
        "trade_id": msg["trade_id"],
        "ts": msg["time"],  # already ISO 8601 from Coinbase
    }


def subscribe_message() -> str:
    return json.dumps(
        {
            "type": "subscribe",
            "product_ids": PRODUCT_IDS,
            "channels": ["matches", "heartbeat"],
        }
    )


async def sleep_or_stop(seconds: float, stop: asyncio.Event) -> None:
    """Sleep for `seconds`, but wake immediately if shutdown is requested."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


def handle_message(raw: str, producer: Producer, meter: ThroughputMeter) -> None:
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("could not parse message: %.200s", raw)
        return

    msg_type = msg.get("type")

    if msg_type == "match":
        try:
            record = transform_match(msg)
        except (KeyError, ValueError) as exc:
            log.warning("malformed match message (%s): %.200s", exc, raw)
            return
        publish_trade(producer, record)
        meter.record(record["symbol"])
        TRADES_PUBLISHED.labels(record["symbol"]).inc()
    elif msg_type == "heartbeat":
        log.debug("heartbeat %s seq=%s", msg.get("product_id"), msg.get("sequence"))
    elif msg_type == "subscriptions":
        log.info("subscription confirmed: %s", msg.get("channels"))
    elif msg_type == "error":
        # Log, do not crash. E.g. bad product_id or rate limiting.
        log.error("coinbase error: %s", msg.get("message"))
    else:
        # last_match, ticker, and anything else we didn't ask to act on.
        log.debug("ignoring message type=%s", msg_type)


async def consume(ws, producer: Producer, meter: ThroughputMeter, stop: asyncio.Event) -> None:
    """Read messages until shutdown is requested or the connection drops."""
    while not stop.is_set():
        try:
            # Bounded wait so we can re-check the stop flag even on a quiet feed.
            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        touch_heartbeat()  # any message = the connection is alive
        handle_message(raw, producer, meter)


async def stream_forever(producer: Producer, meter: ThroughputMeter, stop: asyncio.Event) -> None:
    attempt = 0
    while not stop.is_set():
        try:
            log.info("connecting to %s (attempt %d)", COINBASE_WS_URL, attempt + 1)
            async with connect(
                COINBASE_WS_URL,
                ping_interval=20,
                ping_timeout=20,
                max_queue=1024,
            ) as ws:
                log.info("connected; subscribing to %s", ", ".join(PRODUCT_IDS))
                attempt = 0  # reset backoff on a successful connection
                touch_heartbeat()
                await ws.send(subscribe_message())
                await consume(ws, producer, meter, stop)
        except asyncio.CancelledError:
            raise
        except (websockets.exceptions.WebSocketException, OSError) as exc:
            log.warning("disconnected: %s", exc)
        except Exception:  # noqa: BLE001 - never let the stream loop die
            log.exception("unexpected error in stream loop")

        if stop.is_set():
            break

        attempt += 1
        backoff = min(BACKOFF_CAP_S, BACKOFF_BASE_S * 2 ** (attempt - 1))
        delay = random.uniform(0, backoff)  # full jitter
        log.info("reconnecting in %.2fs (attempt %d)", delay, attempt + 1)
        await sleep_or_stop(delay, stop)


async def poll_loop(producer: Producer, stop: asyncio.Event) -> None:
    """Service librdkafka delivery callbacks off the event loop."""
    while not stop.is_set():
        producer.poll(0)
        await asyncio.sleep(0.1)


async def report_loop(meter: ThroughputMeter, stop: asyncio.Event) -> None:
    while not stop.is_set():
        await sleep_or_stop(REPORT_INTERVAL_S, stop)
        if not stop.is_set():
            meter.report()


def install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            # Windows event loops don't support add_signal_handler.
            signal.signal(sig, lambda *_: stop.set())


async def main() -> None:
    log.info(
        "starting tradepulse producer | products=%s topic=%s",
        ",".join(PRODUCT_IDS),
        KAFKA_TOPIC,
    )
    stop = asyncio.Event()
    install_signal_handlers(stop)
    touch_heartbeat()  # seed the file so the HEALTHCHECK has something to read
    start_http_server(METRICS_PORT)  # expose /metrics for Prometheus

    producer = build_producer()
    meter = ThroughputMeter()

    poll_task = asyncio.create_task(poll_loop(producer, stop))
    report_task = asyncio.create_task(report_loop(meter, stop))
    try:
        await stream_forever(producer, meter, stop)
    finally:
        log.info("shutting down; flushing Kafka producer...")
        for task in (poll_task, report_task):
            task.cancel()
        await asyncio.gather(poll_task, report_task, return_exceptions=True)
        remaining = producer.flush(10.0)
        if remaining > 0:
            log.warning("%d message(s) still undelivered after flush", remaining)
        log.info("shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
