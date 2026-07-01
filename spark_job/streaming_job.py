"""TradePulse candle aggregation (Phase 3).

PySpark Structured Streaming job that reads trades from Kafka (`trades.raw`),
computes 1-minute OHLC candles per symbol, and writes finalized candles to
Postgres.

Design notes
------------
OHLC is computed as a windowed *aggregation* in the streaming query, not inside
foreachBatch. A 1-minute window spans many micro-batches, so aggregating raw
trades per micro-batch would fragment candles. Instead:

  open  = min_by(price, (ts, trade_id))   # price at the earliest trade
  close = max_by(price, (ts, trade_id))   # price at the latest trade
  high  = max(price)
  low   = min(price)
  volume= sum(size)

With withWatermark + append output mode, Spark emits each window's candle once,
after the watermark confirms it is final. foreachBatch is then used only as the
JDBC sink (Structured Streaming has no built-in JDBC sink), doing a plain
INSERT/append. A second lightweight query logs malformed-record counts per
micro-batch.

No alert derivation or dashboard here; this phase is candle aggregation only.
"""

from __future__ import annotations

import logging
import os
from datetime import timezone
from functools import reduce

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
)
log = logging.getLogger("tradepulse.spark")

# --- Configuration ---------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trades.raw")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "tradepulse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "tradepulse")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "change_me")

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/opt/spark_checkpoints")
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "1 minute")

SPARK_PACKAGES = os.getenv(
    "SPARK_PACKAGES",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2",
)
SPARK_IVY_DIR = os.getenv("SPARK_IVY_DIR", "/opt/.ivy2")

# A finalized candle whose |open->close| move exceeds this percent raises an alert.
ALERT_THRESHOLD_PCT = float(os.getenv("ALERT_THRESHOLD_PCT", "0.3"))

# Explicit schema for the trades.raw JSON value. Mirrors
# schemas/trades.raw.schema.json. No schema inference.
TRADE_SCHEMA = StructType(
    [
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("size", DoubleType()),
        StructField("side", StringType()),
        StructField("trade_id", LongType()),
        StructField("ts", StringType()),  # ISO 8601 string, parsed below
    ]
)

REQUIRED_FIELDS = ["symbol", "price", "size", "side", "trade_id", "ts"]


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("tradepulse-candles")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.jars.ivy", SPARK_IVY_DIR)
        # Candles/windows are computed in UTC to match the exchange timestamps.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def valid_condition():
    """A row is valid only if every required field parsed to a non-null."""
    return reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in REQUIRED_FIELDS])


def parse_trades(raw_df: DataFrame) -> DataFrame:
    """Kafka value -> typed trade columns (ts as timestamp). Bad JSON or bad
    fields surface as nulls, which the caller filters on."""
    return (
        raw_df.select(F.col("value").cast("string").alias("json"))
        .select(F.from_json("json", TRADE_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("ts", F.to_timestamp("ts"))
    )


def aggregate_candles(
    trades_df: DataFrame,
    window_duration: str = WINDOW_DURATION,
    watermark_delay: str = WATERMARK_DELAY,
    streaming: bool = True,
) -> DataFrame:
    """Compute 1-minute OHLC candles per symbol.

    open/close are the prices of the earliest/latest trade in the window,
    ordered by (ts, trade_id) so ties are broken deterministically. high/low/
    volume are order-independent aggregates. Pure and batch-testable when
    streaming=False.
    """
    df = trades_df
    if streaming:
        df = df.withWatermark("ts", watermark_delay)

    order = F.struct("ts", "trade_id")
    return (
        df.groupBy(F.window("ts", window_duration), "symbol")
        .agg(
            F.min_by("price", order).alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", order).alias("close"),
            F.sum("size").alias("volume"),
        )
        .select(
            F.col("symbol"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "open",
            "high",
            "low",
            "close",
            "volume",
        )
    )


def pct_change(open_price: float, close_price: float) -> float:
    """Percent change from open to close. 0.42 means +0.42%."""
    if not open_price:
        return 0.0
    return (close_price - open_price) / open_price * 100.0


def build_alert(
    symbol: str,
    window_start,
    window_end,
    open_price: float,
    close_price: float,
    threshold: float,
):
    """Return an alert dict if abs(pct_change) exceeds threshold, else None.

    Pure and batch-testable. ts is the window_end, price is the close.
    """
    pct = pct_change(open_price, close_price)
    if abs(pct) <= threshold:
        return None
    message = (
        f"{symbol} moved {pct:+.2f}% in the "
        f"{window_start:%H:%M}-{window_end:%H:%M} window"
    )
    return {
        "symbol": symbol,
        "ts": window_end,
        "price": close_price,
        "pct_change": pct,
        "message": message,
    }


def _pg_connect():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def _utc(dt):
    """Spark hands back naive UTC datetimes; make them tz-aware for Postgres."""
    if dt is not None and dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


CANDLE_INSERT = (
    "INSERT INTO candles "
    "(symbol, window_start, window_end, open, high, low, close, volume) "
    "VALUES %s ON CONFLICT (symbol, window_start) DO NOTHING"
)
# alerts.ts holds the window_end, so idempotency is keyed on (symbol, ts).
ALERT_INSERT = (
    "INSERT INTO alerts (symbol, ts, price, pct_change, message) "
    "VALUES %s ON CONFLICT (symbol, ts) DO NOTHING"
)


def make_sink(threshold: float):
    """foreachBatch sink: write finalized candles and derive volatility alerts.

    Both inserts use ON CONFLICT DO NOTHING, so a checkpoint-recovery replay of a
    batch is a safe no-op: no duplicate rows and no UPDATE.
    """

    def _write(batch_df: DataFrame, batch_id: int) -> None:
        rows = batch_df.collect()
        if not rows:
            return

        candles = [
            (
                r["symbol"],
                _utc(r["window_start"]),
                _utc(r["window_end"]),
                r["open"],
                r["high"],
                r["low"],
                r["close"],
                r["volume"],
            )
            for r in rows
        ]

        alerts = []
        for r in rows:
            alert = build_alert(
                r["symbol"],
                r["window_start"],
                r["window_end"],
                r["open"],
                r["close"],
                threshold,
            )
            if alert is not None:
                alerts.append(
                    (
                        alert["symbol"],
                        _utc(alert["ts"]),
                        alert["price"],
                        alert["pct_change"],
                        alert["message"],
                    )
                )

        conn = _pg_connect()
        try:
            with conn:
                with conn.cursor() as cur:
                    execute_values(cur, CANDLE_INSERT, candles)
                    if alerts:
                        execute_values(cur, ALERT_INSERT, alerts)
        finally:
            conn.close()

        log.info(
            "batch %s: %d candle(s), %d alert(s)",
            batch_id,
            len(candles),
            len(alerts),
        )

    return _write


def make_malformed_logger():
    def _log(batch_df: DataFrame, batch_id: int) -> None:
        dropped = batch_df.count()
        if dropped > 0:
            log.warning("batch %s: dropped %d malformed record(s)", batch_id, dropped)

    return _log


def main() -> None:
    spark = build_spark()
    log.info("reading kafka %s topic=%s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    trades = parse_trades(raw)
    valid = trades.filter(valid_condition())
    malformed = trades.filter(~valid_condition())

    candles = aggregate_candles(valid, streaming=True)

    candle_query = (
        candles.writeStream.outputMode("append")
        .foreachBatch(make_sink(ALERT_THRESHOLD_PCT))
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "candles"))
        .start()
    )

    malformed_query = (
        malformed.writeStream.outputMode("append")
        .foreachBatch(make_malformed_logger())
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "malformed"))
        .start()
    )

    log.info("streaming started; awaiting termination")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
