"""Unit tests for the OHLC-picking logic (aggregate_candles).

Runs against a small static DataFrame in batch mode (streaming=False), no live
Kafka/stream needed.

    python -m pytest -q            # from the /spark_job directory
"""

from datetime import datetime

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming_job import aggregate_candles

TRADES_SCHEMA = StructType(
    [
        StructField("symbol", StringType()),
        StructField("ts", TimestampType()),
        StructField("price", DoubleType()),
        StructField("size", DoubleType()),
        StructField("trade_id", LongType()),
    ]
)


def _candles(spark, rows):
    df = spark.createDataFrame(rows, schema=TRADES_SCHEMA)
    out = aggregate_candles(df, streaming=False)
    # key candles by (symbol, window_start) for easy assertions
    return {
        (r["symbol"], r["window_start"]): r
        for r in out.collect()
    }


def test_ohlc_within_window(spark):
    rows = [
        # BTC-USD, window 10:12:00 - 10:13:00
        ("BTC-USD", datetime(2026, 7, 1, 10, 12, 5), 100.0, 1.0, 1),
        ("BTC-USD", datetime(2026, 7, 1, 10, 12, 30), 110.0, 2.0, 2),  # high
        ("BTC-USD", datetime(2026, 7, 1, 10, 12, 45), 95.0, 1.0, 3),  # low
        ("BTC-USD", datetime(2026, 7, 1, 10, 12, 59), 105.0, 1.0, 4),  # last
    ]
    c = _candles(spark, rows)
    row = c[("BTC-USD", datetime(2026, 7, 1, 10, 12, 0))]
    assert row["open"] == 100.0
    assert row["high"] == 110.0
    assert row["low"] == 95.0
    assert row["close"] == 105.0
    assert row["volume"] == 5.0
    assert row["window_end"] == datetime(2026, 7, 1, 10, 13, 0)


def test_windows_and_symbols_are_separated(spark):
    rows = [
        ("BTC-USD", datetime(2026, 7, 1, 10, 12, 5), 100.0, 1.0, 1),
        ("BTC-USD", datetime(2026, 7, 1, 10, 12, 59), 105.0, 1.0, 2),
        # next window for BTC
        ("BTC-USD", datetime(2026, 7, 1, 10, 13, 10), 106.0, 3.0, 3),
        # different symbol, same first window
        ("ETH-USD", datetime(2026, 7, 1, 10, 12, 20), 50.0, 3.0, 4),
        ("ETH-USD", datetime(2026, 7, 1, 10, 12, 40), 52.0, 1.0, 5),
    ]
    c = _candles(spark, rows)
    assert len(c) == 3

    btc1 = c[("BTC-USD", datetime(2026, 7, 1, 10, 12, 0))]
    assert (btc1["open"], btc1["close"], btc1["volume"]) == (100.0, 105.0, 2.0)

    btc2 = c[("BTC-USD", datetime(2026, 7, 1, 10, 13, 0))]
    assert (btc2["open"], btc2["close"], btc2["high"], btc2["low"]) == (
        106.0,
        106.0,
        106.0,
        106.0,
    )

    eth1 = c[("ETH-USD", datetime(2026, 7, 1, 10, 12, 0))]
    assert (eth1["open"], eth1["close"], eth1["volume"]) == (50.0, 52.0, 4.0)


def test_same_timestamp_tiebreak_by_trade_id(spark):
    # Two trades share the earliest ts; open must come from the lower trade_id,
    # close from the higher trade_id at the latest ts.
    rows = [
        ("BTC-USD", datetime(2026, 7, 1, 10, 12, 0), 100.0, 1.0, 11),
        ("BTC-USD", datetime(2026, 7, 1, 10, 12, 0), 101.0, 1.0, 10),  # earlier id
        ("BTC-USD", datetime(2026, 7, 1, 10, 12, 30), 200.0, 1.0, 20),
        ("BTC-USD", datetime(2026, 7, 1, 10, 12, 30), 199.0, 1.0, 21),  # later id
    ]
    c = _candles(spark, rows)
    row = c[("BTC-USD", datetime(2026, 7, 1, 10, 12, 0))]
    assert row["open"] == 101.0  # trade_id 10
    assert row["close"] == 199.0  # trade_id 21
    assert row["high"] == 200.0
    assert row["low"] == 100.0
