"""Unit tests for the malformed-record filter (valid_condition).

A row is valid only if every required field parsed to a non-null. This is the
filter that drops bad JSON / missing fields before aggregation.
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

from streaming_job import valid_condition

PARSED_SCHEMA = StructType(
    [
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("size", DoubleType()),
        StructField("side", StringType()),
        StructField("trade_id", LongType()),
        StructField("ts", TimestampType()),
    ]
)

TS = datetime(2026, 7, 1, 10, 12, 0)


def _rows(spark, rows):
    df = spark.createDataFrame(rows, schema=PARSED_SCHEMA)
    return df.filter(valid_condition())


def test_keeps_fully_populated_row(spark):
    rows = [("BTC-USD", 100.0, 1.0, "buy", 1, TS)]
    assert _rows(spark, rows).count() == 1


def test_drops_rows_with_any_null_required_field(spark):
    rows = [
        ("BTC-USD", 100.0, 1.0, "buy", 1, TS),      # valid
        (None, 100.0, 1.0, "buy", 2, TS),           # null symbol
        ("BTC-USD", None, 1.0, "buy", 3, TS),       # null price (bad JSON number)
        ("BTC-USD", 100.0, 1.0, "buy", 4, None),    # null ts (unparseable time)
    ]
    kept = _rows(spark, rows)
    assert kept.count() == 1
    assert kept.collect()[0]["trade_id"] == 1
