"""Integration test: the real psycopg2 sink against a real Postgres.

Spins up Postgres with testcontainers, applies the actual sql/init.sql schema,
and runs make_sink over a Spark micro-batch, asserting candles + alerts land and
that a replayed batch is a no-op (ON CONFLICT DO NOTHING). Skipped when no Docker
daemon is available; runs in CI.
"""

from datetime import datetime
from pathlib import Path

import psycopg2
import pytest

pytestmark = pytest.mark.integration

pytest.importorskip("testcontainers.postgres")

from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from testcontainers.postgres import PostgresContainer  # noqa: E402

import streaming_job  # noqa: E402

INIT_SQL = Path(__file__).resolve().parents[2] / "sql" / "init.sql"

CANDLE_SCHEMA = StructType(
    [
        StructField("symbol", StringType()),
        StructField("window_start", TimestampType()),
        StructField("window_end", TimestampType()),
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("volume", DoubleType()),
    ]
)

W_START = datetime(2026, 7, 1, 14, 32, 0)
W_END = datetime(2026, 7, 1, 14, 33, 0)


def _connect(pg):
    return psycopg2.connect(
        host=pg.get_container_host_ip(),
        port=pg.get_exposed_port(5432),
        dbname="tradepulse",
        user="tradepulse",
        password="pw",
    )


def _count(pg, table):
    conn = _connect(pg)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {table}")
            return cur.fetchone()[0]
    finally:
        conn.close()


def test_sink_writes_candles_and_alerts_idempotently(spark, monkeypatch):
    with PostgresContainer(
        "postgres:18", username="tradepulse", password="pw", dbname="tradepulse"
    ) as pg:
        conn = _connect(pg)
        try:
            with conn, conn.cursor() as cur:
                cur.execute(INIT_SQL.read_text())
        finally:
            conn.close()

        for attr, val in {
            "POSTGRES_HOST": pg.get_container_host_ip(),
            "POSTGRES_PORT": pg.get_exposed_port(5432),
            "POSTGRES_DB": "tradepulse",
            "POSTGRES_USER": "tradepulse",
            "POSTGRES_PASSWORD": "pw",
        }.items():
            monkeypatch.setattr(streaming_job, attr, val)

        rows = [
            ("BTC-USD", W_START, W_END, 100.0, 101.0, 99.0, 100.5, 5.0),   # +0.5%, no alert
            ("ETH-USD", W_START, W_END, 100.0, 106.0, 100.0, 105.0, 3.0),  # +5%, alert
        ]
        df = spark.createDataFrame(rows, schema=CANDLE_SCHEMA)

        sink = streaming_job.make_sink(threshold=1.0)
        sink(df, 0)

        assert _count(pg, "candles") == 2
        assert _count(pg, "alerts") == 1

        conn = _connect(pg)
        try:
            with conn, conn.cursor() as cur:
                cur.execute("SELECT symbol, pct_change FROM alerts")
                sym, pct = cur.fetchone()
                assert sym == "ETH-USD"
                assert round(float(pct), 2) == 5.00
        finally:
            conn.close()

        # Replay the same batch: idempotent, no new rows.
        sink(df, 1)
        assert _count(pg, "candles") == 2
        assert _count(pg, "alerts") == 1
