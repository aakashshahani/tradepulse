#!/usr/bin/env bash
#
# Fault-tolerance demo: kill the Spark job mid-stream and show it resume from its
# checkpoint with no lost or duplicated candles. The guarantees come from Spark
# Structured Streaming checkpoints (offset + state recovery) plus the idempotent
# ON CONFLICT DO NOTHING sink.
#
# Usage: bash scripts/demo_recovery.sh   (from the repo root, stack already up)
set -euo pipefail
cd "$(dirname "$0")/.."

psql() { docker compose exec -T postgres psql -U tradepulse -d tradepulse -tAc "$1"; }

echo "== Candles before kill: $(psql 'SELECT count(*) FROM candles;')"
echo "== Duplicate candles before: $(psql 'SELECT count(*) - count(DISTINCT (symbol, window_start)) FROM candles;')"

echo
echo "== Killing spark_job (simulating a crash) ..."
docker compose kill spark_job >/dev/null

echo "== Restarting; it resumes from the last committed checkpoint offset ..."
docker compose up -d spark_job >/dev/null

echo "== Waiting ~50s for it to recover and process a few batches ..."
sleep 50

echo
echo "== Candles after recovery: $(psql 'SELECT count(*) FROM candles;')"
echo "== Duplicate candles after: $(psql 'SELECT count(*) - count(DISTINCT (symbol, window_start)) FROM candles;')  (expect 0)"
echo
echo "== Recent spark_job log (note it resumes, does not restart from scratch):"
docker compose logs --no-log-prefix --tail 6 spark_job
