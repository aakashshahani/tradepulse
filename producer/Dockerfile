FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HEARTBEAT_FILE=/tmp/tradepulse_heartbeat

WORKDIR /app

# Unhealthy if the heartbeat file hasn't been touched in the last 30s
# (feed silent or disconnected). start-period covers WS connect + first msgs.
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
    CMD test "$(( $(date +%s) - $(stat -c %Y "$HEARTBEAT_FILE" 2>/dev/null || echo 0) ))" -lt 30

# confluent-kafka ships manylinux wheels with librdkafka bundled, so no
# apt build deps are needed on slim.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Run as a non-root user.
RUN useradd --create-home --uid 10001 appuser
USER appuser

CMD ["python", "producer.py"]
