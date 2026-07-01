# Spark 4.x requires Java 17+. temurin gives us a JRE with JAVA_HOME set.
FROM eclipse-temurin:17-jre-jammy

ENV PYTHONUNBUFFERED=1 \
    SPARK_IVY_DIR=/opt/.ivy2 \
    VIRTUAL_ENV=/opt/venv \
    HEARTBEAT_FILE=/tmp/spark_heartbeat \
    PATH=/opt/venv/bin:$PATH

# Python + procps (Spark shells out to `ps`).
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-venv procps \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m venv "$VIRTUAL_ENV"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Warm the Ivy cache so the Kafka connector jar is baked into the image (fast,
# offline-capable startup instead of resolving from Maven on every container
# start). Done before copying the app so code edits don't re-download jars.
ENV SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2"
RUN python -c "from pyspark.sql import SparkSession; import os; \
SparkSession.builder.master('local[1]') \
  .config('spark.jars.packages', os.environ['SPARK_PACKAGES']) \
  .config('spark.jars.ivy', os.environ['SPARK_IVY_DIR']) \
  .getOrCreate().stop()"

COPY streaming_job.py .

# Unhealthy if the job has not made streaming progress in the last 60s. The
# long start-period covers JVM + Spark startup and first micro-batch.
HEALTHCHECK --interval=20s --timeout=5s --start-period=90s --retries=3 \
    CMD test "$(( $(date +%s) - $(stat -c %Y "$HEARTBEAT_FILE" 2>/dev/null || echo 0) ))" -lt 60

CMD ["python", "streaming_job.py"]
