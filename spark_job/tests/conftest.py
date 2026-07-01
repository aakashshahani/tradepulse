import pytest
from pyspark.sql import SparkSession


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: test needs a running Docker daemon (testcontainers)"
    )


def pytest_collection_modifyitems(config, items):
    """Skip integration tests when no Docker daemon is reachable (e.g. running
    inside the spark image locally). CI has Docker, so they run there."""
    try:
        import docker

        docker.from_env().ping()
        return
    except Exception:
        skip = pytest.mark.skip(reason="Docker not available for integration tests")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("tradepulse-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()
