"""Local Spark session factory."""

from __future__ import annotations

from pyspark.sql import SparkSession

from aws_spark_data_platform.config.settings import SparkSettings


def create_spark_session(
    app_name: str,
    settings: SparkSettings | None = None,
) -> SparkSession:
    """Create a deterministic, low-resource Spark session for local development."""
    selected = settings or SparkSettings.from_environment()
    return (
        SparkSession.builder.appName(app_name)
        .master(selected.master)
        .config("spark.driver.memory", selected.driver_memory)
        .config("spark.sql.shuffle.partitions", str(selected.shuffle_partitions))
        .config("spark.sql.session.timeZone", selected.timezone)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
