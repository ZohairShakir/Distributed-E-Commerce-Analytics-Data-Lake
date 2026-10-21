from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from aws_spark_data_platform.config.settings import SparkSettings
from aws_spark_data_platform.config.spark import create_spark_session
from aws_spark_data_platform.generator import GenerationConfig, generate_dataset
from aws_spark_data_platform.orchestration.smoke_job import run_smoke_job


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = create_spark_session(
        "phase1-integration-test",
        SparkSettings(master="local[2]", driver_memory="1g", shuffle_partitions=2),
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.mark.integration
def test_spark_smoke_job_reconciles_and_writes_parquet(
    tmp_path: Path,
    spark: SparkSession,
) -> None:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    generate_dataset(
        input_root,
        GenerationConfig(
            event_count=300,
            invalid_rate=0.02,
            late_arrival_rate=0.05,
            seed=7,
            batch_id="integration-test",
        ),
    )

    counts = run_smoke_job(spark, input_root, output_root)

    assert set(counts) == {"add_to_cart", "product_view", "purchase"}
    assert sum(counts.values()) == 300
    persisted = spark.read.parquet(str(output_root / "event_counts"))
    assert persisted.count() == 3
    assert sum(row["event_count"] for row in persisted.collect()) == 300
