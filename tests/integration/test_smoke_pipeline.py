from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from aws_spark_data_platform.bronze import (
    BatchAlreadyProcessedError,
    SourceAlreadyProcessedError,
    ingest_bronze_batch,
)
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


@pytest.mark.integration
def test_bronze_ingestion_is_traceable_atomic_and_idempotent(
    tmp_path: Path,
    spark: SparkSession,
) -> None:
    source_root = tmp_path / "source"
    lake_root = tmp_path / "lake"
    generation = generate_dataset(
        source_root,
        GenerationConfig(
            event_count=300,
            invalid_rate=0.02,
            late_arrival_rate=0.05,
            seed=11,
            batch_id="source-delivery-1",
        ),
    )
    extra_event = {
        "event_id": "EVENT-EXTRA-0001",
        "event_type": "product_view",
        "event_ts": "2026-01-01T00:00:00Z",
        "received_at": "2026-01-01T00:00:01Z",
        "customer_id": "CUST-0000001",
        "session_id": "SESSION-EXTRA",
        "product_id": "PROD-00001",
        "order_id": None,
        "region": "ap-south-1",
        "source_batch_id": "source-delivery-1",
    }
    second_event_file = source_root / "behavioral_events_part2.jsonl"
    second_event_file.write_text(
        json.dumps(extra_event, sort_keys=True) + "\n" + '{"event_id":"broken"\n',
        encoding="utf-8",
    )
    fixed_ingestion_time = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

    result = ingest_bronze_batch(
        spark,
        source_root,
        lake_root,
        "batch-test-001",
        ingested_at=fixed_ingestion_time,
    )

    assert result.source_file_count == 7
    assert result.records_read["behavioral_events"] == 302
    assert result.malformed_records["behavioral_events"] == 1
    assert sum(result.malformed_records.values()) == 1
    assert not (lake_root / "_staging").exists()

    bronze_events = spark.read.parquet(str(result.batch_path / "tables/behavioral_events"))
    assert bronze_events.count() == 302
    assert bronze_events.filter("_ingestion_batch_id = 'batch-test-001'").count() == 302
    assert bronze_events.filter("_is_malformed").count() == 1
    corrupt = bronze_events.filter("_is_malformed").first()
    assert corrupt["_corrupt_record"] == '{"event_id":"broken"'
    assert corrupt["_source_record_hash"] is not None
    assert corrupt["_source_file"].endswith("behavioral_events_part2.jsonl")
    assert corrupt["_ingested_at"] == fixed_ingestion_time.replace(tzinfo=None)

    copied_file = result.batch_path / "original/behavioral_events" / second_event_file.name
    assert copied_file.read_bytes() == second_event_file.read_bytes()
    metadata = json.loads(result.metadata_path.read_text(encoding="utf-8"))
    assert metadata["status"] == "committed"
    assert metadata["synthetic_source"] is True
    assert metadata["source_fingerprint"] == result.source_fingerprint
    assert metadata["records_read"]["behavioral_events"] == 302

    with pytest.raises(BatchAlreadyProcessedError):
        ingest_bronze_batch(spark, source_root, lake_root, "batch-test-001")

    original_event_file = source_root / "behavioral_events.jsonl"
    original_event_file.rename(source_root / "behavioral_events_renamed.jsonl")
    with pytest.raises(SourceAlreadyProcessedError):
        ingest_bronze_batch(spark, source_root, lake_root, "batch-test-002")

    source_manifest = json.loads(generation.manifest_path.read_text(encoding="utf-8"))
    assert source_manifest["synthetic_data"] is True


@pytest.mark.integration
def test_bronze_rejects_invalid_batch_identifier(tmp_path: Path, spark: SparkSession) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()

    with pytest.raises(ValueError, match="batch_id"):
        ingest_bronze_batch(spark, source_root, tmp_path / "lake", "../unsafe")
