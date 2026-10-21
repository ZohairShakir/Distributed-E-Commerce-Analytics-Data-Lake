from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DecimalType, TimestampType

from aws_spark_data_platform.bronze import (
    BatchAlreadyProcessedError,
    SourceAlreadyProcessedError,
    ingest_bronze_batch,
)
from aws_spark_data_platform.config.settings import SparkSettings
from aws_spark_data_platform.config.spark import create_spark_session
from aws_spark_data_platform.generator import GenerationConfig, generate_dataset
from aws_spark_data_platform.gold import GoldBatchAlreadyProcessedError, process_gold_batch
from aws_spark_data_platform.orchestration.smoke_job import run_smoke_job
from aws_spark_data_platform.silver import (
    SilverBatchAlreadyProcessedError,
    process_silver_batch,
)


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


@pytest.mark.integration
def test_silver_processing_types_quarantines_deduplicates_and_reconciles(
    tmp_path: Path,
    spark: SparkSession,
) -> None:
    source_root = tmp_path / "source"
    lake_root = tmp_path / "lake"
    generation = generate_dataset(
        source_root,
        GenerationConfig(
            event_count=300,
            invalid_rate=0.05,
            late_arrival_rate=0.10,
            seed=23,
            batch_id="silver-source",
        ),
    )
    customer_file = source_root / "customers.csv"
    customer_lines = customer_file.read_text(encoding="utf-8").splitlines()
    customer_file.write_text(
        "\n".join(
            [
                *customer_lines,
                customer_lines[1],
                ",new,NA,2026-01-01T00:00:00Z,silver-source",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    with (source_root / "behavioral_events.jsonl").open("a", encoding="utf-8") as handle:
        handle.write('{"event_id":"truncated"\n')

    ingest_bronze_batch(
        spark,
        source_root,
        lake_root,
        "silver-test-001",
        ingested_at=datetime(2026, 1, 2, tzinfo=UTC),
    )
    result = process_silver_batch(
        spark,
        lake_root,
        "silver-test-001",
        processed_at=datetime(2026, 1, 3, tzinfo=UTC),
    )

    assert result.records_read["customers"] == generation.record_counts["customers"] + 2
    assert result.records_read["behavioral_events"] == generation.record_counts["events"] + 1
    assert result.duplicate_counts["customers"] == 1
    assert result.null_counts["customers"] == 1
    assert result.reason_counts["customers"]["DUPLICATE_RECORD"] == 1
    assert result.reason_counts["customers"]["NULL_CUSTOMER_ID"] == 1
    assert result.reason_counts["behavioral_events"]["MALFORMED_RECORD"] == 1
    assert result.records_rejected["orders"] == generation.invalid_counts["orders"]
    assert result.records_rejected["order_items"] >= generation.invalid_counts["order_items"]
    assert result.records_rejected["payments"] >= generation.invalid_counts["payments"]
    assert result.records_rejected["behavioral_events"] >= generation.invalid_counts["events"] + 1
    for dataset, records_read in result.records_read.items():
        assert records_read == (result.records_accepted[dataset] + result.records_rejected[dataset])

    products = spark.read.parquet(str(result.silver_path / "tables/products"))
    events = spark.read.parquet(str(result.silver_path / "tables/behavioral_events"))
    assert isinstance(products.schema["unit_price"].dataType, DecimalType)
    assert isinstance(products.schema["is_active"].dataType, BooleanType)
    assert isinstance(events.schema["event_ts"].dataType, TimestampType)
    assert isinstance(events.schema["received_at"].dataType, TimestampType)
    assert isinstance(events.schema["is_late"].dataType, BooleanType)
    assert 0 < events.filter("is_late").count() <= generation.late_event_count

    customer_quarantine = spark.read.parquet(str(result.quarantine_path / "customers"))
    duplicate = customer_quarantine.filter(
        F.array_contains("_validation_reasons", "DUPLICATE_RECORD")
    ).first()
    assert duplicate["customer_id"] is not None

    report = json.loads(result.quality_report_path.read_text(encoding="utf-8"))
    assert report["status"] == "committed"
    assert report["reconciliation_passed"] is True
    assert report["processed_at_utc"] == "2026-01-03T00:00:00Z"
    assert (lake_root / "metadata/silver_batches/batch_id=silver-test-001.json").is_file()

    with pytest.raises(SilverBatchAlreadyProcessedError):
        process_silver_batch(spark, lake_root, "silver-test-001")


@pytest.mark.integration
def test_gold_analytics_use_reconciled_financial_rules(
    tmp_path: Path,
    spark: SparkSession,
) -> None:
    lake_root = tmp_path / "lake"
    batch_id = "gold-test-001"
    table_root = lake_root / "silver" / f"batch_id={batch_id}" / "tables"
    tables = {
        "customers": spark.createDataFrame(
            [("C1",), ("C2",)],
            "customer_id string",
        ),
        "products": spark.createDataFrame(
            [("P1", "books"), ("P2", "sports")],
            "product_id string, category string",
        ),
        "orders": spark.createDataFrame(
            [
                ("O1", "C1", datetime(2026, 10, 1, 10), "NA", "completed"),
                ("O2", "C1", datetime(2026, 10, 2, 10), "NA", "refunded"),
                ("O3", "C2", datetime(2026, 10, 1, 11), "EU", "cancelled"),
                ("O4", "C2", datetime(2026, 10, 1, 12), "EU", "completed"),
            ],
            "order_id string, customer_id string, order_ts timestamp, region string, status string",
        ),
        "order_items": spark.createDataFrame(
            [
                ("O1", "P1", 2, Decimal("10.00"), Decimal("1.00")),
                ("O2", "P1", 1, Decimal("10.00"), Decimal("0.00")),
                ("O3", "P2", 1, Decimal("5.00"), Decimal("0.00")),
                ("O4", "P2", 1, Decimal("5.00"), Decimal("0.00")),
            ],
            (
                "order_id string, product_id string, quantity int, "
                "unit_price decimal(18,2), discount_amount decimal(18,2)"
            ),
        ),
        "payments": spark.createDataFrame(
            [
                ("O1", "capture", Decimal("19.00")),
                ("O2", "refund", Decimal("10.00")),
                ("O3", "capture", Decimal("5.00")),
                ("O4", "capture", Decimal("4.00")),
            ],
            "order_id string, payment_event_type string, amount decimal(18,2)",
        ),
        "behavioral_events": spark.createDataFrame(
            [
                ("NA", "product_view"),
                ("NA", "product_view"),
                ("NA", "add_to_cart"),
                ("NA", "purchase"),
                ("EU", "product_view"),
                ("EU", "purchase"),
            ],
            "region string, event_type string",
        ),
    }
    for name, frame in tables.items():
        frame.write.parquet(str(table_root / name))
    silver_commit = lake_root / "metadata/silver_batches" / f"batch_id={batch_id}.json"
    silver_commit.parent.mkdir(parents=True)
    silver_commit.write_text('{"status":"committed"}\n', encoding="utf-8")

    result = process_gold_batch(
        spark,
        lake_root,
        batch_id,
        processed_at=datetime(2026, 10, 3, tzinfo=UTC),
    )

    assert result.qualified_order_count == 2
    assert result.excluded_order_count == 2
    assert result.net_revenue == Decimal("9.00")
    assert result.row_counts == {
        "daily_revenue": 2,
        "product_performance": 1,
        "customer_activity": 1,
        "conversion_funnel": 2,
        "regional_sales": 1,
    }

    daily = spark.read.parquet(str(result.gold_path / "tables/daily_revenue"))
    daily_rows = {(row["date"], row["region"]): row for row in daily.collect()}
    assert daily_rows[(date(2026, 10, 1), "NA")]["revenue"] == Decimal("19.00")
    assert daily_rows[(date(2026, 10, 2), "NA")]["revenue"] == Decimal("-10.00")

    product = spark.read.parquet(str(result.gold_path / "tables/product_performance")).first()
    assert product["product_id"] == "P1"
    assert product["units_sold"] == 1
    assert product["revenue"] == Decimal("9.00")
    assert product["sales_rank"] == 1

    customer = spark.read.parquet(str(result.gold_path / "tables/customer_activity")).first()
    assert customer["customer_id"] == "C1"
    assert customer["order_frequency"] == 2
    assert customer["total_spend"] == Decimal("9.00")
    assert customer["last_purchase_date"] == date(2026, 10, 1)

    funnel = spark.read.parquet(str(result.gold_path / "tables/conversion_funnel"))
    na_funnel = funnel.filter("region = 'NA'").first()
    assert na_funnel["product_views"] == 2
    assert na_funnel["purchase_events"] == 1
    assert na_funnel["conversion_rate_percent"] == Decimal("50.0000")

    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report["revenue_reconciliation_passed"] is True
    assert report["net_revenue"] == "9.00"

    with pytest.raises(GoldBatchAlreadyProcessedError):
        process_gold_batch(spark, lake_root, batch_id)
