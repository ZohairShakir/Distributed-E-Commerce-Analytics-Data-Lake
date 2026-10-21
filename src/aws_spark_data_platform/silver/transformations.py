"""Phase 3 Silver normalization, validation, deduplication, and quarantine."""

from __future__ import annotations

import json
import shutil
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from aws_spark_data_platform.generator.synthetic import (
    CATEGORIES,
    EVENT_TYPES,
    REGIONS,
    SEGMENTS,
)

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"
VALID_ORDER_STATUSES = ("completed", "cancelled", "refunded")
VALID_PAYMENT_EVENT_TYPES = ("capture", "refund")
INTERNAL_COLUMNS = (
    "_duplicate_rank",
    "_is_duplicate",
    "_has_required_null",
    "_validation_reasons",
)


class SilverBatchAlreadyProcessedError(RuntimeError):
    """Raised when a Bronze batch already has committed Silver output."""


@dataclass(frozen=True)
class SilverProcessingResult:
    batch_id: str
    silver_path: Path
    quarantine_path: Path
    quality_report_path: Path
    records_read: dict[str, int]
    records_accepted: dict[str, int]
    records_rejected: dict[str, int]
    duplicate_counts: dict[str, int]
    null_counts: dict[str, int]
    reason_counts: dict[str, dict[str, int]]
    processing_duration_seconds: float


def _normalized_text(frame: DataFrame, columns: Iterable[str]) -> DataFrame:
    normalized = frame
    for name in columns:
        normalized = normalized.withColumn(
            name,
            F.when(F.trim(F.col(name)) == "", F.lit(None)).otherwise(F.trim(F.col(name))),
        )
    return normalized


def _with_duplicate_marker(frame: DataFrame, key_columns: list[str]) -> DataFrame:
    ordering = [
        F.col("_ingested_at").asc_nulls_last(),
        F.col("_source_file").asc_nulls_last(),
        F.col("_source_record_hash").asc_nulls_last(),
    ]
    window = Window.partitionBy(*key_columns).orderBy(*ordering)
    return frame.withColumn("_duplicate_rank", F.row_number().over(window)).withColumn(
        "_is_duplicate", F.col("_duplicate_rank") > 1
    )


def _reason_array(rules: list[tuple[Column, str]]) -> Column:
    return F.array_compact(
        F.array(*[F.when(condition, F.lit(reason)) for condition, reason in rules])
    )


def _base_validation(
    frame: DataFrame,
    *,
    key_columns: list[str],
    required_columns: list[str],
) -> DataFrame:
    normalized = _normalized_text(frame, required_columns)
    with_duplicates = _with_duplicate_marker(normalized, key_columns)
    required_null = F.lit(False)
    for name in required_columns:
        required_null = required_null | F.col(name).isNull()
    return with_duplicates.withColumn("_has_required_null", required_null)


def _common_rules(frame: DataFrame) -> list[tuple[Column, str]]:
    return [
        (F.col("_is_malformed"), "MALFORMED_RECORD"),
        (F.col("_is_duplicate"), "DUPLICATE_RECORD"),
        (F.col("_has_required_null"), "NULL_REQUIRED_FIELD"),
    ]


def _customers(bronze: DataFrame) -> DataFrame:
    frame = _base_validation(
        bronze,
        key_columns=["customer_id"],
        required_columns=[
            "customer_id",
            "customer_segment",
            "home_region",
            "created_at",
            "source_batch_id",
        ],
    ).withColumn("created_at", F.to_timestamp("created_at", TIMESTAMP_FORMAT))
    rules = [
        *_common_rules(frame),
        (F.col("customer_id").isNull(), "NULL_CUSTOMER_ID"),
        (~F.col("customer_segment").isin(*SEGMENTS), "INVALID_CUSTOMER_SEGMENT"),
        (~F.col("home_region").isin(*REGIONS), "INVALID_HOME_REGION"),
        (F.col("created_at").isNull(), "INVALID_CREATED_AT"),
        (F.col("source_batch_id").isNull(), "NULL_SOURCE_BATCH_ID"),
    ]
    return frame.withColumn("_validation_reasons", _reason_array(rules))


def _products(bronze: DataFrame) -> DataFrame:
    frame = _base_validation(
        bronze,
        key_columns=["product_id"],
        required_columns=[
            "product_id",
            "category",
            "unit_price",
            "currency",
            "is_active",
            "source_batch_id",
        ],
    )
    frame = (
        frame.withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
        .withColumn(
            "is_active",
            F.when(F.lower("is_active") == "true", F.lit(True))
            .when(F.lower("is_active") == "false", F.lit(False))
            .otherwise(F.lit(None).cast("boolean")),
        )
        .withColumn("currency", F.upper("currency"))
    )
    rules = [
        *_common_rules(frame),
        (F.col("product_id").isNull(), "NULL_PRODUCT_ID"),
        (~F.col("category").isin(*CATEGORIES), "INVALID_CATEGORY"),
        (F.col("unit_price").isNull(), "INVALID_UNIT_PRICE"),
        (F.col("unit_price") < 0, "NEGATIVE_UNIT_PRICE"),
        (F.col("currency") != "USD", "INVALID_CURRENCY"),
        (F.col("is_active").isNull(), "INVALID_ACTIVE_FLAG"),
        (F.col("source_batch_id").isNull(), "NULL_SOURCE_BATCH_ID"),
    ]
    return frame.withColumn("_validation_reasons", _reason_array(rules))


def _orders(bronze: DataFrame, valid_customer_ids: DataFrame) -> DataFrame:
    frame = _base_validation(
        bronze,
        key_columns=["order_id"],
        required_columns=[
            "order_id",
            "customer_id",
            "order_ts",
            "region",
            "status",
            "currency",
            "source_batch_id",
        ],
    )
    frame = (
        frame.withColumn("order_ts", F.to_timestamp("order_ts", TIMESTAMP_FORMAT))
        .withColumn("status", F.lower("status"))
        .withColumn("currency", F.upper("currency"))
        .join(
            F.broadcast(valid_customer_ids.withColumn("_customer_exists", F.lit(True))),
            on="customer_id",
            how="left",
        )
    )
    rules = [
        *_common_rules(frame),
        (F.col("order_id").isNull(), "NULL_ORDER_ID"),
        (F.col("customer_id").isNull(), "NULL_CUSTOMER_ID"),
        (F.col("_customer_exists").isNull(), "UNKNOWN_CUSTOMER_ID"),
        (F.col("order_ts").isNull(), "INVALID_ORDER_TIMESTAMP"),
        (~F.col("region").isin(*REGIONS), "INVALID_REGION"),
        (~F.col("status").isin(*VALID_ORDER_STATUSES), "INVALID_ORDER_STATUS"),
        (F.col("currency") != "USD", "INVALID_CURRENCY"),
        (F.col("source_batch_id").isNull(), "NULL_SOURCE_BATCH_ID"),
    ]
    return frame.withColumn("_validation_reasons", _reason_array(rules)).drop("_customer_exists")


def _order_items(
    bronze: DataFrame,
    valid_order_ids: DataFrame,
    valid_product_ids: DataFrame,
) -> DataFrame:
    frame = _base_validation(
        bronze,
        key_columns=["order_item_id"],
        required_columns=[
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "discount_amount",
            "source_batch_id",
        ],
    )
    frame = (
        frame.withColumn("quantity", F.col("quantity").cast("integer"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
        .withColumn("discount_amount", F.col("discount_amount").cast("decimal(18,2)"))
        .join(
            F.broadcast(valid_order_ids.withColumn("_order_exists", F.lit(True))),
            on="order_id",
            how="left",
        )
        .join(
            F.broadcast(valid_product_ids.withColumn("_product_exists", F.lit(True))),
            on="product_id",
            how="left",
        )
    )
    rules = [
        *_common_rules(frame),
        (F.col("order_item_id").isNull(), "NULL_ORDER_ITEM_ID"),
        (F.col("order_id").isNull(), "NULL_ORDER_ID"),
        (F.col("_order_exists").isNull(), "UNKNOWN_ORDER_ID"),
        (F.col("product_id").isNull(), "NULL_PRODUCT_ID"),
        (F.col("_product_exists").isNull(), "UNKNOWN_PRODUCT_ID"),
        (F.col("quantity").isNull(), "INVALID_QUANTITY"),
        (F.col("quantity") <= 0, "NON_POSITIVE_QUANTITY"),
        (F.col("unit_price").isNull(), "INVALID_UNIT_PRICE"),
        (F.col("unit_price") < 0, "NEGATIVE_UNIT_PRICE"),
        (F.col("discount_amount").isNull(), "INVALID_DISCOUNT_AMOUNT"),
        (F.col("discount_amount") < 0, "NEGATIVE_DISCOUNT_AMOUNT"),
        (
            F.col("discount_amount") > F.col("unit_price") * F.col("quantity"),
            "EXCESSIVE_DISCOUNT_AMOUNT",
        ),
        (F.col("source_batch_id").isNull(), "NULL_SOURCE_BATCH_ID"),
    ]
    return frame.withColumn("_validation_reasons", _reason_array(rules)).drop(
        "_order_exists", "_product_exists"
    )


def _payments(bronze: DataFrame, valid_order_ids: DataFrame) -> DataFrame:
    frame = _base_validation(
        bronze,
        key_columns=["payment_id"],
        required_columns=[
            "payment_id",
            "order_id",
            "payment_event_type",
            "amount",
            "currency",
            "event_ts",
            "source_batch_id",
        ],
    )
    frame = (
        frame.withColumn("payment_event_type", F.lower("payment_event_type"))
        .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
        .withColumn("currency", F.upper("currency"))
        .withColumn("event_ts", F.to_timestamp("event_ts", TIMESTAMP_FORMAT))
        .join(
            F.broadcast(valid_order_ids.withColumn("_order_exists", F.lit(True))),
            on="order_id",
            how="left",
        )
    )
    rules = [
        *_common_rules(frame),
        (F.col("payment_id").isNull(), "NULL_PAYMENT_ID"),
        (F.col("order_id").isNull(), "NULL_ORDER_ID"),
        (F.col("_order_exists").isNull(), "UNKNOWN_ORDER_ID"),
        (
            ~F.col("payment_event_type").isin(*VALID_PAYMENT_EVENT_TYPES),
            "INVALID_PAYMENT_EVENT_TYPE",
        ),
        (F.col("amount").isNull(), "INVALID_AMOUNT"),
        (F.col("amount") < 0, "NEGATIVE_AMOUNT"),
        (F.col("currency") != "USD", "INVALID_CURRENCY"),
        (F.col("event_ts").isNull(), "INVALID_PAYMENT_TIMESTAMP"),
        (F.col("source_batch_id").isNull(), "NULL_SOURCE_BATCH_ID"),
    ]
    return frame.withColumn("_validation_reasons", _reason_array(rules)).drop("_order_exists")


def _behavioral_events(
    bronze: DataFrame,
    valid_customer_ids: DataFrame,
    valid_product_ids: DataFrame,
    valid_order_ids: DataFrame,
) -> DataFrame:
    frame = _base_validation(
        bronze,
        key_columns=["event_id"],
        required_columns=[
            "event_id",
            "event_type",
            "event_ts",
            "received_at",
            "customer_id",
            "session_id",
            "product_id",
            "region",
            "source_batch_id",
        ],
    )
    frame = (
        frame.withColumn("event_type", F.lower("event_type"))
        .withColumn("event_ts", F.to_timestamp("event_ts", TIMESTAMP_FORMAT))
        .withColumn("received_at", F.to_timestamp("received_at", TIMESTAMP_FORMAT))
        .withColumn(
            "arrival_delay_seconds",
            F.col("received_at").cast("long") - F.col("event_ts").cast("long"),
        )
        .withColumn("is_late", F.col("arrival_delay_seconds") > 86_400)
        .join(
            F.broadcast(valid_customer_ids.withColumn("_customer_exists", F.lit(True))),
            on="customer_id",
            how="left",
        )
        .join(
            F.broadcast(valid_product_ids.withColumn("_product_exists", F.lit(True))),
            on="product_id",
            how="left",
        )
        .join(
            F.broadcast(valid_order_ids.withColumn("_order_exists", F.lit(True))),
            on="order_id",
            how="left",
        )
    )
    rules = [
        *_common_rules(frame),
        (F.col("event_id").isNull(), "NULL_EVENT_ID"),
        (~F.col("event_type").isin(*EVENT_TYPES), "INVALID_EVENT_TYPE"),
        (F.col("event_ts").isNull(), "INVALID_EVENT_TIMESTAMP"),
        (F.col("received_at").isNull(), "INVALID_RECEIVED_TIMESTAMP"),
        (F.col("arrival_delay_seconds") < 0, "RECEIVED_BEFORE_EVENT"),
        (F.col("customer_id").isNull(), "NULL_CUSTOMER_ID"),
        (F.col("_customer_exists").isNull(), "UNKNOWN_CUSTOMER_ID"),
        (F.col("session_id").isNull(), "NULL_SESSION_ID"),
        (F.col("product_id").isNull(), "NULL_PRODUCT_ID"),
        (F.col("_product_exists").isNull(), "UNKNOWN_PRODUCT_ID"),
        (F.col("region").isNull(), "NULL_REGION"),
        (~F.col("region").isin(*REGIONS), "INVALID_REGION"),
        (
            (F.col("event_type") == "purchase") & F.col("order_id").isNull(),
            "PURCHASE_WITHOUT_ORDER_ID",
        ),
        (
            (F.col("event_type") == "purchase") & F.col("_order_exists").isNull(),
            "UNKNOWN_PURCHASE_ORDER_ID",
        ),
        (F.col("source_batch_id").isNull(), "NULL_SOURCE_BATCH_ID"),
    ]
    return frame.withColumn("_validation_reasons", _reason_array(rules)).drop(
        "_customer_exists", "_product_exists", "_order_exists"
    )


def _accepted(frame: DataFrame) -> DataFrame:
    return frame.filter(F.size("_validation_reasons") == 0).drop(*INTERNAL_COLUMNS)


def _dataset_metrics(frame: DataFrame) -> tuple[dict[str, int], dict[str, int]]:
    summary = frame.agg(
        F.count(F.lit(1)).alias("records_read"),
        F.sum(F.when(F.size("_validation_reasons") == 0, 1).otherwise(0)).alias("records_accepted"),
        F.sum(F.when(F.size("_validation_reasons") > 0, 1).otherwise(0)).alias("records_rejected"),
        F.sum(F.col("_is_duplicate").cast("long")).alias("duplicate_count"),
        F.sum(F.col("_has_required_null").cast("long")).alias("null_count"),
    ).first()
    counts = {name: int(summary[name] or 0) for name in summary.asDict()}
    reasons = {
        row["reason"]: row["count"]
        for row in (
            frame.select(F.explode("_validation_reasons").alias("reason"))
            .groupBy("reason")
            .count()
            .collect()
        )
    }
    return counts, reasons


def _load_bronze_tables(spark: SparkSession, bronze_batch_path: Path) -> dict[str, DataFrame]:
    table_root = bronze_batch_path / "tables"
    datasets = (
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "behavioral_events",
    )
    missing = [name for name in datasets if not (table_root / name).is_dir()]
    if missing:
        raise FileNotFoundError(f"Bronze batch is missing tables: {', '.join(missing)}")
    return {name: spark.read.parquet(str(table_root / name)) for name in datasets}


def _build_validated_frames(bronze: dict[str, DataFrame]) -> dict[str, DataFrame]:
    customers = _customers(bronze["customers"])
    products = _products(bronze["products"])
    valid_customer_ids = _accepted(customers).select("customer_id")
    valid_product_ids = _accepted(products).select("product_id")
    orders = _orders(bronze["orders"], valid_customer_ids)
    valid_order_ids = _accepted(orders).select("order_id")
    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": _order_items(bronze["order_items"], valid_order_ids, valid_product_ids),
        "payments": _payments(bronze["payments"], valid_order_ids),
        "behavioral_events": _behavioral_events(
            bronze["behavioral_events"],
            valid_customer_ids,
            valid_product_ids,
            valid_order_ids,
        ),
    }


def process_silver_batch(
    spark: SparkSession,
    lake_root: str | Path,
    batch_id: str,
    *,
    processed_at: datetime | None = None,
) -> SilverProcessingResult:
    """Process one committed Bronze batch into Silver and quarantine outputs."""
    lake = Path(lake_root).resolve()
    bronze_batch = lake / "bronze" / f"batch_id={batch_id}"
    if not bronze_batch.is_dir():
        raise FileNotFoundError(f"Committed Bronze batch does not exist: {bronze_batch}")

    silver_path = lake / "silver" / f"batch_id={batch_id}"
    quarantine_path = lake / "quarantine" / f"batch_id={batch_id}"
    quality_path = lake / "quality" / f"batch_id={batch_id}"
    commit_path = lake / "metadata" / "silver_batches" / f"batch_id={batch_id}.json"
    output_exists = any(
        path.exists() for path in (commit_path, silver_path, quarantine_path, quality_path)
    )
    if output_exists:
        raise SilverBatchAlreadyProcessedError(
            f"Silver output already exists for Bronze batch: {batch_id}"
        )

    started = time.perf_counter()
    timestamp = (processed_at or datetime.now(UTC)).astimezone(UTC)
    staging = lake / "_staging" / f"silver-{batch_id}-{uuid.uuid4().hex}"
    frames = _build_validated_frames(_load_bronze_tables(spark, bronze_batch))
    records_read: dict[str, int] = {}
    records_accepted: dict[str, int] = {}
    records_rejected: dict[str, int] = {}
    duplicate_counts: dict[str, int] = {}
    null_counts: dict[str, int] = {}
    reason_counts: dict[str, dict[str, int]] = {}

    try:
        for dataset, frame in frames.items():
            frame.persist(StorageLevel.MEMORY_AND_DISK)
            try:
                counts, reasons = _dataset_metrics(frame)
                records_read[dataset] = counts["records_read"]
                records_accepted[dataset] = counts["records_accepted"]
                records_rejected[dataset] = counts["records_rejected"]
                duplicate_counts[dataset] = counts["duplicate_count"]
                null_counts[dataset] = counts["null_count"]
                reason_counts[dataset] = reasons

                _accepted(frame).write.mode("errorifexists").option(
                    "compression", "snappy"
                ).parquet(str(staging / "silver" / "tables" / dataset))
                frame.filter(F.size("_validation_reasons") > 0).drop(
                    "_duplicate_rank", "_is_duplicate", "_has_required_null"
                ).write.mode("errorifexists").option("compression", "snappy").parquet(
                    str(staging / "quarantine" / dataset)
                )
            finally:
                frame.unpersist()

        duration = round(time.perf_counter() - started, 6)
        report = {
            "status": "committed",
            "batch_id": batch_id,
            "processed_at_utc": timestamp.isoformat().replace("+00:00", "Z"),
            "source_bronze_batch": str(bronze_batch),
            "records_read": records_read,
            "records_accepted": records_accepted,
            "records_rejected": records_rejected,
            "duplicate_counts": duplicate_counts,
            "null_counts": null_counts,
            "reason_counts": reason_counts,
            "processing_duration_seconds": duration,
            "reconciliation_passed": all(
                records_read[name] == records_accepted[name] + records_rejected[name]
                for name in records_read
            ),
            "late_event_definition": "received_at more than 86400 seconds after event_ts",
        }
        quality_staging = staging / "quality"
        quality_staging.mkdir(parents=True, exist_ok=True)
        report_file = quality_staging / "report.json"
        report_file.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

        for path in (silver_path.parent, quarantine_path.parent, quality_path.parent):
            path.mkdir(parents=True, exist_ok=True)
        (staging / "silver").rename(silver_path)
        (staging / "quarantine").rename(quarantine_path)
        (staging / "quality").rename(quality_path)
        staging.rmdir()
        staging_parent = lake / "_staging"
        if staging_parent.exists() and not any(staging_parent.iterdir()):
            staging_parent.rmdir()

        commit_path.parent.mkdir(parents=True, exist_ok=True)
        commit_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return SilverProcessingResult(
            batch_id=batch_id,
            silver_path=silver_path,
            quarantine_path=quarantine_path,
            quality_report_path=quality_path / "report.json",
            records_read=records_read,
            records_accepted=records_accepted,
            records_rejected=records_rejected,
            duplicate_counts=duplicate_counts,
            null_counts=null_counts,
            reason_counts=reason_counts,
            processing_duration_seconds=duration,
        )
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise
