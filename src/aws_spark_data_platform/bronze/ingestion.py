"""Atomic, traceable ingestion of source deliveries into the Bronze layer."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from aws_spark_data_platform.ingestion.schemas import (
    BEHAVIORAL_EVENTS_SCHEMA,
    CUSTOMERS_SCHEMA,
    ORDER_ITEMS_SCHEMA,
    ORDERS_SCHEMA,
    PAYMENT_EVENTS_SCHEMA,
    PRODUCTS_SCHEMA,
)

BATCH_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
CORRUPT_COLUMN = "_corrupt_record"


class BatchAlreadyProcessedError(RuntimeError):
    """Raised when a committed batch identifier is reused."""


class SourceAlreadyProcessedError(RuntimeError):
    """Raised when an identical set of source files was previously committed."""


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    pattern: str
    source_format: str
    schema: StructType


@dataclass(frozen=True)
class BronzeIngestionResult:
    batch_id: str
    batch_path: Path
    source_fingerprint: str
    records_read: dict[str, int]
    malformed_records: dict[str, int]
    source_file_count: int
    metadata_path: Path


DATASET_SPECS = (
    DatasetSpec("customers", "customers*.csv", "csv", CUSTOMERS_SCHEMA),
    DatasetSpec("products", "products*.csv", "csv", PRODUCTS_SCHEMA),
    DatasetSpec("orders", "orders*.csv", "csv", ORDERS_SCHEMA),
    DatasetSpec("order_items", "order_items*.csv", "csv", ORDER_ITEMS_SCHEMA),
    DatasetSpec("payments", "payment_events*.jsonl", "json", PAYMENT_EVENTS_SCHEMA),
    DatasetSpec(
        "behavioral_events",
        "behavioral_events*.jsonl",
        "json",
        BEHAVIORAL_EVENTS_SCHEMA,
    ),
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _discover_source_files(source_root: Path) -> dict[str, list[Path]]:
    discovered: dict[str, list[Path]] = {}
    for spec in DATASET_SPECS:
        files = sorted(path for path in source_root.glob(spec.pattern) if path.is_file())
        if not files:
            raise FileNotFoundError(
                f"No source files matched {spec.pattern!r} for dataset {spec.name!r} "
                f"under {source_root}"
            )
        discovered[spec.name] = files
    return discovered


def _inventory(
    source_root: Path,
    files_by_dataset: dict[str, list[Path]],
) -> list[dict[str, str | int]]:
    entries: list[dict[str, str | int]] = []
    for dataset_name in sorted(files_by_dataset):
        for path in files_by_dataset[dataset_name]:
            entries.append(
                {
                    "dataset": dataset_name,
                    "relative_path": path.relative_to(source_root).as_posix(),
                    "sha256": _sha256(path),
                    "size_bytes": path.stat().st_size,
                }
            )
    return entries


def _source_fingerprint(inventory: list[dict[str, str | int]]) -> str:
    content_identity = sorted(
        (
            {
                "dataset": entry["dataset"],
                "sha256": entry["sha256"],
                "size_bytes": entry["size_bytes"],
            }
            for entry in inventory
        ),
        key=lambda entry: (entry["dataset"], entry["sha256"], entry["size_bytes"]),
    )
    serialized = json.dumps(content_identity, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(serialized).hexdigest()


def _is_synthetic_source(source_root: Path) -> bool:
    manifest_path = source_root / "manifest.json"
    if not manifest_path.is_file():
        return False
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as error:
        raise RuntimeError(f"Unreadable source manifest: {manifest_path}") from error
    return manifest.get("synthetic_data") is True


def _read_committed_metadata(bronze_root: Path) -> list[dict[str, object]]:
    committed: list[dict[str, object]] = []
    for metadata_path in sorted(bronze_root.glob("batch_id=*/_metadata/ingestion.json")):
        try:
            committed.append(json.loads(metadata_path.read_text(encoding="utf-8")))
        except (json.JSONDecodeError, OSError) as error:
            raise RuntimeError(f"Unreadable committed Bronze metadata: {metadata_path}") from error
    return committed


def _assert_not_processed(
    bronze_root: Path,
    batch_id: str,
    source_fingerprint: str,
) -> None:
    final_batch_path = bronze_root / f"batch_id={batch_id}"
    if final_batch_path.exists():
        raise BatchAlreadyProcessedError(f"Bronze batch already exists: {batch_id}")

    for metadata in _read_committed_metadata(bronze_root):
        if metadata.get("batch_id") == batch_id:
            raise BatchAlreadyProcessedError(f"Bronze batch already committed: {batch_id}")
        if metadata.get("source_fingerprint") == source_fingerprint:
            previous_batch = metadata.get("batch_id", "unknown")
            raise SourceAlreadyProcessedError(
                f"This exact source delivery was already committed as batch {previous_batch}"
            )


def _schema_with_corrupt_record(schema: StructType) -> StructType:
    return StructType([*schema.fields, StructField(CORRUPT_COLUMN, StringType(), True)])


def _read_source(
    spark: SparkSession,
    spec: DatasetSpec,
    paths: list[Path],
) -> DataFrame:
    reader = (
        spark.read.schema(_schema_with_corrupt_record(spec.schema))
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
    )
    path_strings = [str(path) for path in paths]
    if spec.source_format == "csv":
        return reader.option("header", "true").option("multiLine", "false").csv(path_strings)
    return reader.json(path_strings)


def _add_ingestion_metadata(
    frame: DataFrame,
    batch_id: str,
    ingested_at: datetime,
    source_columns: list[str],
) -> DataFrame:
    record_json = F.to_json(F.struct(*[F.col(name) for name in source_columns]))
    return (
        frame.withColumn("_source_file", F.input_file_name())
        .withColumn("_ingestion_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.lit(ingested_at.replace(tzinfo=None)).cast("timestamp"))
        .withColumn("_is_malformed", F.col(CORRUPT_COLUMN).isNotNull())
        .withColumn("_source_record_hash", F.sha2(F.coalesce(record_json, F.lit("")), 256))
    )


def ingest_bronze_batch(
    spark: SparkSession,
    source_root: str | Path,
    lake_root: str | Path,
    batch_id: str,
    *,
    ingested_at: datetime | None = None,
) -> BronzeIngestionResult:
    """Ingest one source delivery and publish it as an immutable Bronze batch."""
    if not BATCH_ID_PATTERN.fullmatch(batch_id):
        raise ValueError(
            "batch_id must start with an alphanumeric character and contain only "
            "letters, numbers, dots, underscores, or hyphens"
        )

    source = Path(source_root).resolve()
    lake = Path(lake_root).resolve()
    if not source.is_dir():
        raise FileNotFoundError(f"Source root does not exist: {source}")
    if lake == source or source in lake.parents:
        raise ValueError("lake_root must not be the source directory or one of its children")

    files_by_dataset = _discover_source_files(source)
    file_inventory = _inventory(source, files_by_dataset)
    fingerprint = _source_fingerprint(file_inventory)
    bronze_root = lake / "bronze"
    final_batch_path = bronze_root / f"batch_id={batch_id}"
    _assert_not_processed(bronze_root, batch_id, fingerprint)

    timestamp = (ingested_at or datetime.now(UTC)).astimezone(UTC)
    staging_root = lake / "_staging" / f"bronze-{batch_id}-{uuid.uuid4().hex}"
    records_read: dict[str, int] = {}
    malformed_records: dict[str, int] = {}

    try:
        for spec in DATASET_SPECS:
            paths = files_by_dataset[spec.name]
            frame = _read_source(spark, spec, paths)
            source_columns = [field.name for field in spec.schema.fields] + [CORRUPT_COLUMN]
            enriched = _add_ingestion_metadata(frame, batch_id, timestamp, source_columns)
            enriched.persist(StorageLevel.MEMORY_AND_DISK)
            try:
                records_read[spec.name] = enriched.count()
                malformed_records[spec.name] = enriched.filter(F.col("_is_malformed")).count()
                enriched.write.mode("errorifexists").option("compression", "snappy").parquet(
                    str(staging_root / "tables" / spec.name)
                )
            finally:
                enriched.unpersist()

            original_dataset_root = staging_root / "original" / spec.name
            original_dataset_root.mkdir(parents=True, exist_ok=True)
            for path in paths:
                shutil.copy2(path, original_dataset_root / path.name)

        metadata = {
            "status": "committed",
            "synthetic_source": _is_synthetic_source(source),
            "batch_id": batch_id,
            "ingested_at_utc": timestamp.isoformat().replace("+00:00", "Z"),
            "source_root": str(source),
            "source_fingerprint": fingerprint,
            "source_file_count": len(file_inventory),
            "source_files": file_inventory,
            "records_read": records_read,
            "malformed_records": malformed_records,
            "storage_format": "parquet",
            "compression": "snappy",
            "schema_policy": "explicit-string-preserving",
        }
        metadata_root = staging_root / "_metadata"
        metadata_root.mkdir(parents=True, exist_ok=True)
        (metadata_root / "ingestion.json").write_text(
            json.dumps(metadata, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        bronze_root.mkdir(parents=True, exist_ok=True)
        staging_root.rename(final_batch_path)
        staging_parent = lake / "_staging"
        if staging_parent.exists() and not any(staging_parent.iterdir()):
            staging_parent.rmdir()

        return BronzeIngestionResult(
            batch_id=batch_id,
            batch_path=final_batch_path,
            source_fingerprint=fingerprint,
            records_read=records_read,
            malformed_records=malformed_records,
            source_file_count=len(file_inventory),
            metadata_path=final_batch_path / "_metadata" / "ingestion.json",
        )
    except Exception:
        if staging_root.exists():
            shutil.rmtree(staging_root)
        raise
