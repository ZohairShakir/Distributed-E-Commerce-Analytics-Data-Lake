"""Phase 1 Spark smoke job: read explicit-schema JSON and write Parquet counts."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from aws_spark_data_platform.config.spark import create_spark_session
from aws_spark_data_platform.ingestion.schemas import BEHAVIORAL_EVENTS_SCHEMA

LOGGER = logging.getLogger(__name__)


def run_smoke_job(
    spark: SparkSession,
    input_root: str | Path,
    output_root: str | Path,
) -> dict[str, int]:
    """Aggregate event types with Spark and persist the result as Snappy Parquet."""
    input_path = Path(input_root).resolve()
    event_path = input_path / "behavioral_events.jsonl"
    manifest_path = input_path / "manifest.json"
    if not event_path.is_file() or not manifest_path.is_file():
        raise FileNotFoundError(
            f"Expected behavioral_events.jsonl and manifest.json under {input_path}. "
            "Run `make generate-small` first."
        )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected_count = int(manifest["record_counts"]["events"])

    events = spark.read.schema(BEHAVIORAL_EVENTS_SCHEMA).json(str(event_path))
    actual_count = events.count()
    if actual_count != expected_count:
        raise ValueError(f"Manifest expects {expected_count} events, but Spark read {actual_count}")

    event_counts = (
        events.groupBy("event_type")
        .agg(F.count(F.lit(1)).alias("event_count"))
        .orderBy("event_type")
    )
    output_path = Path(output_root).resolve() / "event_counts"
    event_counts.write.mode("overwrite").option("compression", "snappy").parquet(str(output_path))

    counts = {row["event_type"]: row["event_count"] for row in event_counts.collect()}
    if sum(counts.values()) != expected_count:
        raise ValueError("Aggregated event counts do not reconcile to the input count")
    LOGGER.info("Processed %s events into %s", actual_count, output_path)
    return counts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args()
    spark = create_spark_session("phase1-synthetic-event-smoke-job")
    spark.sparkContext.setLogLevel("WARN")
    try:
        counts = run_smoke_job(spark, args.input_root, args.output_root)
        print(json.dumps({"status": "success", "event_counts": counts}, indent=2, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
