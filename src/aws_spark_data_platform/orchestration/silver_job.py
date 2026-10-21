"""Command-line entry point for Phase 3 Silver processing."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from aws_spark_data_platform.config.spark import create_spark_session
from aws_spark_data_platform.silver import SilverBatchAlreadyProcessedError, process_silver_batch


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake-root", type=Path, required=True)
    parser.add_argument("--batch-id", required=True)
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args()
    spark = create_spark_session("phase3-silver-processing")
    spark.sparkContext.setLogLevel("WARN")
    try:
        try:
            result = process_silver_batch(spark, args.lake_root, args.batch_id)
        except SilverBatchAlreadyProcessedError as error:
            raise SystemExit(f"Silver processing refused: {error}") from error
        print(
            json.dumps(
                {
                    "status": "success",
                    "batch_id": result.batch_id,
                    "silver_path": str(result.silver_path),
                    "quarantine_path": str(result.quarantine_path),
                    "quality_report_path": str(result.quality_report_path),
                    "records_read": result.records_read,
                    "records_accepted": result.records_accepted,
                    "records_rejected": result.records_rejected,
                    "duplicate_counts": result.duplicate_counts,
                    "null_counts": result.null_counts,
                    "reason_counts": result.reason_counts,
                    "processing_duration_seconds": result.processing_duration_seconds,
                },
                indent=2,
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
