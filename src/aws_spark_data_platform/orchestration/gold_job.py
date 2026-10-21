"""Command-line entry point for Phase 4 Gold analytics."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from aws_spark_data_platform.config.spark import create_spark_session
from aws_spark_data_platform.gold import GoldBatchAlreadyProcessedError, process_gold_batch


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake-root", type=Path, required=True)
    parser.add_argument("--batch-id", required=True)
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args()
    spark = create_spark_session("phase4-gold-analytics")
    spark.sparkContext.setLogLevel("WARN")
    try:
        try:
            result = process_gold_batch(spark, args.lake_root, args.batch_id)
        except GoldBatchAlreadyProcessedError as error:
            raise SystemExit(f"Gold processing refused: {error}") from error
        print(
            json.dumps(
                {
                    "status": "success",
                    "batch_id": result.batch_id,
                    "gold_path": str(result.gold_path),
                    "report_path": str(result.report_path),
                    "row_counts": result.row_counts,
                    "qualified_order_count": result.qualified_order_count,
                    "excluded_order_count": result.excluded_order_count,
                    "net_revenue": str(result.net_revenue),
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
