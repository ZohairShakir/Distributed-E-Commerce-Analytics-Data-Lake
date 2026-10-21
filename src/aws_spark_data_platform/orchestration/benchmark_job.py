"""Command-line entry point for measured Phase 5 Spark benchmarks."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from aws_spark_data_platform.benchmarks import run_benchmarks
from aws_spark_data_platform.config.spark import create_spark_session


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake-root", type=Path, required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--run-id")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args()
    spark = create_spark_session("phase5-optimization-benchmarks")
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = run_benchmarks(
            spark,
            args.lake_root,
            args.batch_id,
            repetitions=args.repetitions,
            run_id=args.run_id,
        )
        print(
            json.dumps(
                {
                    "status": "success",
                    "run_id": result.run_id,
                    "report_path": str(result.report_path),
                    "json_path": str(result.json_path),
                    "experiments": result.measurements,
                },
                indent=2,
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
