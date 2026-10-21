"""Command-line entry point for recoverable incremental pipeline execution."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from aws_spark_data_platform.config.spark import create_spark_session
from aws_spark_data_platform.orchestration.incremental import run_incremental_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--lake-root", type=Path, required=True)
    parser.add_argument("--batch-id", required=True)
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args()
    spark = create_spark_session("phase6-incremental-pipeline")
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = run_incremental_pipeline(
            spark,
            args.source_root,
            args.lake_root,
            args.batch_id,
        )
        print(
            json.dumps(
                {
                    "status": result.status,
                    "batch_id": result.batch_id,
                    "resumed": result.resumed,
                    "state_path": str(result.state_path),
                    "stages": result.stages,
                    "event_watermark": result.event_watermark,
                },
                indent=2,
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
