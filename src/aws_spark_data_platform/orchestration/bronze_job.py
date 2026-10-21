"""Command-line entry point for Phase 2 Bronze ingestion."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from aws_spark_data_platform.bronze import (
    BatchAlreadyProcessedError,
    SourceAlreadyProcessedError,
    ingest_bronze_batch,
)
from aws_spark_data_platform.config.spark import create_spark_session


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--lake-root", type=Path, required=True)
    parser.add_argument("--batch-id", required=True)
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args()
    spark = create_spark_session("phase2-bronze-ingestion")
    spark.sparkContext.setLogLevel("WARN")
    try:
        try:
            result = ingest_bronze_batch(
                spark,
                source_root=args.source_root,
                lake_root=args.lake_root,
                batch_id=args.batch_id,
            )
        except (BatchAlreadyProcessedError, SourceAlreadyProcessedError) as error:
            raise SystemExit(f"Bronze ingestion refused: {error}") from error
        print(
            json.dumps(
                {
                    "status": "success",
                    "batch_id": result.batch_id,
                    "batch_path": str(result.batch_path),
                    "source_fingerprint": result.source_fingerprint,
                    "source_file_count": result.source_file_count,
                    "records_read": result.records_read,
                    "malformed_records": result.malformed_records,
                    "metadata_path": str(result.metadata_path),
                },
                indent=2,
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
