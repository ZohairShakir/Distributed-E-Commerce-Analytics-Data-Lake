"""Command-line interface for deterministic synthetic data generation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from aws_spark_data_platform.generator.models import SIZE_RECORDS, GenerationConfig
from aws_spark_data_platform.generator.synthetic import generate_dataset


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size", choices=SIZE_RECORDS, default="small")
    parser.add_argument(
        "--records", type=int, help="Override the event count for tests or experiments"
    )
    parser.add_argument("--output", type=Path, default=Path("data/raw/synthetic/small"))
    parser.add_argument("--seed", type=int, default=20_261_021)
    parser.add_argument("--invalid-rate", type=float, default=0.01)
    parser.add_argument("--late-arrival-rate", type=float, default=0.05)
    parser.add_argument("--batch-id", default="batch-0001")
    parser.add_argument(
        "--force", action="store_true", help="Replace only the selected output directory"
    )
    parser.add_argument(
        "--allow-large",
        action="store_true",
        help="Required for more than 10,000 events to prevent accidental resource use",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    event_count = args.records if args.records is not None else SIZE_RECORDS[args.size]
    if event_count > SIZE_RECORDS["small"] and not args.allow_large:
        raise SystemExit(
            "Refusing to generate more than 10,000 events without --allow-large. "
            "Review local CPU, memory, and disk capacity first."
        )
    result = generate_dataset(
        args.output,
        GenerationConfig(
            event_count=event_count,
            invalid_rate=args.invalid_rate,
            late_arrival_rate=args.late_arrival_rate,
            seed=args.seed,
            batch_id=args.batch_id,
        ),
        force=args.force,
    )
    print(
        json.dumps(
            {
                "synthetic_data": True,
                "output_root": str(result.output_root),
                "record_counts": result.record_counts,
                "invalid_counts": result.invalid_counts,
                "late_event_count": result.late_event_count,
                "manifest": str(result.manifest_path),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
