"""Measured Small-profile Spark optimization experiments."""

from __future__ import annotations

import json
import os
import platform
import shutil
import statistics
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from aws_spark_data_platform.ingestion.schemas import ORDERS_SCHEMA


@dataclass(frozen=True)
class BenchmarkResult:
    run_id: str
    report_path: Path
    json_path: Path
    measurements: dict[str, dict[str, Any]]


def _directory_stats(path: Path) -> dict[str, int]:
    files = [candidate for candidate in path.rglob("*") if candidate.is_file()]
    data_files = [candidate for candidate in files if not candidate.name.startswith((".", "_"))]
    return {
        "size_bytes": sum(candidate.stat().st_size for candidate in data_files),
        "file_count": len(data_files),
    }


def _timed_runs(
    spark: SparkSession,
    action: Callable[[], int],
    *,
    repetitions: int,
) -> tuple[list[float], int]:
    durations: list[float] = []
    expected_rows: int | None = None
    spark.catalog.clearCache()
    for _ in range(repetitions):
        started = time.perf_counter()
        rows = action()
        durations.append(round(time.perf_counter() - started, 6))
        if expected_rows is None:
            expected_rows = rows
        elif rows != expected_rows:
            raise RuntimeError(
                f"Benchmark row count changed between runs: {expected_rows} != {rows}"
            )
    return durations, int(expected_rows or 0)


def _warm_median(durations: list[float]) -> float:
    candidates = durations[1:] if len(durations) > 1 else durations
    return round(statistics.median(candidates), 6)


def _scan_action(frame_factory: Callable[[], DataFrame]) -> int:
    rows = frame_factory().groupBy("region").count().collect()
    return sum(row["count"] for row in rows)


def _join_action(frame_factory: Callable[[], DataFrame]) -> int:
    row = (
        frame_factory()
        .agg(
            F.count(F.lit(1)).alias("row_count"),
            F.sum(F.col("quantity") * F.col("unit_price")).alias("gross_value"),
        )
        .first()
    )
    if row["gross_value"] is None:
        raise RuntimeError("Join benchmark unexpectedly produced no financial value")
    return int(row["row_count"])


def _filter_action(frame_factory: Callable[[], DataFrame]) -> int:
    rows = frame_factory().groupBy("event_type").count().collect()
    return sum(row["count"] for row in rows)


def _plan(frame: DataFrame) -> str:
    return frame._jdf.queryExecution().executedPlan().toString()  # noqa: SLF001


def _measurement(durations: list[float], row_count: int, storage: dict[str, int]) -> dict[str, Any]:
    return {
        "durations_seconds": durations,
        "cold_seconds": durations[0],
        "warm_median_seconds": _warm_median(durations),
        "row_count": row_count,
        **storage,
    }


def _ratio(baseline: dict[str, Any], candidate: dict[str, Any]) -> float:
    return round(baseline["warm_median_seconds"] / candidate["warm_median_seconds"], 3)


def _memory_limit_bytes() -> int | None:
    limit_path = Path("/sys/fs/cgroup/memory.max")
    if not limit_path.is_file():
        return None
    raw = limit_path.read_text(encoding="utf-8").strip()
    return None if raw == "max" else int(raw)


def _markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Measured Spark optimization report",
        "",
        f"Run ID: `{report['run_id']}`  ",
        f"Recorded UTC: `{report['recorded_at_utc']}`  ",
        f"Input profile: `{report['input_profile']}`",
        "",
        "> These are local-mode observations, not multi-node scalability claims. The first run is",
        "> labeled cold and the remaining runs use their median. OS caching can still affect both",
        "> categories.",
        "",
        "## Results",
        "",
        "| Experiment | Variant | Rows | Cold s | Warm median s | Bytes | Files |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for experiment, variants in report["experiments"].items():
        for variant, values in variants.items():
            if variant == "observed_ratio":
                continue
            lines.append(
                f"| {experiment} | {variant} | {values['row_count']} | "
                f"{values['cold_seconds']:.6f} | {values['warm_median_seconds']:.6f} | "
                f"{values['size_bytes']} | {values['file_count']} |"
            )
        lines.append(
            f"| {experiment} | observed baseline/candidate warm ratio | — | — | "
            f"{variants['observed_ratio']:.3f}× | — | — |"
        )
    lines.extend(
        [
            "",
            "## Physical-plan observations",
            "",
            f"- CSV plan contained `FileScan csv`: {report['plans']['csv_file_scan']}.",
            f"- Parquet plan contained `FileScan parquet`: {report['plans']['parquet_file_scan']}.",
            "- Forced regular join contained `SortMergeJoin`: "
            f"{report['plans']['regular_sort_merge']}.",
            "- Hinted join contained `BroadcastHashJoin`: "
            f"{report['plans']['broadcast_hash_join']}.",
            "- Partitioned filter exposed partition filters: "
            f"{report['plans']['partition_filter']}.",
            "",
            "## Environment",
            "",
            "```json",
            json.dumps(report["environment"], indent=2, sort_keys=True),
            "```",
            "",
            "## Interpretation guardrails",
            "",
            "An observed ratio above 1 means the candidate was faster in these runs; below 1 means",
            "it was slower. Small inputs can make startup, scheduling, and file metadata dominate.",
            "Partitioning can regress when filters do not prune partitions or when it creates many",
            "tiny files. Broadcasting is safe here only because the product table is deliberately",
            "small. Repeated larger-profile testing requires explicit resource approval.",
            "",
        ]
    )
    return "\n".join(lines)


def run_benchmarks(
    spark: SparkSession,
    lake_root: str | Path,
    batch_id: str,
    *,
    repetitions: int = 3,
    run_id: str | None = None,
) -> BenchmarkResult:
    """Run three paired Spark experiments and generate JSON and Markdown reports."""
    if repetitions < 3:
        raise ValueError("Benchmark repetitions must be at least 3")
    lake = Path(lake_root).resolve()
    selected_run_id = run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_root = lake / "benchmarks" / f"run_id={selected_run_id}"
    if report_root.exists():
        raise FileExistsError(f"Benchmark run already exists: {selected_run_id}")

    bronze = lake / "bronze" / f"batch_id={batch_id}"
    silver = lake / "silver" / f"batch_id={batch_id}" / "tables"
    orders_csv = bronze / "original" / "orders" / "orders.csv"
    orders_parquet = bronze / "tables" / "orders"
    items_parquet = silver / "order_items"
    products_parquet = silver / "products"
    events_parquet = silver / "behavioral_events"
    required = (orders_csv, orders_parquet, items_parquet, products_parquet, events_parquet)
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Benchmark inputs are missing: {', '.join(missing)}")

    fixture_root = lake / "benchmarks" / "_fixtures" / f"batch_id={batch_id}"
    if fixture_root.exists():
        shutil.rmtree(fixture_root)
    events = spark.read.parquet(str(events_parquet)).withColumn("event_date", F.to_date("event_ts"))
    unpartitioned_events = fixture_root / "events_unpartitioned"
    partitioned_events = fixture_root / "events_partitioned"
    events.write.mode("errorifexists").option("compression", "snappy").parquet(
        str(unpartitioned_events)
    )
    events.repartition("event_date").write.mode("errorifexists").option(
        "compression", "snappy"
    ).partitionBy("event_date").parquet(str(partitioned_events))
    target_date = (
        events.groupBy("event_date").count().orderBy(F.desc("count")).first()["event_date"]
    )

    def csv_factory() -> DataFrame:
        return spark.read.schema(ORDERS_SCHEMA).option("header", "true").csv(str(orders_csv))

    def parquet_factory() -> DataFrame:
        return spark.read.parquet(str(orders_parquet))

    csv_durations, csv_rows = _timed_runs(
        spark, lambda: _scan_action(csv_factory), repetitions=repetitions
    )
    parquet_durations, parquet_rows = _timed_runs(
        spark, lambda: _scan_action(parquet_factory), repetitions=repetitions
    )
    if csv_rows != parquet_rows:
        raise RuntimeError(f"CSV/Parquet scan rows differ: {csv_rows} != {parquet_rows}")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    def regular_factory() -> DataFrame:
        return spark.read.parquet(str(items_parquet)).join(
            spark.read.parquet(str(products_parquet)).select("product_id", "category"),
            on="product_id",
            how="inner",
        )

    def broadcast_factory() -> DataFrame:
        return spark.read.parquet(str(items_parquet)).join(
            F.broadcast(spark.read.parquet(str(products_parquet)).select("product_id", "category")),
            on="product_id",
            how="inner",
        )

    regular_durations, regular_rows = _timed_runs(
        spark, lambda: _join_action(regular_factory), repetitions=repetitions
    )
    broadcast_durations, broadcast_rows = _timed_runs(
        spark, lambda: _join_action(broadcast_factory), repetitions=repetitions
    )
    if regular_rows != broadcast_rows:
        raise RuntimeError(f"Join rows differ: {regular_rows} != {broadcast_rows}")

    def unpartitioned_factory() -> DataFrame:
        return spark.read.parquet(str(unpartitioned_events)).filter(
            F.col("event_date") == F.lit(target_date)
        )

    def partitioned_factory() -> DataFrame:
        return spark.read.parquet(str(partitioned_events)).filter(
            F.col("event_date") == F.lit(target_date)
        )

    unpartitioned_durations, unpartitioned_rows = _timed_runs(
        spark, lambda: _filter_action(unpartitioned_factory), repetitions=repetitions
    )
    partitioned_durations, partitioned_rows = _timed_runs(
        spark, lambda: _filter_action(partitioned_factory), repetitions=repetitions
    )
    if unpartitioned_rows != partitioned_rows:
        raise RuntimeError(
            f"Partition-filter rows differ: {unpartitioned_rows} != {partitioned_rows}"
        )

    scan = {
        "csv_baseline": _measurement(csv_durations, csv_rows, _directory_stats(orders_csv.parent)),
        "parquet_candidate": _measurement(
            parquet_durations, parquet_rows, _directory_stats(orders_parquet)
        ),
    }
    scan["observed_ratio"] = _ratio(scan["csv_baseline"], scan["parquet_candidate"])
    source_stats = {
        "size_bytes": _directory_stats(items_parquet)["size_bytes"]
        + _directory_stats(products_parquet)["size_bytes"],
        "file_count": _directory_stats(items_parquet)["file_count"]
        + _directory_stats(products_parquet)["file_count"],
    }
    joins = {
        "shuffle_baseline": _measurement(regular_durations, regular_rows, source_stats),
        "broadcast_candidate": _measurement(broadcast_durations, broadcast_rows, source_stats),
    }
    joins["observed_ratio"] = _ratio(joins["shuffle_baseline"], joins["broadcast_candidate"])
    partitions = {
        "unpartitioned_baseline": _measurement(
            unpartitioned_durations,
            unpartitioned_rows,
            _directory_stats(unpartitioned_events),
        ),
        "partitioned_candidate": _measurement(
            partitioned_durations,
            partitioned_rows,
            _directory_stats(partitioned_events),
        ),
    }
    partitions["observed_ratio"] = _ratio(
        partitions["unpartitioned_baseline"], partitions["partitioned_candidate"]
    )

    plans = {
        "csv_file_scan": "FileScan csv" in _plan(csv_factory()),
        "parquet_file_scan": "FileScan parquet" in _plan(parquet_factory()),
        "regular_sort_merge": "SortMergeJoin" in _plan(regular_factory()),
        "broadcast_hash_join": "BroadcastHashJoin" in _plan(broadcast_factory()),
        "partition_filter": "PartitionFilters" in _plan(partitioned_factory()),
    }
    report = {
        "run_id": selected_run_id,
        "recorded_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "input_profile": "Small synthetic batch",
        "batch_id": batch_id,
        "repetitions": repetitions,
        "target_event_date": str(target_date),
        "experiments": {
            "CSV vs Parquet scan": scan,
            "Shuffle vs broadcast join": joins,
            "Unpartitioned vs date-partitioned filter": partitions,
        },
        "plans": plans,
        "environment": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "logical_cpu_count": os.cpu_count(),
            "container_memory_limit_bytes": _memory_limit_bytes(),
            "spark_version": spark.version,
            "spark_master": spark.sparkContext.master,
            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            "adaptive_execution": spark.conf.get("spark.sql.adaptive.enabled"),
        },
    }
    report_root.mkdir(parents=True)
    json_path = report_root / "report.json"
    markdown_path = report_root / "report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_markdown(report), encoding="utf-8")
    return BenchmarkResult(selected_run_id, markdown_path, json_path, report["experiments"])
