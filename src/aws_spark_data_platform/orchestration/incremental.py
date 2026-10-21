"""Recoverable local incremental pipeline orchestration and batch ledger."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from aws_spark_data_platform.bronze import ingest_bronze_batch
from aws_spark_data_platform.gold import process_gold_batch
from aws_spark_data_platform.silver import process_silver_batch

StageName = Literal["bronze", "silver", "gold"]


class PipelineBatchConflictError(RuntimeError):
    """Raised when a batch ID is reused for a different source path."""


class PipelineLockError(RuntimeError):
    """Raised when the same batch is already being processed."""


class SimulatedPipelineFailure(RuntimeError):
    """Test-only failure raised after a requested committed stage."""


@dataclass(frozen=True)
class IncrementalPipelineResult:
    batch_id: str
    status: str
    resumed: bool
    state_path: Path
    stages: dict[str, str]
    event_watermark: dict[str, Any]


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _committed_stages(lake: Path, batch_id: str) -> dict[str, str]:
    markers = {
        "bronze": lake / "bronze" / f"batch_id={batch_id}" / "_metadata/ingestion.json",
        "silver": lake / "metadata" / "silver_batches" / f"batch_id={batch_id}.json",
        "gold": lake / "gold" / f"batch_id={batch_id}" / "_metadata/report.json",
    }
    return {
        stage: "committed" if marker.is_file() else "pending" for stage, marker in markers.items()
    }


def _archive_partial_outputs(lake: Path, batch_id: str, stages: dict[str, str]) -> list[str]:
    candidates: list[tuple[str, Path]] = []
    if stages["silver"] != "committed":
        candidates.extend(
            [
                ("silver", lake / "silver" / f"batch_id={batch_id}"),
                ("quarantine", lake / "quarantine" / f"batch_id={batch_id}"),
                ("quality", lake / "quality" / f"batch_id={batch_id}"),
            ]
        )
    if stages["gold"] != "committed":
        candidates.append(("gold", lake / "gold" / f"batch_id={batch_id}"))
    existing = [(name, path) for name, path in candidates if path.exists()]
    if not existing:
        return []

    recovery_root = (
        lake / "_recovery" / f"batch_id={batch_id}" / _utc_now().strftime("%Y%m%dT%H%M%S%fZ")
    )
    recovery_root.mkdir(parents=True)
    archived: list[str] = []
    for name, path in existing:
        destination = recovery_root / name
        path.rename(destination)
        archived.append(str(destination))
    return archived


def _event_watermark(
    spark: SparkSession,
    lake: Path,
    batch_id: str,
    previous: dict[str, Any],
) -> dict[str, Any]:
    events_path = lake / "silver" / f"batch_id={batch_id}" / "tables/behavioral_events"
    events = spark.read.parquet(str(events_path))
    summary = events.agg(
        F.max("event_ts").alias("max_event_ts"),
        F.max("received_at").alias("max_received_at"),
        F.sum(F.col("is_late").cast("long")).alias("late_event_count"),
    ).first()
    previous_max = previous.get("max_event_ts")
    at_or_before_previous = 0
    if previous_max:
        at_or_before_previous = events.filter(
            F.col("event_ts") <= F.to_timestamp(F.lit(previous_max))
        ).count()
    return {
        "max_event_ts": summary["max_event_ts"]
        .replace(tzinfo=UTC)
        .isoformat()
        .replace("+00:00", "Z"),
        "max_received_at": summary["max_received_at"]
        .replace(tzinfo=UTC)
        .isoformat()
        .replace("+00:00", "Z"),
        "late_event_count": int(summary["late_event_count"] or 0),
        "events_at_or_before_previous_watermark": at_or_before_previous,
        "previous_max_event_ts": previous_max,
    }


def _update_checkpoint(
    checkpoint_path: Path,
    batch_id: str,
    watermark: dict[str, Any],
) -> None:
    checkpoint = _read_json(checkpoint_path) if checkpoint_path.is_file() else {}
    processed = list(checkpoint.get("processed_batches", []))
    if batch_id not in processed:
        processed.append(batch_id)
    prior_max_event = checkpoint.get("max_event_ts")
    prior_max_received = checkpoint.get("max_received_at")
    _atomic_write_json(
        checkpoint_path,
        {
            "updated_at_utc": _iso(_utc_now()),
            "processed_batches": processed,
            "max_event_ts": max(
                value for value in (prior_max_event, watermark["max_event_ts"]) if value
            ),
            "max_received_at": max(
                value for value in (prior_max_received, watermark["max_received_at"]) if value
            ),
        },
    )


def _maybe_fail(stage: StageName, fail_after_stage: StageName | None) -> None:
    if fail_after_stage == stage:
        raise SimulatedPipelineFailure(f"Simulated failure after committed {stage} stage")


def run_incremental_pipeline(
    spark: SparkSession,
    source_root: str | Path,
    lake_root: str | Path,
    batch_id: str,
    *,
    fail_after_stage: StageName | None = None,
) -> IncrementalPipelineResult:
    """Process or resume one batch and atomically maintain ledger/checkpoint metadata."""
    source = Path(source_root).resolve()
    lake = Path(lake_root).resolve()
    state_path = lake / "metadata" / "pipeline_batches" / f"batch_id={batch_id}.json"
    lock_path = lake / "metadata" / "locks" / f"batch_id={batch_id}.lock"
    checkpoint_path = lake / "metadata" / "checkpoints" / "behavioral_events.json"
    prior_state = _read_json(state_path) if state_path.is_file() else {}
    if prior_state and prior_state.get("source_root") != str(source):
        raise PipelineBatchConflictError(
            f"Batch {batch_id} is already registered to {prior_state.get('source_root')}"
        )

    stages = _committed_stages(lake, batch_id)
    initially_committed = any(status == "committed" for status in stages.values())
    if prior_state.get("status") == "completed" and all(
        status == "committed" for status in stages.values()
    ):
        return IncrementalPipelineResult(
            batch_id=batch_id,
            status="completed",
            resumed=True,
            state_path=state_path,
            stages=stages,
            event_watermark=prior_state.get("event_watermark", {}),
        )

    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as error:
        raise PipelineLockError(f"Batch is already locked: {batch_id}") from error
    os.write(descriptor, f"pid={os.getpid()}\n".encode())
    os.close(descriptor)

    attempt = int(prior_state.get("attempt", 0)) + 1
    state: dict[str, Any] = {
        **prior_state,
        "batch_id": batch_id,
        "source_root": str(source),
        "status": "running",
        "attempt": attempt,
        "started_at_utc": prior_state.get("started_at_utc", _iso(_utc_now())),
        "updated_at_utc": _iso(_utc_now()),
        "stages": stages,
    }
    try:
        archived = _archive_partial_outputs(lake, batch_id, stages)
        if archived:
            state.setdefault("recovered_outputs", []).extend(archived)
            _atomic_write_json(state_path, state)

        if stages["bronze"] != "committed":
            bronze = ingest_bronze_batch(spark, source, lake, batch_id)
            state["source_fingerprint"] = bronze.source_fingerprint
            stages["bronze"] = "committed"
            state["stages"] = stages
            state["updated_at_utc"] = _iso(_utc_now())
            _atomic_write_json(state_path, state)
        else:
            bronze_metadata = _read_json(
                lake / "bronze" / f"batch_id={batch_id}" / "_metadata/ingestion.json"
            )
            state["source_fingerprint"] = bronze_metadata["source_fingerprint"]
        _maybe_fail("bronze", fail_after_stage)

        if stages["silver"] != "committed":
            process_silver_batch(spark, lake, batch_id)
            stages["silver"] = "committed"
            state["stages"] = stages
            state["updated_at_utc"] = _iso(_utc_now())
            _atomic_write_json(state_path, state)
        _maybe_fail("silver", fail_after_stage)

        previous_checkpoint = _read_json(checkpoint_path) if checkpoint_path.is_file() else {}
        watermark = _event_watermark(spark, lake, batch_id, previous_checkpoint)
        state["event_watermark"] = watermark

        if stages["gold"] != "committed":
            process_gold_batch(spark, lake, batch_id)
            stages["gold"] = "committed"
            state["stages"] = stages
            state["updated_at_utc"] = _iso(_utc_now())
            _atomic_write_json(state_path, state)
        _maybe_fail("gold", fail_after_stage)

        _update_checkpoint(checkpoint_path, batch_id, watermark)
        state.update(
            {
                "status": "completed",
                "completed_at_utc": _iso(_utc_now()),
                "updated_at_utc": _iso(_utc_now()),
                "stages": stages,
            }
        )
        state.pop("last_error", None)
        _atomic_write_json(state_path, state)
        return IncrementalPipelineResult(
            batch_id=batch_id,
            status="completed",
            resumed=attempt > 1 or initially_committed,
            state_path=state_path,
            stages=stages,
            event_watermark=watermark,
        )
    except Exception as error:
        state.update(
            {
                "status": "failed",
                "updated_at_utc": _iso(_utc_now()),
                "stages": stages,
                "last_error": {"type": type(error).__name__, "message": str(error)},
            }
        )
        _atomic_write_json(state_path, state)
        raise
    finally:
        lock_path.unlink(missing_ok=True)
