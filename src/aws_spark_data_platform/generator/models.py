"""Configuration and result objects for synthetic data generation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

SIZE_RECORDS = {
    "small": 10_000,
    "medium": 100_000,
    "large": 1_000_000,
    "stress": 5_000_000,
}


@dataclass(frozen=True)
class GenerationConfig:
    event_count: int = SIZE_RECORDS["small"]
    invalid_rate: float = 0.01
    late_arrival_rate: float = 0.05
    seed: int = 20_261_021
    batch_id: str = "batch-0001"

    def __post_init__(self) -> None:
        if self.event_count < 1:
            raise ValueError("event_count must be at least 1")
        if not 0.0 <= self.invalid_rate <= 0.25:
            raise ValueError("invalid_rate must be between 0 and 0.25")
        if not 0.0 <= self.late_arrival_rate <= 1.0:
            raise ValueError("late_arrival_rate must be between 0 and 1")
        if not self.batch_id.strip():
            raise ValueError("batch_id must not be blank")


@dataclass(frozen=True)
class GenerationResult:
    output_root: Path
    record_counts: dict[str, int]
    invalid_counts: dict[str, int]
    late_event_count: int
    manifest_path: Path
