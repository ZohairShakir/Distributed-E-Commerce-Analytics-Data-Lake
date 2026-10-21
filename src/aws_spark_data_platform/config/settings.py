"""Typed settings shared by local commands."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class SparkSettings:
    master: str = "local[2]"
    driver_memory: str = "1g"
    shuffle_partitions: int = 4
    timezone: str = "UTC"

    @classmethod
    def from_environment(cls) -> SparkSettings:
        return cls(
            master=os.getenv("SPARK_MASTER", "local[2]"),
            driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "1g"),
            shuffle_partitions=int(os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4")),
        )
