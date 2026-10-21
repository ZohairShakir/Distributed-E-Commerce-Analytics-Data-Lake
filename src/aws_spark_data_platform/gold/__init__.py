"""Spark SQL Gold analytics datasets."""

from aws_spark_data_platform.gold.analytics import (
    GoldBatchAlreadyProcessedError,
    GoldProcessingResult,
    process_gold_batch,
)

__all__ = [
    "GoldBatchAlreadyProcessedError",
    "GoldProcessingResult",
    "process_gold_batch",
]
