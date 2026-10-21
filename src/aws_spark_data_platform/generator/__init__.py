"""Deterministic, non-sensitive synthetic source data."""

from aws_spark_data_platform.generator.models import GenerationConfig, GenerationResult
from aws_spark_data_platform.generator.synthetic import generate_dataset

__all__ = ["GenerationConfig", "GenerationResult", "generate_dataset"]
