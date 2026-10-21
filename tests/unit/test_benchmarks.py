from __future__ import annotations

from aws_spark_data_platform.benchmarks.harness import _warm_median


def test_warm_median_excludes_first_cold_observation() -> None:
    assert _warm_median([9.0, 2.0, 4.0]) == 3.0


def test_warm_median_supports_single_observation() -> None:
    assert _warm_median([1.25]) == 1.25
