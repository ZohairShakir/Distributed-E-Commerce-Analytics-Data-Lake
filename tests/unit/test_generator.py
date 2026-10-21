from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from aws_spark_data_platform.generator import GenerationConfig, generate_dataset
from aws_spark_data_platform.generator.cli import main


def _contents(root: Path) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in sorted(root.iterdir()) if path.is_file()}


def test_generator_is_byte_for_byte_deterministic(tmp_path: Path) -> None:
    config = GenerationConfig(
        event_count=250,
        invalid_rate=0.04,
        late_arrival_rate=0.10,
        seed=1234,
        batch_id="test-batch",
    )
    first = tmp_path / "first"
    second = tmp_path / "second"

    generate_dataset(first, config)
    generate_dataset(second, config)

    assert _contents(first) == _contents(second)


def test_generator_preserves_valid_references_and_injects_declared_failures(tmp_path: Path) -> None:
    output = tmp_path / "synthetic"
    result = generate_dataset(
        output,
        GenerationConfig(event_count=1_000, invalid_rate=0.05, late_arrival_rate=0.10, seed=42),
    )

    with (output / "customers.csv").open(encoding="utf-8", newline="") as handle:
        customer_ids = {row["customer_id"] for row in csv.DictReader(handle)}
    with (output / "products.csv").open(encoding="utf-8", newline="") as handle:
        product_ids = {row["product_id"] for row in csv.DictReader(handle)}
    with (output / "orders.csv").open(encoding="utf-8", newline="") as handle:
        orders = list(csv.DictReader(handle))
    with (output / "order_items.csv").open(encoding="utf-8", newline="") as handle:
        items = list(csv.DictReader(handle))

    assert all(
        order["customer_id"] in customer_ids
        for order in orders
        if not order["customer_id"].startswith("INVALID-")
    )
    assert all(item["product_id"] in product_ids for item in items)
    assert result.invalid_counts["orders"] > 0
    assert result.invalid_counts["order_items"] > 0
    assert result.invalid_counts["payments"] > 0
    assert result.invalid_counts["events"] > 0
    assert result.late_event_count > 0

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["synthetic_data"] is True
    assert manifest["record_counts"] == result.record_counts
    assert manifest["invalid_counts"] == result.invalid_counts
    assert manifest["record_counts"]["events"] == 1_000


@pytest.mark.parametrize(
    ("field", "value"),
    (("event_count", 0), ("invalid_rate", 0.26), ("late_arrival_rate", 1.01)),
)
def test_generation_config_rejects_unsafe_values(field: str, value: float) -> None:
    values: dict[str, float | int] = {
        "event_count": 10,
        "invalid_rate": 0.01,
        "late_arrival_rate": 0.05,
    }
    values[field] = value
    with pytest.raises(ValueError):
        GenerationConfig(**values)


def test_generator_requires_explicit_force_to_replace_output(tmp_path: Path) -> None:
    output = tmp_path / "synthetic"
    config = GenerationConfig(event_count=10)
    generate_dataset(output, config)

    with pytest.raises(FileExistsError, match="force=True"):
        generate_dataset(output, config)

    generate_dataset(output, config, force=True)
    assert (output / "manifest.json").is_file()


def test_cli_refuses_large_profile_without_opt_in(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "medium"
    monkeypatch.setattr(
        "sys.argv",
        ["generate-data", "--size", "medium", "--output", str(output)],
    )

    with pytest.raises(SystemExit, match="--allow-large"):
        main()

    assert not output.exists()
