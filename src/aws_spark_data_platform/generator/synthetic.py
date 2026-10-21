"""Deterministic generator for synthetic e-commerce source records.

No names, email addresses, street addresses, or other sensitive personal fields are
generated. The data models relationships and failure modes without resembling people.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import random
import shutil
from collections.abc import Iterator, Sequence
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, TextIO

from aws_spark_data_platform.generator.models import GenerationConfig, GenerationResult

REGIONS = ("NA", "EU", "APAC", "LATAM")
CATEGORIES = ("electronics", "home", "books", "sports", "beauty", "grocery")
SEGMENTS = ("new", "returning", "loyal")
EVENT_TYPES = ("product_view", "add_to_cart", "purchase")
ANCHOR_TIME = datetime(2026, 10, 20, tzinfo=UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _random_time(rng: random.Random, *, days_back: int) -> datetime:
    seconds = rng.randrange(days_back * 24 * 60 * 60)
    return ANCHOR_TIME - timedelta(seconds=seconds)


def _invalid(rng: random.Random, rate: float) -> bool:
    return rate > 0 and rng.random() < rate


def _write_json_line(handle: TextIO, record: dict[str, Any]) -> None:
    handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
    handle.write("\n")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _csv_writer(handle: TextIO, fields: Sequence[str]) -> csv.DictWriter:
    writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    return writer


def _product_rows(
    rng: random.Random,
    count: int,
    batch_id: str,
) -> Iterator[tuple[dict[str, str], Decimal]]:
    for index in range(1, count + 1):
        price = Decimal(rng.randrange(500, 50_001)) / Decimal(100)
        yield (
            {
                "product_id": f"PROD-{index:06d}",
                "category": rng.choice(CATEGORIES),
                "unit_price": f"{price:.2f}",
                "currency": "USD",
                "is_active": "true" if rng.random() > 0.04 else "false",
                "source_batch_id": batch_id,
            },
            price,
        )


def generate_dataset(
    output_root: str | Path,
    config: GenerationConfig | None = None,
    *,
    force: bool = False,
) -> GenerationResult:
    """Generate a relational synthetic dataset and atomically publish its directory."""
    selected = config or GenerationConfig()
    destination = Path(output_root).resolve()
    if destination.exists() and not force:
        raise FileExistsError(
            f"Output already exists: {destination}. Use force=True to replace it."
        )

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.parent / f".{destination.name}.tmp-{os.getpid()}"
    if temporary.exists():
        shutil.rmtree(temporary)
    temporary.mkdir()

    rng = random.Random(selected.seed)
    customer_count = max(10, selected.event_count // 20)
    product_count = max(10, selected.event_count // 50)
    order_count = max(20, selected.event_count // 5)
    record_counts: dict[str, int] = {}
    invalid_counts: dict[str, int] = {
        "customers": 0,
        "products": 0,
        "orders": 0,
        "order_items": 0,
        "payments": 0,
        "events": 0,
    }
    late_event_count = 0

    try:
        with ExitStack() as stack:
            customers_handle = stack.enter_context(
                (temporary / "customers.csv").open("w", encoding="utf-8", newline="")
            )
            products_handle = stack.enter_context(
                (temporary / "products.csv").open("w", encoding="utf-8", newline="")
            )
            orders_handle = stack.enter_context(
                (temporary / "orders.csv").open("w", encoding="utf-8", newline="")
            )
            items_handle = stack.enter_context(
                (temporary / "order_items.csv").open("w", encoding="utf-8", newline="")
            )
            payments_handle = stack.enter_context(
                (temporary / "payment_events.jsonl").open("w", encoding="utf-8")
            )
            events_handle = stack.enter_context(
                (temporary / "behavioral_events.jsonl").open("w", encoding="utf-8")
            )

            customers_writer = _csv_writer(
                customers_handle,
                ("customer_id", "customer_segment", "home_region", "created_at", "source_batch_id"),
            )
            for index in range(1, customer_count + 1):
                customers_writer.writerow(
                    {
                        "customer_id": f"CUST-{index:07d}",
                        "customer_segment": rng.choice(SEGMENTS),
                        "home_region": rng.choice(REGIONS),
                        "created_at": _iso(_random_time(rng, days_back=730)),
                        "source_batch_id": selected.batch_id,
                    }
                )
            record_counts["customers"] = customer_count

            products_writer = _csv_writer(
                products_handle,
                (
                    "product_id",
                    "category",
                    "unit_price",
                    "currency",
                    "is_active",
                    "source_batch_id",
                ),
            )
            product_prices: list[Decimal] = []
            for product, price in _product_rows(rng, product_count, selected.batch_id):
                products_writer.writerow(product)
                product_prices.append(price)
            record_counts["products"] = product_count

            orders_writer = _csv_writer(
                orders_handle,
                (
                    "order_id",
                    "customer_id",
                    "order_ts",
                    "region",
                    "status",
                    "currency",
                    "source_batch_id",
                ),
            )
            items_writer = _csv_writer(
                items_handle,
                (
                    "order_item_id",
                    "order_id",
                    "product_id",
                    "quantity",
                    "unit_price",
                    "discount_amount",
                    "source_batch_id",
                ),
            )

            order_item_count = 0
            payment_count = 0
            for order_index in range(1, order_count + 1):
                order_id = f"ORD-{order_index:08d}"
                customer_id = f"CUST-{rng.randint(1, customer_count):07d}"
                order_is_invalid = _invalid(rng, selected.invalid_rate)
                if order_is_invalid:
                    customer_id = f"INVALID-CUST-{order_index:07d}"
                    invalid_counts["orders"] += 1
                order_ts = _random_time(rng, days_back=90)
                region = rng.choice(REGIONS)
                status = rng.choices(
                    ("completed", "cancelled", "refunded"), weights=(88, 7, 5), k=1
                )[0]
                orders_writer.writerow(
                    {
                        "order_id": order_id,
                        "customer_id": customer_id,
                        "order_ts": _iso(order_ts),
                        "region": region,
                        "status": status,
                        "currency": "USD",
                        "source_batch_id": selected.batch_id,
                    }
                )

                order_total = Decimal("0.00")
                for _item_sequence in range(1, rng.randint(1, 4) + 1):
                    order_item_count += 1
                    product_index = rng.randint(1, product_count)
                    quantity = rng.randint(1, 4)
                    price = product_prices[product_index - 1]
                    discount = (price * Decimal(rng.choice((0, 0, 0, 5, 10, 15))) / 100).quantize(
                        Decimal("0.01")
                    )
                    item_is_invalid = _invalid(rng, selected.invalid_rate)
                    output_price = -price if item_is_invalid else price
                    if item_is_invalid:
                        invalid_counts["order_items"] += 1
                    order_total += price * quantity - discount
                    items_writer.writerow(
                        {
                            "order_item_id": f"ITEM-{order_item_count:09d}",
                            "order_id": order_id,
                            "product_id": f"PROD-{product_index:06d}",
                            "quantity": quantity,
                            "unit_price": f"{output_price:.2f}",
                            "discount_amount": f"{discount:.2f}",
                            "source_batch_id": selected.batch_id,
                        }
                    )

                payment_is_invalid = _invalid(rng, selected.invalid_rate)
                payment_amount = -order_total if payment_is_invalid else order_total
                if payment_is_invalid:
                    invalid_counts["payments"] += 1
                payment_count += 1
                _write_json_line(
                    payments_handle,
                    {
                        "payment_id": f"PAY-{payment_count:09d}",
                        "order_id": order_id,
                        "payment_event_type": "refund" if status == "refunded" else "capture",
                        "amount": f"{payment_amount:.2f}",
                        "currency": "USD",
                        "event_ts": _iso(order_ts + timedelta(minutes=rng.randint(1, 15))),
                        "source_batch_id": selected.batch_id,
                    },
                )

            record_counts["orders"] = order_count
            record_counts["order_items"] = order_item_count
            record_counts["payments"] = payment_count

            for event_index in range(1, selected.event_count + 1):
                event_ts = _random_time(rng, days_back=30)
                is_late = rng.random() < selected.late_arrival_rate
                if is_late:
                    received_at = event_ts + timedelta(
                        days=rng.randint(1, 7), minutes=rng.randint(0, 59)
                    )
                    late_event_count += 1
                else:
                    received_at = event_ts + timedelta(minutes=rng.randint(0, 30))

                event_type = rng.choices(EVENT_TYPES, weights=(70, 20, 10), k=1)[0]
                product_id = f"PROD-{rng.randint(1, product_count):06d}"
                event_ts_text = _iso(event_ts)
                event_is_invalid = _invalid(rng, selected.invalid_rate)
                if event_is_invalid:
                    invalid_counts["events"] += 1
                    if event_index % 2:
                        product_id = f"INVALID-PROD-{event_index:07d}"
                    else:
                        event_ts_text = "not-a-timestamp"

                _write_json_line(
                    events_handle,
                    {
                        "event_id": f"EVT-{event_index:010d}",
                        "event_type": event_type,
                        "event_ts": event_ts_text,
                        "received_at": _iso(received_at),
                        "customer_id": f"CUST-{rng.randint(1, customer_count):07d}",
                        "session_id": (
                            f"SESSION-{rng.randint(1, max(1, selected.event_count // 3)):09d}"
                        ),
                        "product_id": product_id,
                        "order_id": (
                            f"ORD-{rng.randint(1, order_count):08d}"
                            if event_type == "purchase"
                            else None
                        ),
                        "region": rng.choice(REGIONS),
                        "source_batch_id": selected.batch_id,
                    },
                )
            record_counts["events"] = selected.event_count

        data_files = sorted(path for path in temporary.iterdir() if path.is_file())
        manifest = {
            "synthetic_data": True,
            "generator_version": "0.1.0",
            "seed": selected.seed,
            "batch_id": selected.batch_id,
            "anchor_time_utc": _iso(ANCHOR_TIME),
            "invalid_rate_configured": selected.invalid_rate,
            "late_arrival_rate_configured": selected.late_arrival_rate,
            "late_event_count": late_event_count,
            "record_counts": record_counts,
            "invalid_counts": invalid_counts,
            "files": {path.name: _sha256(path) for path in data_files},
        }
        manifest_path = temporary / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

        if destination.exists():
            shutil.rmtree(destination)
        temporary.replace(destination)
        return GenerationResult(
            output_root=destination,
            record_counts=record_counts,
            invalid_counts=invalid_counts,
            late_event_count=late_event_count,
            manifest_path=destination / "manifest.json",
        )
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        raise
