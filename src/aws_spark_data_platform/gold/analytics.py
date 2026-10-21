"""Build reconciled Gold business datasets with Spark SQL."""

from __future__ import annotations

import json
import shutil
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

GOLD_DATASETS = (
    "daily_revenue",
    "product_performance",
    "customer_activity",
    "conversion_funnel",
    "regional_sales",
)


class GoldBatchAlreadyProcessedError(RuntimeError):
    """Raised when a Silver batch already has committed Gold output."""


@dataclass(frozen=True)
class GoldProcessingResult:
    batch_id: str
    gold_path: Path
    report_path: Path
    row_counts: dict[str, int]
    qualified_order_count: int
    excluded_order_count: int
    net_revenue: Decimal
    processing_duration_seconds: float


def _load_silver_tables(spark: SparkSession, silver_batch_path: Path) -> dict[str, DataFrame]:
    table_root = silver_batch_path / "tables"
    datasets = (
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "behavioral_events",
    )
    missing = [name for name in datasets if not (table_root / name).is_dir()]
    if missing:
        raise FileNotFoundError(f"Silver batch is missing tables: {', '.join(missing)}")
    return {name: spark.read.parquet(str(table_root / name)) for name in datasets}


def _register_inputs(tables: dict[str, DataFrame]) -> None:
    for name, frame in tables.items():
        frame.createOrReplaceTempView(f"silver_{name}")


def _qualified_orders(spark: SparkSession) -> DataFrame:
    item_totals = spark.sql(
        """
        SELECT
            order_id,
            CAST(SUM(quantity * unit_price - discount_amount) AS DECIMAL(18, 2)) AS item_total,
            COUNT(*) AS accepted_item_count
        FROM silver_order_items
        GROUP BY order_id
        """
    )
    payment_totals = spark.sql(
        """
        SELECT
            order_id,
            CAST(SUM(CASE WHEN payment_event_type = 'capture' THEN amount ELSE 0 END)
                AS DECIMAL(18, 2)) AS capture_amount,
            CAST(SUM(CASE WHEN payment_event_type = 'refund' THEN amount ELSE 0 END)
                AS DECIMAL(18, 2)) AS refund_amount,
            SUM(CASE WHEN payment_event_type = 'capture' THEN 1 ELSE 0 END) AS capture_count,
            SUM(CASE WHEN payment_event_type = 'refund' THEN 1 ELSE 0 END) AS refund_count
        FROM silver_payments
        GROUP BY order_id
        """
    )
    item_totals.createOrReplaceTempView("gold_item_totals")
    payment_totals.createOrReplaceTempView("gold_payment_totals")
    return spark.sql(
        """
        SELECT
            o.order_id,
            o.customer_id,
            o.order_ts,
            o.region,
            o.status,
            i.item_total,
            i.accepted_item_count,
            CASE
                WHEN o.status = 'completed' THEN p.capture_amount
                WHEN o.status = 'refunded' THEN -p.refund_amount
            END AS revenue
        FROM silver_orders o
        INNER JOIN gold_item_totals i ON o.order_id = i.order_id
        INNER JOIN gold_payment_totals p ON o.order_id = p.order_id
        WHERE
            (
                o.status = 'completed'
                AND p.capture_count = 1
                AND p.refund_count = 0
                AND ABS(p.capture_amount - i.item_total) <= 0.01
            )
            OR
            (
                o.status = 'refunded'
                AND p.refund_count = 1
                AND p.capture_count = 0
                AND ABS(p.refund_amount - i.item_total) <= 0.01
            )
        """
    )


def _gold_frames(
    spark: SparkSession, tables: dict[str, DataFrame]
) -> tuple[dict[str, DataFrame], int]:
    _register_inputs(tables)
    qualified = _qualified_orders(spark).persist(StorageLevel.MEMORY_AND_DISK)
    qualified_count = qualified.count()
    qualified.createOrReplaceTempView("gold_qualified_orders")

    daily_revenue = spark.sql(
        """
        SELECT
            TO_DATE(order_ts) AS date,
            YEAR(order_ts) AS year,
            MONTH(order_ts) AS month,
            region,
            CAST(SUM(revenue) AS DECIMAL(18, 2)) AS revenue,
            COUNT(*) AS order_count,
            CAST(SUM(revenue) / COUNT(*) AS DECIMAL(18, 2)) AS average_order_value
        FROM gold_qualified_orders
        GROUP BY TO_DATE(order_ts), YEAR(order_ts), MONTH(order_ts), region
        """
    )
    product_performance = spark.sql(
        """
        WITH product_sales AS (
            SELECT
                i.product_id,
                p.category,
                SUM(CASE WHEN q.status = 'completed' THEN i.quantity ELSE -i.quantity END)
                    AS units_sold,
                CAST(SUM(
                    CASE WHEN q.status = 'completed'
                        THEN i.quantity * i.unit_price - i.discount_amount
                        ELSE -(i.quantity * i.unit_price - i.discount_amount)
                    END
                ) AS DECIMAL(18, 2)) AS revenue
            FROM gold_qualified_orders q
            INNER JOIN silver_order_items i ON q.order_id = i.order_id
            INNER JOIN silver_products p ON i.product_id = p.product_id
            GROUP BY i.product_id, p.category
        )
        SELECT
            product_id,
            category,
            units_sold,
            revenue,
            DENSE_RANK() OVER (ORDER BY revenue DESC) AS sales_rank
        FROM product_sales
        """
    )
    customer_activity = spark.sql(
        """
        SELECT
            customer_id,
            COUNT(*) AS order_frequency,
            CAST(SUM(revenue) AS DECIMAL(18, 2)) AS total_spend,
            MAX(CASE WHEN status = 'completed' THEN TO_DATE(order_ts) END) AS last_purchase_date
        FROM gold_qualified_orders
        GROUP BY customer_id
        """
    )
    conversion_funnel = spark.sql(
        """
        SELECT
            region,
            SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS product_views,
            SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_events,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_events,
            CAST(
                100.0 * SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END), 0)
                AS DECIMAL(10, 4)
            ) AS conversion_rate_percent
        FROM silver_behavioral_events
        GROUP BY region
        """
    )
    regional_sales = spark.sql(
        """
        SELECT
            region,
            CAST(SUM(revenue) AS DECIMAL(18, 2)) AS revenue,
            COUNT(DISTINCT customer_id) AS unique_customers,
            COUNT(*) AS total_transactions
        FROM gold_qualified_orders
        GROUP BY region
        """
    )
    return (
        {
            "daily_revenue": daily_revenue,
            "product_performance": product_performance,
            "customer_activity": customer_activity,
            "conversion_funnel": conversion_funnel,
            "regional_sales": regional_sales,
        },
        qualified_count,
    )


def process_gold_batch(
    spark: SparkSession,
    lake_root: str | Path,
    batch_id: str,
    *,
    processed_at: datetime | None = None,
) -> GoldProcessingResult:
    """Build all Gold datasets from one committed Silver batch."""
    lake = Path(lake_root).resolve()
    silver_path = lake / "silver" / f"batch_id={batch_id}"
    silver_commit = lake / "metadata" / "silver_batches" / f"batch_id={batch_id}.json"
    if not silver_path.is_dir() or not silver_commit.is_file():
        raise FileNotFoundError(f"Committed Silver batch does not exist: {batch_id}")

    gold_path = lake / "gold" / f"batch_id={batch_id}"
    if gold_path.exists():
        raise GoldBatchAlreadyProcessedError(f"Gold output already exists for batch: {batch_id}")

    started = time.perf_counter()
    timestamp = (processed_at or datetime.now(UTC)).astimezone(UTC)
    staging = lake / "_staging" / f"gold-{batch_id}-{uuid.uuid4().hex}"
    tables = _load_silver_tables(spark, silver_path)
    total_silver_orders = tables["orders"].count()
    frames, qualified_order_count = _gold_frames(spark, tables)
    row_counts: dict[str, int] = {}

    try:
        for name, frame in frames.items():
            frame.persist(StorageLevel.MEMORY_AND_DISK)
            try:
                row_counts[name] = frame.count()
                frame.write.mode("errorifexists").option("compression", "snappy").parquet(
                    str(staging / "tables" / name)
                )
            finally:
                frame.unpersist()

        daily_total = frames["daily_revenue"].agg(F.sum("revenue").alias("revenue")).first()[
            "revenue"
        ] or Decimal("0.00")
        product_total = frames["product_performance"].agg(
            F.sum("revenue").alias("revenue")
        ).first()["revenue"] or Decimal("0.00")
        regional_total = frames["regional_sales"].agg(F.sum("revenue").alias("revenue")).first()[
            "revenue"
        ] or Decimal("0.00")
        if not daily_total == product_total == regional_total:
            raise RuntimeError(
                "Gold revenue reconciliation failed: "
                f"daily={daily_total}, product={product_total}, regional={regional_total}"
            )

        duration = round(time.perf_counter() - started, 6)
        report = {
            "status": "committed",
            "batch_id": batch_id,
            "processed_at_utc": timestamp.isoformat().replace("+00:00", "Z"),
            "source_silver_batch": str(silver_path),
            "row_counts": row_counts,
            "silver_order_count": total_silver_orders,
            "qualified_order_count": qualified_order_count,
            "excluded_order_count": total_silver_orders - qualified_order_count,
            "net_revenue": str(daily_total),
            "revenue_reconciliation_passed": True,
            "processing_duration_seconds": duration,
            "funnel_definition": "event-count ratio: purchase events / product-view events",
            "revenue_policy": (
                "completed capture positive; refund negative; cancelled excluded; "
                "accepted items must reconcile to one matching payment"
            ),
        }
        metadata_path = staging / "_metadata" / "report.json"
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

        gold_path.parent.mkdir(parents=True, exist_ok=True)
        staging.rename(gold_path)
        staging_parent = lake / "_staging"
        if staging_parent.exists() and not any(staging_parent.iterdir()):
            staging_parent.rmdir()
        return GoldProcessingResult(
            batch_id=batch_id,
            gold_path=gold_path,
            report_path=gold_path / "_metadata" / "report.json",
            row_counts=row_counts,
            qualified_order_count=qualified_order_count,
            excluded_order_count=total_silver_orders - qualified_order_count,
            net_revenue=daily_total,
            processing_duration_seconds=duration,
        )
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise
