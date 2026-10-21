"""Explicit source schemas.

Source fields intentionally enter as strings. Bronze ingestion must preserve malformed values;
Silver will parse and validate typed columns without losing the original source text.
"""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType


def _string_schema(*field_names: str) -> StructType:
    return StructType([StructField(name, StringType(), nullable=True) for name in field_names])


CUSTOMERS_SCHEMA = _string_schema(
    "customer_id",
    "customer_segment",
    "home_region",
    "created_at",
    "source_batch_id",
)

PRODUCTS_SCHEMA = _string_schema(
    "product_id",
    "category",
    "unit_price",
    "currency",
    "is_active",
    "source_batch_id",
)

ORDERS_SCHEMA = _string_schema(
    "order_id",
    "customer_id",
    "order_ts",
    "region",
    "status",
    "currency",
    "source_batch_id",
)

ORDER_ITEMS_SCHEMA = _string_schema(
    "order_item_id",
    "order_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount_amount",
    "source_batch_id",
)

PAYMENT_EVENTS_SCHEMA = _string_schema(
    "payment_id",
    "order_id",
    "payment_event_type",
    "amount",
    "currency",
    "event_ts",
    "source_batch_id",
)

BEHAVIORAL_EVENTS_SCHEMA = _string_schema(
    "event_id",
    "event_type",
    "event_ts",
    "received_at",
    "customer_id",
    "session_id",
    "product_id",
    "order_id",
    "region",
    "source_batch_id",
)
