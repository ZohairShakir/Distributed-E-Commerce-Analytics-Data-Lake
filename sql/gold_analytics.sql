-- Executable query shapes used by Phase 4. The Python orchestration registers the Silver and
-- qualified-order temporary views before executing equivalent Spark SQL statements.

-- daily_revenue grain: order date x region
SELECT
    TO_DATE(order_ts) AS date,
    region,
    SUM(revenue) AS revenue,
    COUNT(*) AS order_count,
    SUM(revenue) / COUNT(*) AS average_order_value
FROM gold_qualified_orders
GROUP BY TO_DATE(order_ts), region;

-- conversion_funnel grain: region for the batch reporting window.
-- This is an event-count ratio, not a unique-user or session conversion rate.
SELECT
    region,
    SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS product_views,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_events,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_events
FROM silver_behavioral_events
GROUP BY region;
