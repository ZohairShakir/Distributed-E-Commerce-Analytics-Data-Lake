# Phase 4: reconciled Gold analytics with Spark SQL

## Implemented datasets and grains

| Dataset | Grain | Implemented metrics |
|---|---|---|
| `daily_revenue` | order date × region | signed revenue, order count, average order value |
| `product_performance` | product | net units, signed revenue, global dense sales rank |
| `customer_activity` | customer with a qualified order | frequency, signed spend, last completed purchase date |
| `conversion_funnel` | region for the batch window | view, cart, purchase event counts and purchase/view rate |
| `regional_sales` | region | signed revenue, distinct customers, transaction count |

The funnel is explicitly event-based. Its conversion percentage is purchase-event count divided
by product-view-event count; it is not a unique-customer or session conversion rate.

## Financial qualification policy

Gold does not assume every accepted Silver order is financially complete. It first aggregates
accepted items and payments, then qualifies an order only when:

- a completed order has exactly one capture, no refund, and capture amount equals item total;
- a refunded order has exactly one refund, no capture, and refund amount equals item total; and
- the accepted item total is `quantity × unit_price - discount_amount` for every line.

Completed revenue is positive, refund revenue is negative, and cancelled orders contribute zero
because they are excluded. This also excludes orders whose item or payment was quarantined and
would otherwise produce an incomplete financial fact.

Daily, product, and regional revenue are independently summed and must match before the batch is
published. A mismatch fails the job rather than writing contradictory business datasets.

## Spark execution concepts

The pipeline registers Silver DataFrames as temporary views and executes Spark SQL aggregations.
Item and payment grouping and all business aggregations are wide transformations that can
shuffle. Qualified orders are cached because five downstream datasets reuse them. The global
product rank intentionally has no partition key, so Spark places the small product summary in one
window partition; this is acceptable for 200 products but must be revisited for a very large
catalog.

All transformations remain lazy until count, reconciliation, or Parquet-write actions execute.
No Python UDF is used.

## Independent financial test

The integration fixture uses manually specified expected values rather than generator-derived
expectations: a $19 completed order, a $10 refund, a cancelled $5 order, and a completed order
whose $4 payment does not match its $5 item. Gold must qualify two orders, exclude two, and report
exactly $9 net revenue in daily, product, customer, and regional outputs. It also verifies a 50%
event-count conversion rate from two views and one purchase.

## Verified Small-run evidence

Observed locally on 2026-10-21 from the committed Phase 3 Silver batch:

```text
Accepted Silver orders:       1,983
Qualified financial orders:   1,768
Excluded orders:                215
Net revenue:          $2,501,057.56

daily_revenue rows:              357
product_performance rows:        200
customer_activity rows:          486
conversion_funnel rows:            4
regional_sales rows:                4

Gold batch size:                180 KB
Files in Gold batch:                41
Spark processing time:      17.416523 seconds
CLI elapsed time:                21.53 seconds
Duplicate rerun:          refused with exit code 2
```

Regional revenue independently reconciled to the same total: APAC $626,599.32, EU $607,348.55,
LATAM $606,783.89, and NA $660,325.80. These are synthetic business results. Timings are
single-run correctness observations, not benchmark results.

## Production considerations

- Financial event models often allow multiple captures and partial refunds; this project
  deliberately implements a narrower one-terminal-payment contract and documents it.
- Global ranking is a single-partition summary operation and can bottleneck on a huge catalog.
- Negative net revenue and units are valid when refunds exceed completed sales in a group.
- Plain Parquet has no snapshot isolation across multiple batch directories.
- Gold currently represents one batch. Cross-batch incremental merging and recovery are Phase 6.
