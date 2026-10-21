# Phase 3: typed Silver data and reason-coded quarantine

## What was built

- Native Spark normalization for timestamps, decimals, integers, booleans, casing, and blanks.
- Deterministic duplicate detection with a window over each dataset's business key.
- Domain, null, monetary, timestamp, and referential-integrity rules.
- Broadcast joins from facts to the validated customer, product, and order key sets.
- Valid Snappy Parquet tables under `silver/batch_id=<id>/tables/`.
- Invalid Parquet records under `quarantine/batch_id=<id>/` with arrays of reason codes.
- A JSON quality report and final batch commit marker with reconciled counts.
- Late-event delay measurement; lateness is annotated and retained rather than treated as invalid.

No Python UDF is used. Built-in expressions stay visible to Catalyst, avoid Python/JVM row
serialization, and allow Spark to optimize the complete logical plan.

## Validation contract

| Dataset | Business key | Core rules |
|---|---|---|
| Customers | `customer_id` | segment, region, timestamp, required fields |
| Products | `product_id` | category, nonnegative decimal price, USD, boolean active flag |
| Orders | `order_id` | accepted customer, timestamp, region, status, USD |
| Order items | `order_item_id` | accepted order/product, positive quantity, valid money |
| Payments | `payment_id` | accepted order, event type, nonnegative amount, USD, timestamp |
| Behavioral events | `event_id` | accepted customer/product, event type, timestamps, region; purchase order |

An order rejected for an unknown customer is not considered a valid parent. Its items, payments,
and purchase events can therefore be quarantined with an `UNKNOWN_ORDER_ID` reason. This
referential cascade prevents orphan facts from entering Silver and explains why rejected counts
can exceed the generator's independently injected error counts.

## Nulls, duplicates, and malformed input

Blank required strings normalize to null and receive `NULL_REQUIRED_FIELD` plus a specific key
reason where applicable. For duplicate keys, ordering by ingestion timestamp, source filename,
and source-record hash makes the retained row deterministic. Syntax failures already captured in
Bronze receive `MALFORMED_RECORD`; their corrupt payload and source metadata remain traceable.

## Late-arriving events

`arrival_delay_seconds` is `received_at - event_ts`. An accepted event is marked `is_late=true`
when the delay exceeds 86,400 seconds. Late records remain in Silver because arrival delay is an
operational property, not a validity failure. Cross-batch watermarking and reprocessing belong to
Phase 6.

## Commit and recovery boundary

Silver, quarantine, and quality outputs are fully written under a unique staging directory. They
are published to their batch paths and then a JSON commit marker is written under
`metadata/silver_batches/`. Consumers should require that marker. Plain filesystem renames do not
translate into an atomic multi-prefix S3 transaction; hardened orphan recovery and an
object-store-aware protocol remain Phase 6 work.

## Verified Small-run evidence

Observed locally on 2026-10-21 using the committed Phase 2 Bronze batch:

```text
Dataset                Read   Accepted   Rejected
Customers               500        500          0
Products                200        200          0
Orders                 2000       1983         17
Order items            4947       4856         91
Payments               2000       1965         35
Behavioral events     10000       9889        111
Total                 25647      25393        254

Accepted late events:   518
Duplicate records:        0
Required-null records:    0
Spark processing time: 35.293315 seconds
CLI elapsed time:      39.49 seconds
Duplicate rerun:       refused with exit code 2
```

The 111 behavioral rejects comprise 45 invalid event timestamps, 57 unknown products, and nine
purchase events tied to rejected orders. The 91 item rejects include 43 negative prices and 48
unknown accepted-order references. The 35 payment rejects include 18 negative amounts and 17
unknown accepted-order references. A focused integration fixture separately proves malformed,
duplicate, and null quarantine because those cases do not occur in the deterministic Small files.

These timings are single correctness observations with local container overhead, not benchmark
or scalability claims. Repeated performance experiments begin in Phase 5.

## Production failure modes to discuss

- A hot or null-heavy deduplication key can create a skewed window partition.
- Incorrect reference-data timing can quarantine otherwise valid late facts.
- Cascading rejection can amplify one bad dimension delivery, so alert thresholds matter.
- Multiple Parquet prefixes cannot be committed transactionally without a stronger table or
  commit protocol.
- Rules and reason codes require versioning when business definitions change.
