# Portfolio and interview guide

## Resume project title

**Distributed E-Commerce Analytics Data Lake** | PySpark, Spark SQL, Parquet, Docker

AWS S3 is intentionally omitted from the skills line until the optional cloud phase is actually
deployed and tested.

## Resume bullets

- Built a Dockerized PySpark 3.5.9 medallion data lake that processed 25,647 deterministic
  synthetic e-commerce records across six CSV/JSON sources into traceable Bronze, typed Silver,
  and five Spark SQL Gold datasets, backed by 15 automated tests.
- Implemented SHA-256 delivery idempotency, native Spark validation and deduplication, reason-coded
  quarantine for 254 records, late-event tracking for 518 accepted events, and a recoverable batch
  ledger proven with injected-failure resume testing.
- Reconciled 1,768 qualified synthetic financial transactions to $2,501,057.56 across daily,
  product, and regional analytics; measured a 1.165× warm-median broadcast-join improvement while
  documenting Parquet and partitioning regressions on the Small profile.

All counts and timings come from the verified local Small run. The revenue is synthetic, Spark ran
in `local[2]`, and the project makes no multi-node or AWS deployment claim.

## Interview explanations

### What is distributed computing?

It divides data and work into partitions that multiple executors can process concurrently. A
driver builds and schedules the plan; tasks operate on partitions. This project exercises that
execution model locally, but two local worker threads do not prove cluster scalability.

### Why Spark instead of Pandas?

Pandas is ideal for in-memory, single-process analysis. Spark provides lazy logical plans,
Catalyst optimization, partitioned execution, shuffle-aware joins and aggregations, spill, and
cluster execution. Those capabilities match a data-lake workload whose future data may exceed one
machine.

### What is a shuffle?

A shuffle redistributes records across partitions, typically for joins, grouping, deduplication,
or global ordering. It creates a stage boundary and adds serialization, network, memory, and disk
cost. The forced sort-merge benchmark shuffles both sides; the broadcast candidate avoids that for
the large side.

### What is a broadcast join?

Spark copies a sufficiently small relation to executors so each partition of the large relation
can join locally. It helped for the 200-row product table in the measured run. Broadcasting a
large or misestimated table can exhaust executor memory, so the hint is data-size-dependent.

### What is partition pruning?

Spark uses directory partition predicates to skip irrelevant files. The benchmark plan showed a
real partition filter, but 30 tiny files erased the expected benefit on 9,889 events. Partitioning
helps only when common selective filters and sufficient data size outweigh file/metadata costs.

### Why Parquet for analytics?

Parquet stores typed columns separately, supports compression, column pruning, predicate pushdown,
and schema metadata. It usually reduces analytical I/O. In the tiny scan benchmark, its warm
median was slower and its Bronze file larger because metadata and scheduling dominated—an example
of why format benefits must be measured in context.

### How do Bronze, Silver, and Gold differ?

Bronze preserves exact inputs and provenance. Silver standardizes types, removes duplicate keys,
validates relationships and business rules, and quarantines failures. Gold applies explicitly
defined business semantics and publishes stable analytical grains.

### How does incremental batch processing work here?

Content fingerprints and batch IDs identify deliveries. Commit markers identify complete layers.
The orchestrator locks one batch, discovers committed stages, archives uncommitted partial output,
resumes the next stage, and only advances the global event checkpoint after Gold commits. This is
idempotent recovery, not exactly-once processing.

### What causes data skew?

A few hot keys can make one shuffle or window partition much larger than the rest, producing
straggler tasks. Null-heavy deduplication keys and dominant products/regions are examples. Typical
responses include early filtering, better partition keys, adaptive execution, salting selected
joins, and separating exceptional keys after measuring the plan.

### How would this run on EMR?

Package the same jobs for an EMR Spark driver, size executor cores/memory for the measured data,
and store layers in IAM-authorized S3 prefixes. Use instance roles rather than keys, CloudWatch and
Spark UI for observability, and an S3-aware commit protocol. Cluster launch, scaling, and cost must
be tested before calling this EMR experience; none was launched for this project.

## Likely follow-up questions

1. Why can a content fingerprint still not guarantee exactly-once business outcomes?
2. Why do rejected parent orders cascade to otherwise parseable child records?
3. When would the narrow one-payment Gold contract need redesign for partial refunds?
4. Why did partition pruning fail to improve the Small warm median?
5. What failure can leave a stale lock, and how would lease expiry be made safe?
6. How would Iceberg change cross-batch Gold updates and rollback behavior?
