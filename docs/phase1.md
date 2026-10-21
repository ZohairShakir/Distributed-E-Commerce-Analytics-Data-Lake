# Phase 1: what was built and what to explain

## What was built

- A Python 3.11/Java 17/PySpark 3.5.9 Docker runtime.
- A deterministic generator for related CSV and JSONL datasets.
- Explicit string-preserving Spark source schemas.
- A Spark job that reads 10,000 behavioral events, executes a distributed-style aggregation,
  reconciles the count to the manifest, and writes Snappy Parquet.
- Unit and end-to-end local Spark tests.

## How Spark executes the smoke transformation

Reading creates a lazy DataFrame plan. `count()` is the first action and starts a job whose scan
tasks read input partitions. `groupBy(event_type)` is a wide transformation: values with the same
key may need to cross partition boundaries in a shuffle. The Parquet write is another action.
Spark schedules stages around shuffle boundaries and tasks within each stage. `local[2]` runs up
to two tasks concurrently in one machine's JVM.

The job intentionally uses built-in `count`, `groupBy`, and ordering expressions. A Python UDF
would serialize data between the JVM and Python workers, hide expression semantics from Spark's
Catalyst optimizer, and add overhead without solving a problem here.

## Interview-ready distinctions

- **Distributed computing:** data and work are divided into partitions and tasks that can run on
  multiple executors; the driver plans and coordinates the work.
- **Spark versus Pandas:** Pandas is excellent for in-memory single-process analysis. Spark adds
  a lazy optimizer, partitioned execution, shuffle-aware operators, spill, and cluster managers.
- **Shuffle:** redistribution of records across partitions, often for joins and aggregations. It
  adds network/disk/serialization cost and creates stage boundaries.
- **Broadcast join:** copies a sufficiently small relation to executors to avoid shuffling the
  large relation. It can fail or regress if the broadcast side is not actually small.
- **Partition pruning:** skips directory partitions whose keys cannot match a filter. Too many
  partitions create small files and metadata overhead.
- **Parquet:** columnar layout, typed metadata, compression, and predicate/column pushdown make it
  well suited to analytical scans.
- **Medallion layers:** Bronze preserves traceability, Silver creates reliable standardized data,
  and Gold presents defined business metrics.
- **Data skew:** a few hot keys create much larger partitions and straggler tasks.
- **EMR path:** submit the same packaged job to a driver on EMR; executors run on worker capacity,
  data lives in S3, IAM roles replace local credentials, and CloudWatch/Spark UI support monitoring.

## Likely Amazon follow-ups

1. Why does a local Spark job not prove horizontal scalability?
2. Which operation in the smoke job is wide, and where is the shuffle boundary?
3. Why ingest malformed monetary values as strings before casting?
4. How will a batch manifest prevent accidental reprocessing?
5. What guarantees are missing when multiple Parquet files are published?
