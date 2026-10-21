# Measured Spark optimization report

Run ID: `20261021T095644Z`  
Recorded UTC: `2026-10-21T09:57:02.272814Z`  
Input profile: `Small synthetic batch`

> These are local-mode observations, not multi-node scalability claims. The first run is
> labeled cold and the remaining runs use their median. OS caching can still affect both
> categories.

## Results

| Experiment | Variant | Rows | Cold s | Warm median s | Bytes | Files |
|---|---|---:|---:|---:|---:|---:|
| CSV vs Parquet scan | csv_baseline | 2000 | 0.753128 | 0.278200 | 152610 | 1 |
| CSV vs Parquet scan | parquet_candidate | 2000 | 0.427233 | 0.322916 | 168724 | 1 |
| CSV vs Parquet scan | observed baseline/candidate warm ratio | — | — | 0.862× | — | — |
| Shuffle vs broadcast join | shuffle_baseline | 4856 | 1.206476 | 0.607984 | 456207 | 8 |
| Shuffle vs broadcast join | broadcast_candidate | 4856 | 0.651685 | 0.521882 | 456207 | 8 |
| Shuffle vs broadcast join | observed baseline/candidate warm ratio | — | — | 1.165× | — | — |
| Unpartitioned vs date-partitioned filter | unpartitioned_baseline | 359 | 0.424881 | 0.260498 | 1008494 | 2 |
| Unpartitioned vs date-partitioned filter | partitioned_candidate | 359 | 0.338629 | 0.261402 | 1247756 | 30 |
| Unpartitioned vs date-partitioned filter | observed baseline/candidate warm ratio | — | — | 0.997× | — | — |

## Physical-plan observations

- CSV plan contained `FileScan csv`: true.
- Parquet plan contained `FileScan parquet`: true.
- Forced regular join contained `SortMergeJoin`: true.
- Hinted join contained `BroadcastHashJoin`: true.
- Partitioned filter exposed partition filters: true.

## Environment

```json
{
  "adaptive_execution": "true",
  "container_memory_limit_bytes": 3221225472,
  "logical_cpu_count": 8,
  "machine": "aarch64",
  "platform": "Linux-6.12.76-linuxkit-aarch64-with-glibc2.36",
  "shuffle_partitions": "4",
  "spark_master": "local[2]",
  "spark_version": "3.5.9"
}
```

## Interpretation

The broadcast candidate improved the warm median in this run because the 200-row product side is
small enough to copy to executors and avoid shuffling both inputs. That does not justify
broadcasting a large or uncertain relation.

Parquet had a faster cold observation but a slower warm median than CSV for this 2,000-row,
single-column grouping. The Parquet Bronze file also carries ingestion metadata and was larger
than the original CSV. At this scale, scheduling and metadata dominate the format advantages.

Date partitioning exposed a real `PartitionFilters` plan but created 30 data files versus two and
was essentially neutral in the warm median. Partition pruning can matter at larger scale, but
this Small result argues against adding high-cardinality partitions prematurely.

An observed ratio above 1 means the candidate was faster in these runs; below 1 means it was
slower. Repeated Medium-profile testing may be useful later, but it requires an explicit resource
decision and was not run automatically.
