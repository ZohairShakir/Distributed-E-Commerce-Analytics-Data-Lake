# Performance benchmark methodology

Status: Phase 5 Small-profile experiments implemented and run. See
[`benchmark_results.md`](benchmark_results.md) for exact measurements.

The harness records cold and warm runs, byte sizes, row counts, Spark configuration, run count,
CPU/memory environment, elapsed time, file count, and physical-plan observations. It compares CSV
with Parquet scans, unpartitioned with event-date-partitioned reads, and forced sort-merge with
broadcast-hash joins.

Each variant runs three times. The first observation is labeled cold; the other two form the warm
median. The harness preserves regressions and warns that OS caching and local scheduler overhead
remain confounders. A candidate is never called an optimization solely because its plan looks
different. Caching and shuffle-partition experiments remain useful future additions.
