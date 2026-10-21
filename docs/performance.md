# Performance benchmark plan

Status: planned for Phase 5. No benchmark numbers or speedup claims exist yet.

The harness will record cold and warm runs, input/output byte sizes, row counts, compression,
Spark configuration, run count, CPU/memory environment, elapsed wall time, file count, and
physical-plan observations. Initial experiments will compare CSV with Parquet, partitioned with
unpartitioned reads, regular with broadcast joins, and recomputation with caching. Each conclusion
will report variance and acknowledge regressions.
