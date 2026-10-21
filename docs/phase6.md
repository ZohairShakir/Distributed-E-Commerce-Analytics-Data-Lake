# Phase 6: incremental state, watermarks, and recovery

## Implemented control plane

`make run-pipeline` runs or resumes a source batch through Bronze, Silver, and Gold. Each batch has
an atomically replaced JSON state document under `metadata/pipeline_batches/` containing its
source path, source fingerprint, attempt number, committed stages, timestamps, watermark summary,
and last failure.

An exclusive per-batch lock file rejects concurrent local processing. Existing layer commit
markers are discovered before execution, so a retry skips already committed work. A completed
batch returns without incrementing its attempt or rewriting analytical output.

## Failure recovery

The integration test injects a failure immediately after Bronze commits. It then creates a fake
partial Silver directory. On retry, orchestration:

1. recognizes the committed Bronze marker;
2. moves the uncommitted Silver directory to a timestamped `_recovery/` archive;
3. executes Silver and Gold once;
4. writes the final checkpoint and completed batch state; and
5. returns a subsequent rerun without a third processing attempt.

Recovery archives rather than deletes partial output, preserving evidence for diagnosis. Bronze's
own staging/publish protocol still prevents an incomplete final Bronze batch.

## Event watermark behavior

After Silver commits, the pipeline records maximum accepted `event_ts`, maximum `received_at`,
accepted late-event count, and the number of events at or before the previous global event-time
watermark. The global checkpoint is updated only after Gold commits. Late records are retained;
the metadata makes their cross-batch effect visible rather than silently discarding them.

This is batch watermark tracking, not Structured Streaming exactly-once processing. Gold remains
batch-scoped, and plain Parquet does not provide atomic upserts or snapshot isolation.

## Verified local evidence

The existing Small batch was adopted into the ledger without rewriting its committed layers:

```text
Batch:                          batch-0001
Attempt:                                  1
Bronze/Silver/Gold:               committed
Accepted late events:                   518
Maximum event time:     2026-10-19T23:49:10Z
Maximum receipt time:   2026-10-26T23:43:15Z
First ledger adoption:                  8.42 seconds elapsed
Completed idempotent rerun:             3.03 seconds elapsed
Rerun attempt count:                       1 (unchanged)
```

The recovery integration fixture passed with an injected failure, two attempts, an archived
orphan, completed downstream stages, and one checkpoint entry.

## Guarantees and limitations

- Content fingerprints prevent identical source delivery under a renamed file or new batch ID.
- Layer commit markers prevent downstream consumers from accepting staged output.
- Batch state and checkpoint JSON use same-filesystem atomic replacement locally.
- A process killed without `finally` may leave a stale lock requiring operator review.
- S3 does not provide directory rename semantics; this local protocol must not be described as an
  S3 transaction or exactly-once delivery.
- Cross-batch Gold consolidation would require deterministic rewrites or a transactional table
  format such as Iceberg; that remains an optional design extension.
