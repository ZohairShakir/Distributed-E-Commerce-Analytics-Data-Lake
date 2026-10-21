# Phase 2: immutable Bronze ingestion

## What was built

- Discovery of multiple CSV or JSONL files per source dataset.
- Explicit string-preserving schemas and permissive parsing with `_corrupt_record` capture.
- Per-record source path, batch identifier, ingestion timestamp, malformed flag, and SHA-256 hash.
- Exact source-file copies plus Snappy Parquet tables under one immutable batch directory.
- A SHA-256 delivery fingerprint based on dataset and file contents, independent of filenames.
- Atomic local publication through a unique staging directory and one final directory rename.
- Committed JSON metadata with source checksums, row counts, malformed counts, and provenance.
- Duplicate rejection for reused batch IDs and content-identical deliveries, including renamed files.

## Local layout

```text
data/lake/bronze/batch_id=<batch-id>/
├── _metadata/ingestion.json
├── original/<dataset>/<source-file>
└── tables/<dataset>/*.snappy.parquet
```

Only the final `batch_id=...` directory is considered committed. A failed job removes its unique
staging directory. This is a practical local-filesystem commit protocol, not a claim of a
cross-object atomic transaction on Amazon S3.

## Malformed versus business-invalid records

Bronze marks syntax-level parser failures, such as truncated JSON, as malformed and preserves the
raw parser payload in `_corrupt_record`. Values that parse but violate business rules—negative
amounts, unknown keys, or invalid timestamp strings—remain ordinary Bronze records. Phase 3 owns
typing, referential checks, reason codes, and quarantine.

## Verified evidence

The integration fixture uses two behavioral-event files and a truncated JSON record. It verifies
302 event rows, one malformed row, the retained corrupt payload, exact source bytes, metadata,
staging cleanup, and both duplicate guards. A renamed but content-identical delivery is rejected.

The developer workflow was also run on the deterministic Small source on 2026-10-21:

```text
Source files:                 6
Customers:                  500
Products:                   200
Orders:                   2,000
Order items:              4,947
Payment events:           2,000
Behavioral events:       10,000
Reconciled source rows:   25,647
Syntax-malformed rows:         0
Committed Bronze size:      5.4 MB
Files in batch directory:      31
Observed elapsed time:       29.77 seconds
Duplicate rerun exit code:      2
```

The elapsed time is one correctness run with container startup included, not a benchmark.

## Interview-ready explanation

Bronze is immutable because raw history and provenance are more valuable than early cleanup.
Explicit string schemas avoid accidental type coercion. Built-in Spark expressions keep hashing
and metadata visible to Catalyst. The content fingerprint prevents a producer from bypassing
idempotency simply by renaming files, while the batch ID still gives operators a human-readable
delivery key.

Plain Parquet does not provide transactional table updates. This phase only atomically publishes
one local batch directory; later incremental processing still needs a durable ledger, downstream
checkpoints, recovery tests, and an S3-aware commit design.
