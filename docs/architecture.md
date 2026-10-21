# Architecture and local resource plan

## Decision record

Phase 1 uses PySpark 3.5.9 in local mode with filesystem-backed storage. Local mode is the
smallest environment that executes actual Spark logical and physical plans. Docker supplies the
missing Java 17 and Python 3.11 runtimes and behaves consistently on ARM64 and x86-64 hosts.

Spark 4 is not required for the target concepts, while Spark 3.5 remains representative of many
managed Spark environments and has a mature Hadoop 3 integration line. The project can revisit
Spark 4 after the complete pipeline establishes a benchmark baseline.

## Logical data flow

```mermaid
flowchart TB
    subgraph Sources
      C[CSV dimensions and transactions]
      J[JSONL payment and behavior events]
    end
    subgraph Lake
      B[Bronze / immutable batch paths]
      S[Silver / validated Parquet]
      X[Quarantine / reason-coded rejects]
      G[Gold / partitioned analytical Parquet]
      M[Metadata / manifests and batch ledger]
    end
    subgraph Compute
      P[PySpark DataFrame transformations]
      SQL[Spark SQL]
    end
    C --> B
    J --> B
    B --> P
    M <--> P
    P --> S
    P --> X
    S --> SQL
    SQL --> G
```

Local storage will use `data/{bronze,silver,gold,quarantine,metadata}`. AWS mode will preserve the
same logical prefixes behind a configurable URI. Code should not encode bucket names.

## Implemented Bronze batch boundary

Phase 2 fingerprints each source delivery from its dataset names and file content, then rejects a
reused batch ID or content-identical delivery. It writes exact source copies, explicit-schema
Parquet, and commit metadata to a unique staging path before publishing one immutable batch
directory with a local filesystem rename. The fingerprint does not depend on filenames, so a
renamed redelivery is still detected. This provides safe Bronze reruns but is not called
exactly-once processing.

Phase 6 now adds a local batch ledger, per-batch lock, event watermark checkpoint, committed-stage
discovery, and recoverable archival of uncommitted Silver/Gold outputs. State files are published
with same-filesystem atomic replacement. The S3 warning still applies: this local control plane is
not presented as an object-store transaction or exactly-once guarantee.

Plain Parquet cannot atomically update rows across files. Cross-batch Gold consolidation would
need deterministic rewrites; Iceberg remains an optional enhancement for transactional updates.

## Resource estimates

These are capacity-planning ranges, not measured benchmark results:

| Profile | Behavioral events | Suggested cores | Suggested container memory | Approx. working disk |
|---|---:|---:|---:|---:|
| Small | 10,000 | 2 | 2–3 GB | under 250 MB plus image |
| Medium | 100,000 | 2–4 | 3–4 GB | 0.5–1 GB plus image |
| Large | 1,000,000 | 4+ | 6–8 GB | 3–6 GB plus image |
| Stress | 5,000,000+ | hardware-dependent | 12 GB+ | 15 GB+ plus image |

The actual expansion depends on line-item multiplicity, Spark spill, file counts, and output
format. The current 8 GB host should remain on Small for normal development. Medium may be used
later with monitoring; Large and Stress should not run without explicit review.

## Failure domains to design for

- Truncated or malformed source files.
- Re-delivered files under a different filename.
- Partial writes and driver interruption.
- Duplicate events and dimension-key misses.
- Event-time lateness across batch boundaries.
- Skewed products or regions causing long-running shuffle partitions.
- Excessive small Parquet files.
- S3 request throttling, IAM denial, or network interruption in AWS mode.
