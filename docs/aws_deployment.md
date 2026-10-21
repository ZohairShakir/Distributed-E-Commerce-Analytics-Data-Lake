# AWS deployment guardrails

Status: planned for Phase 7 and blocked on explicit user approval. No resource has been provisioned.

AWS mode will use IAM roles rather than static access keys, configurable bucket names, encryption,
blocked public access, least-privilege prefixes, and documented cleanup. The exact Hadoop AWS and
AWS SDK JAR versions will be pinned against Spark 3.5.9's Hadoop 3.3 dependency line and tested on
the Small dataset before any larger upload. Glue, Athena, and EMR remain independent optional work.
