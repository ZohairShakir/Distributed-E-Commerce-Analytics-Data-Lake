# Cost-estimation framework

Status: planned; no AWS costs have been incurred.

Before deployment, estimate S3 stored GB-months, PUT/LIST/GET requests, Athena bytes scanned,
Glue crawler/catalog usage, EMR instance-hours, EBS, and data transfer. Use current regional AWS
pricing at execution time, apply a small-data cap, add resource tags, and define cleanup steps.
Prices are intentionally not hardcoded because they change.
