# Cloud-Native Big Data Infrastructure & Governance
## Governed Research-Data Landing Zone

### 1. System Design
This laboratory implements an isolated, governed, multi-tenant research data landing zone on Kubernetes using single-node SeaweedFS (`weed mini`) as the S3-compatible object store. 

Tenant boundaries are enforced through a strict `team-budget` ResourceQuota restricting the aggregate footprint to 2 CPU requests / 4 CPU limits, 2Gi memory requests / 4Gi limits, and one 4Gi PersistentVolumeClaim (`object-data`). Network ingress to the private ClusterIP storage Service on port 8333 is tightly constrained via a Kubernetes `NetworkPolicy` (`private-object-store`), allowing traffic solely from Pods carrying the label `access: s3`.

The object store hosts two segregated data zones: an unapproved landing zone (`research-raw`) and an approved release zone (`research-release`). Access governance is enforced at the S3 API layer using bucket-scoped action policies across three workloads: `owner` (full administrative rights), `ingestor` (read/write/list restricted to `research-raw`), and `analyst` (read/list restricted to `research-release`). A distinct unauthenticated client (`blocked`) and a read-only Kubernetes ServiceAccount (`observer`) validate network and control-plane isolation boundaries.

### 2. Experimental Results
1. **Tenant Guardrails (Task 1):** Admission control cleanly admitted a compliant probe (`quota-positive`) and explicitly rejected an oversized request (`quota-negative`, requesting 3 CPUs) with HTTP 403 Forbidden due to quota exhaustion, preserving the 900m CPU / 1024Mi memory operational budget.
2. **Persistent Storage & Data Seeding (Task 2):** Bound a 4Gi PVC mounted at `/data`, seeded four fixtures across both buckets, and verified role-based reads matching the expected SHA-256 digest (`9ce4c8bb...`).
3. **Access Governance (Task 3):** Successfully demonstrated all 22 security controls (12 S3, 6 RBAC, 4 Network). All unauthorized S3 requests returned explicit HTTP 403 `AccessDenied` responses. Observer permissions verified read-only API access without secret extraction or pod deletion privileges.
4. **Data Path Measurement (Task 4):** Completed six controlled trials (192 PUTs and 192 GETs, 100% verified integrity). Increasing concurrency from 1 to 4 yielded median Goodput ratios of 0.96x for PUT and 0.90x for GET, while p95 latency increased dramatically (from ~110 ms to ~686 ms for PUT) due to single-core CPU throttling and disk lock serialization.
5. **Fault Recovery (Task 5):** Replaced the storage Pod under graceful termination. A replacement Pod became Ready within 31.28 seconds, with an observed canary interruption $T_{observed} = 10.05\text{ s}$ ($\le 120\text{ s}$). All 32 benchmark object hashes remained unchanged on the retained PVC UID (`9484778c...`).

### 3. Limitations & Production Gaps
While functional for education, this deployment possesses explicit architectural limitations:
- **No High Availability:** The single storage replica experiences transient downtime ($10.05\text{ s}$) during container replacement.
- **No Backup or Node-Loss Durability:** Volume retention on a single hostPath StorageClass ties durability to a single physical node without point-in-time recovery.
- **Transport Security & Workload Identity:** Payloads transmit over plaintext HTTP without TLS or encryption at rest. Static Secret environment variables are used in place of federated OpenID Connect workload identities.
