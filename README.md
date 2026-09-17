# gitops-lakehouse

GitOps source of truth for the Nimbus Iceberg lakehouse on the `nimbus` RKE2 cluster: MSSQL CDC
(Debezium) → Kafka → Flink (RAW/SILVER) → Airflow-orchestrated Spark (GOLD) → StarRocks/Superset,
with OpenMetadata + Ranger for governance. MinIO and MSSQL are external (LXC host
`192.168.80.128`). Full design and rationale: see the working repo's
`~/.claude/plans/snazzy-strolling-perlis.md` plan document (Pre-Deployment / Deployment /
Post-Deployment).

## Layout

- `platform/` — operators + CRDs (Strimzi, Flink, Spark, StarRocks). Installed once, rarely
  destroyed.
- `workloads/` — the actual lakehouse: Kafka, Flink jobs, Airflow, Spark jobs, StarRocks,
  OpenMetadata, Ranger, Superset. This is what `make destroy`/`make redeploy` targets.
- `infra/` — namespaces and CNPG `Cluster` CRs shared across workloads.
- `bootstrap/` — the two ArgoCD app-of-apps roots (`platform-root`, `workloads-root`).
- `scripts/` — operational helpers (CDC simulation wrapper).

Every `platform/<x>` and `workloads/<x>` directory contains its own `application.yaml` (an
ArgoCD `Application`) plus the actual resources it manages, kept unambiguous via either a
`kustomization.yaml` that lists only the real resources, or by keeping the Application's own
source path pointed at a `manifests/` subfolder (Airflow) — never the two mixed in one
directory listing. See `bootstrap/root-*-app.yaml` for why (`directory.include: "**/application.yaml"`).

## Day-0 (once, before `make deploy`)

1. Confirm the cluster can reach `192.168.80.128` (both MinIO `:39090` and MSSQL `:1433` ride
   the same path) — e.g. a throwaway debug pod with `nc -zv`.
2. Create the `mssql-credentials` Secret in `lakehouse-streaming` (CDC-reader username/password;
   keys `cdc_username` / `cdc_password` — never commit these to git).
3. Copy the wildcard TLS cert: `infra/tls/copy-wildcard-tls.sh` (or rely on the Rancher
   project-scoped auto-sync via `infra/namespaces/namespaces.yaml`'s project annotation).
4. Install ArgoCD in the `argocd` namespace (not managed by this repo — chicken/egg).
5. `make deploy`.

Two more Day-0-shaped secrets get created the first time their component actually boots
(documented alongside them, not repeated here): `polaris-bootstrap-credentials` (placeholder
already in git, rotate it), `om-bot-token` and `ranger-admin-credentials` (see
`workloads/openmetadata/README.md` and `workloads/ranger/README.md`).

## Day-2

- `make status` — what's Synced/Healthy vs not.
- `make destroy CONFIRM=yes` — tears down `workloads-root` only (Kafka, Flink jobs, Airflow,
  Spark jobs, StarRocks, OpenMetadata, Ranger, Superset, their CNPG clusters). Leaves
  `platform-root` (operators/CRDs) and all PVs (Ceph `Retain`) alone.
- `make redeploy CONFIRM=yes` — destroy + deploy in one step; the actual reproducibility test.
- `make destroy-all CONFIRM=yes` — also removes `platform-root` and explicitly deletes the
  orphaned PVs. The only path that discards data.
- `scripts/simulate_cdc.sh [batch]` — generate CDC traffic against RetailDB and walk through the
  post-deployment verification checklist it prints.

## Known follow-ups (flagged in place, not silently assumed done)

- `workloads/flink-jobs/README.md` — the SQL-runner jar isn't vendored yet.
- `workloads/ranger/README.md` — StarRocks Ranger plugin registration isn't wired yet.
- Exact Helm value schemas for Airflow/Superset/OpenMetadata/Ranger were written against
  current chart versions at authoring time — re-verify with `helm show values` before first
  deploy, since none of these charts have been dry-run against the live cluster yet.
