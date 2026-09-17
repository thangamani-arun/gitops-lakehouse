# Ranger — Day-0 notes and known follow-ups

- **`ranger-admin-credentials` Secret** (keys `RANGER_ADMIN_USER`, `RANGER_ADMIN_PASSWORD`):
  same Day-0-manual-secret pattern as `mssql-credentials` — Ranger ships a default
  `admin`/`rangeradmin1` credential that must be rotated on first boot, then stored here.
  `kubectl create secret generic ranger-admin-credentials -n lakehouse-governance --from-literal=...`

- **StarRocks Ranger plugin registration is not yet wired here.** Enforcing
  `policies/customers-deny-test-policy.yaml` against StarRocks requires two things this repo
  doesn't do yet:
  1. Registering a Ranger **service** named `starrocks_nimbus-lakehouse` (service type
     `starrocks`) via Ranger admin — the policy above assumes this service already exists.
  2. Installing the StarRocks-side Ranger plugin (config pointing StarRocks FE at
     `ranger-admin.lakehouse-governance.svc:6080`) — packaging/config for this varies by
     StarRocks version; check the StarRocks docs for the version pinned in
     `workloads/starrocks/starrockscluster.yaml` (currently `3.3`) before wiring it up.

  Until both exist, `ranger-apply-customers-deny-policy` will fail with "service not found" —
  expected on a fresh install, and one of the Post-Deployment "Catalog & lineage management"
  checks (the plan's Ranger deny-test) can't pass until this is completed.
