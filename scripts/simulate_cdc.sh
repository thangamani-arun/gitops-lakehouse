#!/usr/bin/env bash
# Post-deployment CDC simulation — thin wrapper around the generator that already exists on
# the mssql-src LXC container. See the plan's "End-to-end CDC simulation" section for the full
# verification procedure this feeds into (RAW -> SILVER -> change-log -> selective GOLD -> BI).
set -euo pipefail

BATCH="${1:-1000}"

echo "==> generating ~${BATCH} CDC events across RetailDB (customers/products/sales_transactions)"
lxc exec mssql-src -- python3 /opt/mssql-cdc-incremental.py --batch "${BATCH}"

echo "==> done. Now check, in order:"
echo "   1. Kafka Connect REST: connector status + consumer lag on nimbus-cdc-kafka topics"
echo "   2. RAW Iceberg row counts (via Polaris / StarRocks)"
echo "   3. SILVER Iceberg values for touched PKs"
echo "   4. silver_change_log rows (changelog-pg)"
echo "   5. Airflow gold_impact_trigger run history — only affected GOLD SparkApplications should fire"
echo "   6. StarRocks GOLD tables / Superset dashboards reflect the new values"
echo "   7. OpenMetadata lineage for RetailDB.dbo.sales_transactions picks up the new activity"
