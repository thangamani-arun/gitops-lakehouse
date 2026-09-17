"""
Daily Iceberg catalog housekeeping: snapshot expiration + orphan-file cleanup on every
RAW/SILVER/GOLD table, run via the existing Spark Operator (SparkApplication) rather than a
new execution engine. See plan's "Catalog & lineage management" section.
"""
from __future__ import annotations

import datetime

from airflow.decorators import dag, task
import subprocess

NAMESPACE = "lakehouse-orchestration"
TABLES = [
    "raw.customers", "raw.products", "raw.sales_transactions", "raw.stores",
    "silver.customers", "silver.products", "silver.sales_transactions", "silver.stores",
    "gold.sales_summary", "gold.customer_loyalty",
]


@dag(
    dag_id="catalog_housekeeping",
    schedule="0 3 * * *",
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "catalog", "maintenance"],
)
def catalog_housekeeping():
    @task
    def run_housekeeping_job() -> None:
        # Re-applies the housekeeping SparkApplication (workloads/spark-jobs/housekeeping.yaml),
        # which iterates TABLES doing expire_snapshots + remove_orphan_files via Iceberg's Spark
        # procedures. See workloads/spark-jobs for the actual Spark app.
        subprocess.run(
            ["kubectl", "-n", NAMESPACE, "delete", "sparkapplication", "catalog-housekeeping", "--ignore-not-found"],
            check=True,
        )
        subprocess.run(["kubectl", "apply", "-f", "/spark-jobs/housekeeping.yaml"], check=True)

    run_housekeeping_job()


catalog_housekeeping()
