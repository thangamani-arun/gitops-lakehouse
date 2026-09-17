"""
Impact-based GOLD scheduling.

Every 2 minutes, diff `silver_change_log` (written by the Flink RAW->SILVER job after each
committed micro-batch) against the static `gold_dependency_graph`, and trigger only the
SparkApplication(s) for GOLD tables whose SILVER dependencies actually changed since the last
run -- not a full refresh of every GOLD table on every tick.

KNOWN FOLLOW-UPS before this runs for real (flagging rather than guessing at exact values):
  - The KubernetesExecutor worker pod needs `kubectl` on PATH and a ServiceAccount with
    get/delete/create on `sparkapplications.sparkoperator.k8s.io` in lakehouse-orchestration
    (RBAC not yet added here -- add alongside workloads/spark-jobs).
  - `/spark-jobs/*.yaml` must be mounted into the worker pod (e.g. via a pod_template_file
    volume referencing the spark-jobs ConfigMap/git-sync) -- not yet wired.
  - `changelog_pg` must exist as an Airflow connection (Postgres, pointed at
    changelog-pg-rw.lakehouse-orchestration.svc:5432/changelog) -- add via the chart's
    `connections` value or the changelog-pg-app secret, not hardcoded here.
"""
from __future__ import annotations

import datetime

import subprocess

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

CHANGELOG_CONN_ID = "changelog_pg"
NAMESPACE = "lakehouse-orchestration"


def _get_last_run_watermark(ti) -> datetime.datetime:
    prev = ti.xcom_pull(key="last_watermark", include_prior_dates=True)
    return prev or datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


@dag(
    dag_id="gold_impact_trigger",
    schedule="*/2 * * * *",
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "gold", "impact-scheduling"],
)
def gold_impact_trigger():
    @task
    def find_affected_gold_tables(**context) -> list[str]:
        ti = context["ti"]
        watermark = _get_last_run_watermark(ti)
        hook = PostgresHook(postgres_conn_id=CHANGELOG_CONN_ID)
        changed_silver_tables = {
            row[0]
            for row in hook.get_records(
                "SELECT DISTINCT table_name FROM silver_change_log WHERE committed_at > %s",
                parameters=(watermark,),
            )
        }
        if not changed_silver_tables:
            return []

        affected_gold = {
            row[0]
            for row in hook.get_records(
                "SELECT DISTINCT gold_table FROM gold_dependency_graph "
                "WHERE depends_on_silver_table = ANY(%s)",
                parameters=(list(changed_silver_tables),),
            )
        }
        ti.xcom_push(key="last_watermark", value=datetime.datetime.now(datetime.timezone.utc))
        return sorted(affected_gold)

    @task
    def trigger_gold_spark_jobs(affected_tables: list[str]) -> None:
        # SparkApplication CRs live under workloads/spark-jobs; each GOLD table's manifest is
        # re-applied (delete+apply, since SparkApplication doesn't support simple "rerun") only
        # for the tables actually affected by this batch of SILVER changes.
        for gold_table in affected_tables:
            name = gold_table.split(".")[-1].replace("_", "-")
            manifest_path = f"/spark-jobs/{gold_table.split('.')[-1]}.yaml"
            subprocess.run(
                ["kubectl", "-n", NAMESPACE, "delete", "sparkapplication", name, "--ignore-not-found"],
                check=True,
            )
            subprocess.run(["kubectl", "apply", "-f", manifest_path], check=True)

    trigger_gold_spark_jobs(find_affected_gold_tables())


gold_impact_trigger()
