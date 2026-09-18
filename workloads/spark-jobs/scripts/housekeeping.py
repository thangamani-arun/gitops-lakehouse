"""Daily catalog housekeeping: expire old snapshots + remove orphan files on every
RAW/SILVER/GOLD table. Triggered by the catalog_housekeeping Airflow DAG.
"""
import os

from pyspark.sql import SparkSession

# See sales_summary.py for why the Polaris credential is injected via env var here rather than
# static sparkConf in the SparkApplication CR (keeps the root credential out of git).
spark = (
    SparkSession.builder.appName("catalog-housekeeping")
    .config("spark.sql.catalog.polaris.credential", f"{os.environ['POLARIS_CLIENT_ID']}:{os.environ['POLARIS_CLIENT_SECRET']}")
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.header.Polaris-Realm", "nimbus-lakehouse")
    .getOrCreate()
)
spark.sql("USE polaris")

TABLES = [
    "raw.customers", "raw.products", "raw.sales_transactions", "raw.stores",
    "silver.customers", "silver.products", "silver.sales_transactions", "silver.stores",
    "gold.sales_summary", "gold.customer_loyalty",
]

for table in TABLES:
    print(f"housekeeping: {table}")
    try:
        spark.sql(f"CALL polaris.system.expire_snapshots(table => '{table}', older_than => TIMESTAMP '2100-01-01 00:00:00', retain_last => 10)")
    except Exception as e:  # noqa: BLE001 — table may not exist yet on first run, keep going
        print(f"  expire_snapshots skipped for {table}: {e}")
    try:
        spark.sql(f"CALL polaris.system.remove_orphan_files(table => '{table}')")
    except Exception as e:  # noqa: BLE001
        print(f"  remove_orphan_files skipped for {table}: {e}")

spark.stop()
