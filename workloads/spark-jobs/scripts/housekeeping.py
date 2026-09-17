"""Daily catalog housekeeping: expire old snapshots + remove orphan files on every
RAW/SILVER/GOLD table. Triggered by the catalog_housekeeping Airflow DAG.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("catalog-housekeeping").getOrCreate()
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
