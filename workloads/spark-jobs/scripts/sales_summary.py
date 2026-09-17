"""GOLD: sales_summary — aggregates SILVER sales_transactions/products/stores.
Triggered selectively by the gold_impact_trigger Airflow DAG when any of its SILVER
dependencies changes (see workloads/airflow/dags/gold_trigger_dag.py + gold_dependency_graph).
Column names verified directly against RetailDB (see plan's "Source system" section).
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("gold-sales-summary").getOrCreate()

spark.sql("USE polaris")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS gold.sales_summary (
      store_id INT,
      product_id INT,
      total_qty BIGINT,
      total_amount DOUBLE,
      txn_count BIGINT,
      updated_at TIMESTAMP
    ) USING iceberg
    """
)

result = spark.sql(
    """
    SELECT
      t.store_id,
      t.product_id,
      SUM(t.qty)          AS total_qty,
      SUM(t.total_amount)  AS total_amount,
      COUNT(*)             AS txn_count,
      current_timestamp()  AS updated_at
    FROM silver.sales_transactions t
    WHERE t.status = 'completed'
    GROUP BY t.store_id, t.product_id
    """
)

result.createOrReplaceTempView("new_summary")
spark.sql(
    """
    MERGE INTO gold.sales_summary tgt
    USING new_summary src
    ON tgt.store_id = src.store_id AND tgt.product_id = src.product_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

spark.stop()
