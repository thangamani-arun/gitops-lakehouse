"""GOLD: customer_loyalty — joins SILVER customers with their sales_transactions history.
Column names verified directly against RetailDB (see plan's "Source system" section).
"""
import os

from pyspark.sql import SparkSession

# See sales_summary.py for why the Polaris credential is injected via env var here rather than
# static sparkConf in the SparkApplication CR (keeps the root credential out of git).
spark = (
    SparkSession.builder.appName("gold-customer-loyalty")
    .config("spark.sql.catalog.polaris.credential", f"{os.environ['POLARIS_CLIENT_ID']}:{os.environ['POLARIS_CLIENT_SECRET']}")
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.header.Polaris-Realm", "nimbus-lakehouse")
    .getOrCreate()
)

spark.sql("USE polaris")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS gold.customer_loyalty (
      customer_id INT,
      name STRING,
      loyalty_tier STRING,
      lifetime_spend DOUBLE,
      txn_count BIGINT,
      updated_at TIMESTAMP
    ) USING iceberg
    """
)

result = spark.sql(
    """
    SELECT
      c.customer_id,
      c.name,
      c.loyalty_tier,
      c.total_spend AS lifetime_spend,
      COUNT(t.txn_id) AS txn_count,
      current_timestamp() AS updated_at
    FROM silver.customers c
    LEFT JOIN silver.sales_transactions t ON t.customer_id = c.customer_id
    GROUP BY c.customer_id, c.name, c.loyalty_tier, c.total_spend
    """
)

result.createOrReplaceTempView("new_loyalty")
spark.sql(
    """
    MERGE INTO gold.customer_loyalty tgt
    USING new_loyalty src
    ON tgt.customer_id = src.customer_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

spark.stop()
