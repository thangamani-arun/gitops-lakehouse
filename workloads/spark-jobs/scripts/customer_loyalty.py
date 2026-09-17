"""GOLD: customer_loyalty — joins SILVER customers with their sales_transactions history."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("gold-customer-loyalty").getOrCreate()

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
      c.spend AS lifetime_spend,
      COUNT(t.txn_id) AS txn_count,
      current_timestamp() AS updated_at
    FROM silver.customers c
    LEFT JOIN silver.sales_transactions t ON t.customer_id = c.customer_id
    GROUP BY c.customer_id, c.name, c.loyalty_tier, c.spend
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
