-- Kafka (Debezium JSON) -> RAW Iceberg, append-only, one INSERT per RetailDB table.
-- Executed as a single STATEMENT SET so all 4 pipelines run in one Flink job/checkpoint group.

CREATE CATALOG polaris WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
  'uri' = 'http://polaris.lakehouse-catalog.svc:8181/api/catalog',
  'warehouse' = 'nimbus-lakehouse',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'https://192.168.80.128:39090',
  's3.path-style-access' = 'true',
  's3.endpoint.signing-region' = 'us-east-1'
);

USE CATALOG polaris;
CREATE DATABASE IF NOT EXISTS raw;

-- ── Kafka source tables (Debezium envelope) ─────────────────────────────────────────
CREATE TEMPORARY TABLE src_customers (
  op STRING,
  before ROW<customer_id INT, name STRING, city STRING, phone STRING, spend DOUBLE, loyalty_tier STRING>,
  after  ROW<customer_id INT, name STRING, city STRING, phone STRING, spend DOUBLE, loyalty_tier STRING>,
  source ROW<ts_ms BIGINT>,
  ts_ms BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'retaildb.dbo.customers',
  'properties.bootstrap.servers' = 'nimbus-cdc-kafka-kafka-bootstrap.lakehouse-streaming.svc:9092',
  'properties.group.id' = 'flink-raw-customers',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE src_products (
  op STRING,
  before ROW<product_id INT, name STRING, category STRING, price DOUBLE>,
  after  ROW<product_id INT, name STRING, category STRING, price DOUBLE>,
  source ROW<ts_ms BIGINT>,
  ts_ms BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'retaildb.dbo.products',
  'properties.bootstrap.servers' = 'nimbus-cdc-kafka-kafka-bootstrap.lakehouse-streaming.svc:9092',
  'properties.group.id' = 'flink-raw-products',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE src_sales_transactions (
  op STRING,
  before ROW<txn_id INT, customer_id INT, product_id INT, store_id INT, qty INT, amount DOUBLE, status STRING, txn_ts BIGINT>,
  after  ROW<txn_id INT, customer_id INT, product_id INT, store_id INT, qty INT, amount DOUBLE, status STRING, txn_ts BIGINT>,
  source ROW<ts_ms BIGINT>,
  ts_ms BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'retaildb.dbo.sales_transactions',
  'properties.bootstrap.servers' = 'nimbus-cdc-kafka-kafka-bootstrap.lakehouse-streaming.svc:9092',
  'properties.group.id' = 'flink-raw-sales-transactions',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE src_stores (
  op STRING,
  before ROW<store_id INT, name STRING, city STRING>,
  after  ROW<store_id INT, name STRING, city STRING>,
  source ROW<ts_ms BIGINT>,
  ts_ms BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'retaildb.dbo.stores',
  'properties.bootstrap.servers' = 'nimbus-cdc-kafka-kafka-bootstrap.lakehouse-streaming.svc:9092',
  'properties.group.id' = 'flink-raw-stores',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- ── RAW Iceberg sinks (append-only CDC envelope, per plan's RAW convention) ────────
CREATE TABLE IF NOT EXISTS raw.customers (
  op STRING, before_json STRING, after_json STRING, source_ts_ms BIGINT, ingested_at TIMESTAMP(3)
) PARTITIONED BY (days(ingested_at)) WITH ('format-version' = '2');

CREATE TABLE IF NOT EXISTS raw.products LIKE raw.customers;
CREATE TABLE IF NOT EXISTS raw.sales_transactions LIKE raw.customers;
CREATE TABLE IF NOT EXISTS raw.stores LIKE raw.customers;

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO raw.customers
    SELECT op, CAST(before AS STRING), CAST(after AS STRING), source.ts_ms, CURRENT_TIMESTAMP FROM src_customers;
  INSERT INTO raw.products
    SELECT op, CAST(before AS STRING), CAST(after AS STRING), source.ts_ms, CURRENT_TIMESTAMP FROM src_products;
  INSERT INTO raw.sales_transactions
    SELECT op, CAST(before AS STRING), CAST(after AS STRING), source.ts_ms, CURRENT_TIMESTAMP FROM src_sales_transactions;
  INSERT INTO raw.stores
    SELECT op, CAST(before AS STRING), CAST(after AS STRING), source.ts_ms, CURRENT_TIMESTAMP FROM src_stores;
END;
