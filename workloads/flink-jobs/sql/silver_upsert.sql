-- RAW -> SILVER: re-read the same Kafka Debezium topics directly (not RAW Iceberg) so the
-- SILVER upsert is a clean typed merge-by-PK, keyed the way Iceberg's row-level UPDATE/DELETE
-- expects. RAW remains the append-only audit log; SILVER is the current-state, deduped view.
-- Column lists verified directly against RetailDB (see plan's "Source system" section).
-- After each checkpoint-committed micro-batch, a row is appended to the Postgres
-- silver_change_log table (see workloads/airflow) so Airflow can trigger only affected GOLD jobs.

CREATE CATALOG polaris WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
  'uri' = 'http://polaris.lakehouse-catalog.svc:8181/api/catalog',
  'warehouse' = 'nimbus-lakehouse',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'https://minio.nimbus.my:39090',
  's3.path-style-access' = 'true',
  's3.endpoint.signing-region' = 'us-east-1',
  'credential' = '${POLARIS_CLIENT_ID}:${POLARIS_CLIENT_SECRET}',
  'scope' = 'PRINCIPAL_ROLE:ALL',
  'header.Polaris-Realm' = 'nimbus-lakehouse',
  'oauth2-server-uri' = 'http://polaris.lakehouse-catalog.svc:8181/api/catalog/v1/oauth/tokens'
);

USE CATALOG polaris;
-- No CREATE DATABASE here -- see raw_ingest.sql's comment: the silver namespace is pre-created
-- via the Polaris REST API directly, sidestepping an Iceberg/Flink-vs-Polaris response-parsing bug.

CREATE TEMPORARY TABLE cdc_customers (
  customer_id INT, name STRING, email STRING, phone STRING, city STRING,
  loyalty_tier STRING, total_spend DECIMAL(12,2), registered_at STRING,
  created_at STRING, updated_at STRING,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'retaildb.dbo.customers',
  'properties.bootstrap.servers' = 'nimbus-cdc-kafka-kafka-bootstrap.lakehouse-streaming.svc:9092',
  'properties.group.id' = 'flink-silver-customers',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

CREATE TEMPORARY TABLE cdc_products (
  product_id INT, sku STRING, name STRING, category STRING, brand STRING,
  unit_price DECIMAL(12,2), cost_price DECIMAL(12,2), is_active BOOLEAN,
  created_at STRING, updated_at STRING,
  PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'retaildb.dbo.products',
  'properties.bootstrap.servers' = 'nimbus-cdc-kafka-kafka-bootstrap.lakehouse-streaming.svc:9092',
  'properties.group.id' = 'flink-silver-products',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

CREATE TEMPORARY TABLE cdc_sales_transactions (
  txn_id INT, store_id INT, product_id INT, customer_id INT, qty INT,
  unit_price DECIMAL(12,2), total_amount DECIMAL(12,2), status STRING,
  txn_at STRING, created_at STRING, updated_at STRING,
  PRIMARY KEY (txn_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'retaildb.dbo.sales_transactions',
  'properties.bootstrap.servers' = 'nimbus-cdc-kafka-kafka-bootstrap.lakehouse-streaming.svc:9092',
  'properties.group.id' = 'flink-silver-sales-transactions',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

CREATE TEMPORARY TABLE cdc_stores (
  store_id INT, code STRING, name STRING, city STRING, region STRING,
  is_active BOOLEAN, opened_date STRING, created_at STRING, updated_at STRING,
  PRIMARY KEY (store_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'retaildb.dbo.stores',
  'properties.bootstrap.servers' = 'nimbus-cdc-kafka-kafka-bootstrap.lakehouse-streaming.svc:9092',
  'properties.group.id' = 'flink-silver-stores',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

-- ── SILVER Iceberg upsert targets (merge-on-read, PK = equality field) ─────────────
CREATE TABLE IF NOT EXISTS silver.customers (
  customer_id INT, name STRING, email STRING, phone STRING, city STRING,
  loyalty_tier STRING, total_spend DECIMAL(12,2), registered_at STRING,
  created_at STRING, updated_at STRING,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

CREATE TABLE IF NOT EXISTS silver.products (
  product_id INT, sku STRING, name STRING, category STRING, brand STRING,
  unit_price DECIMAL(12,2), cost_price DECIMAL(12,2), is_active BOOLEAN,
  created_at STRING, updated_at STRING,
  PRIMARY KEY (product_id) NOT ENFORCED
) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

CREATE TABLE IF NOT EXISTS silver.sales_transactions (
  txn_id INT, store_id INT, product_id INT, customer_id INT, qty INT,
  unit_price DECIMAL(12,2), total_amount DECIMAL(12,2), status STRING,
  txn_at STRING, created_at STRING, updated_at STRING,
  PRIMARY KEY (txn_id) NOT ENFORCED
) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

CREATE TABLE IF NOT EXISTS silver.stores (
  store_id INT, code STRING, name STRING, city STRING, region STRING,
  is_active BOOLEAN, opened_date STRING, created_at STRING, updated_at STRING,
  PRIMARY KEY (store_id) NOT ENFORCED
) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

-- ── change/impact log sink: one JDBC upsert per micro-batch per table ──────────────
CREATE TEMPORARY TABLE silver_change_log (
  table_name STRING,
  affected_keys STRING,
  committed_at TIMESTAMP(3)
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://changelog-pg-rw.lakehouse-orchestration.svc:5432/changelog',
  'table-name' = 'silver_change_log',
  'username' = '${CHANGELOG_PG_USERNAME}',
  'password' = '${CHANGELOG_PG_PASSWORD}'
);

-- Separate Kafka source tables (distinct group.id) for the change-log inserts below, not a
-- reuse of cdc_customers etc. -- since each INSERT here is now its own independently-submitted
-- Flink job (see comment above), reusing the same group.id would make two unrelated jobs join
-- the same Kafka consumer group and split partitions between them, each seeing only a subset.
CREATE TEMPORARY TABLE cdc_customers_cl LIKE cdc_customers (EXCLUDING OPTIONS)
  WITH ('properties.group.id' = 'flink-silver-customers-changelog');
CREATE TEMPORARY TABLE cdc_products_cl LIKE cdc_products (EXCLUDING OPTIONS)
  WITH ('properties.group.id' = 'flink-silver-products-changelog');
CREATE TEMPORARY TABLE cdc_sales_transactions_cl LIKE cdc_sales_transactions (EXCLUDING OPTIONS)
  WITH ('properties.group.id' = 'flink-silver-sales-transactions-changelog');
CREATE TEMPORARY TABLE cdc_stores_cl LIKE cdc_stores (EXCLUDING OPTIONS)
  WITH ('properties.group.id' = 'flink-silver-stores-changelog');

-- Submitted as separate INSERT statements, not one EXECUTE STATEMENT SET -- see raw_ingest.sql's
-- comment: a STATEMENT SET's concurrent target-table resolution races Polaris's REST auth
-- session (reproduced identically on iceberg-flink-runtime 1.6.1 and 1.10.2). Trade-off: N
-- independent jobs/checkpoints instead of one shared one.
INSERT INTO silver.customers SELECT * FROM cdc_customers;
INSERT INTO silver.products SELECT * FROM cdc_products;
INSERT INTO silver.sales_transactions SELECT * FROM cdc_sales_transactions;
INSERT INTO silver.stores SELECT * FROM cdc_stores;

INSERT INTO silver_change_log
  SELECT 'customers', CAST(customer_id AS STRING), CURRENT_TIMESTAMP FROM cdc_customers_cl;
INSERT INTO silver_change_log
  SELECT 'products', CAST(product_id AS STRING), CURRENT_TIMESTAMP FROM cdc_products_cl;
INSERT INTO silver_change_log
  SELECT 'sales_transactions', CAST(txn_id AS STRING), CURRENT_TIMESTAMP FROM cdc_sales_transactions_cl;
INSERT INTO silver_change_log
  SELECT 'stores', CAST(store_id AS STRING), CURRENT_TIMESTAMP FROM cdc_stores_cl;
