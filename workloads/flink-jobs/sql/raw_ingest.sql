-- Kafka (Debezium JSON) -> RAW Iceberg, append-only, one INSERT per RetailDB table.
-- Column lists verified directly against RetailDB (see plan's "Source system" section).
-- Executed as a single STATEMENT SET so all 4 pipelines run in one Flink job/checkpoint group.

CREATE CATALOG polaris WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
  'uri' = 'http://polaris.lakehouse-catalog.svc:8181/api/catalog',
  'warehouse' = 'nimbus-lakehouse',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'https://minio.nimbus.my:39090',
  's3.path-style-access' = 'true',
  's3.endpoint.signing-region' = 'us-east-1',
  -- ${POLARIS_CLIENT_ID}/${POLARIS_CLIENT_SECRET} are substituted at submission time by
  -- sql-submit.py from the polaris-catalog-credentials Secret, never committed in plaintext.
  'credential' = '${POLARIS_CLIENT_ID}:${POLARIS_CLIENT_SECRET}',
  'scope' = 'PRINCIPAL_ROLE:ALL',
  'header.Polaris-Realm' = 'nimbus-lakehouse',
  'oauth2-server-uri' = 'http://polaris.lakehouse-catalog.svc:8181/api/catalog/v1/oauth/tokens',
  'header.Connection' = 'close'
);

USE CATALOG polaris;
-- No CREATE DATABASE here: this iceberg-flink-runtime version's RESTSessionCatalog.createNamespace
-- crashes parsing Polaris's response (MismatchedInputException on an empty body) even with
-- ignoreIfExists=true, a client/server response-format incompatibility, not a real conflict.
-- The raw/silver/gold namespaces are pre-created directly via the Polaris REST API instead
-- (see the Day-0 catalog bootstrap notes) -- sidesteps the bug rather than working around it here.

-- ── Kafka source tables (Debezium envelope) ─────────────────────────────────────────
CREATE TEMPORARY TABLE src_customers (
  op STRING,
  before ROW<customer_id INT, name STRING, email STRING, phone STRING, city STRING,
             loyalty_tier STRING, total_spend DECIMAL(12,2), registered_at STRING,
             created_at STRING, updated_at STRING>,
  after  ROW<customer_id INT, name STRING, email STRING, phone STRING, city STRING,
             loyalty_tier STRING, total_spend DECIMAL(12,2), registered_at STRING,
             created_at STRING, updated_at STRING>,
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
  before ROW<product_id INT, sku STRING, name STRING, category STRING, brand STRING,
             unit_price DECIMAL(12,2), cost_price DECIMAL(12,2), is_active BOOLEAN,
             created_at STRING, updated_at STRING>,
  after  ROW<product_id INT, sku STRING, name STRING, category STRING, brand STRING,
             unit_price DECIMAL(12,2), cost_price DECIMAL(12,2), is_active BOOLEAN,
             created_at STRING, updated_at STRING>,
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
  before ROW<txn_id INT, store_id INT, product_id INT, customer_id INT, qty INT,
             unit_price DECIMAL(12,2), total_amount DECIMAL(12,2), status STRING,
             txn_at STRING, created_at STRING, updated_at STRING>,
  after  ROW<txn_id INT, store_id INT, product_id INT, customer_id INT, qty INT,
             unit_price DECIMAL(12,2), total_amount DECIMAL(12,2), status STRING,
             txn_at STRING, created_at STRING, updated_at STRING>,
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
  before ROW<store_id INT, code STRING, name STRING, city STRING, region STRING,
             is_active BOOLEAN, opened_date STRING, created_at STRING, updated_at STRING>,
  after  ROW<store_id INT, code STRING, name STRING, city STRING, region STRING,
             is_active BOOLEAN, opened_date STRING, created_at STRING, updated_at STRING>,
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
-- No PARTITIONED BY: Flink SQL's clause only accepts plain column references, not Iceberg
-- transform functions like days(...) (that's Spark-only syntax) -- confirmed via a parser
-- error ("Encountered '(' ") when this was first written with `PARTITIONED BY (days(ingested_at))`.
-- Not partitioning RAW is fine for Day-0; it's an optimization, not a correctness requirement.
CREATE TABLE IF NOT EXISTS `raw`.customers (
  op STRING, before_json STRING, after_json STRING, source_ts_ms BIGINT, ingested_at TIMESTAMP(3)
) WITH ('format-version' = '2');

CREATE TABLE IF NOT EXISTS `raw`.products LIKE `raw`.customers;
CREATE TABLE IF NOT EXISTS `raw`.sales_transactions LIKE `raw`.customers;
CREATE TABLE IF NOT EXISTS `raw`.stores LIKE `raw`.customers;

-- Submitted as 4 separate INSERT statements (not one EXECUTE STATEMENT SET) -- Flink's planner
-- resolves a STATEMENT SET's INSERT targets via a stream that issues concurrent loadTable calls
-- to Polaris, and only some of those concurrent requests carry the Authorization header
-- (confirmed via Polaris's access log: the failing calls land on a different server thread than
-- the succeeding ones, ~300ms apart, on the identical resource) -- a client-side auth-session
-- thread-safety bug, reproduced identically on iceberg-flink-runtime 1.6.1 and 1.10.2 (latest).
-- Separate statements resolve their target table sequentially, avoiding the race. Trade-off:
-- 4 independent jobs/checkpoints instead of one shared one -- acceptable for Day-0.
INSERT INTO `raw`.customers
  SELECT op, CAST(before AS STRING), CAST(after AS STRING), source.ts_ms, CURRENT_TIMESTAMP FROM src_customers;
INSERT INTO `raw`.products
  SELECT op, CAST(before AS STRING), CAST(after AS STRING), source.ts_ms, CURRENT_TIMESTAMP FROM src_products;
INSERT INTO `raw`.sales_transactions
  SELECT op, CAST(before AS STRING), CAST(after AS STRING), source.ts_ms, CURRENT_TIMESTAMP FROM src_sales_transactions;
INSERT INTO `raw`.stores
  SELECT op, CAST(before AS STRING), CAST(after AS STRING), source.ts_ms, CURRENT_TIMESTAMP FROM src_stores;
