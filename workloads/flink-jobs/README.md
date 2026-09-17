# Flink jobs — build note

`Dockerfile` expects `sql-runner/target/sql-runner.jar` — a thin `TableEnvironment` runner that
executes a mounted `.sql` file, following the community pattern at
https://github.com/ververica/flink-sql-runner-example. That jar isn't vendored in this repo yet;
before the first `docker build`, either:

1. Clone the ververica example, build it with Maven, and drop the resulting jar at
   `sql-runner/target/sql-runner.jar`, or
2. Swap the approach for the Flink SQL Gateway (`flink:1.19` already ships it) and submit
   `sql/raw_ingest.sql` / `sql/silver_upsert.sql` via the gateway's REST API from an init
   container instead of a custom jar.

Either way, build and push the image (e.g. `<registry>/nimbus-lakehouse/flink-sql-runner:1.0.0`)
and update `image:` in `flinkdeployment-raw.yaml` / `flinkdeployment-silver.yaml` accordingly —
they currently reference that tag as a placeholder.
