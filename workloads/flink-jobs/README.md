# Flink jobs — SQL Gateway approach

Originally this used a custom image with a hand-built `sql-runner.jar` (the
[ververica sql-runner-example](https://github.com/ververica/flink-sql-runner-example) pattern).
That jar was never actually built/pushed, so both FlinkDeployments could never start -- this was
found and fixed during Day-0. Replaced with the approach flink:1.19 ships out of the box:

- Both `FlinkDeployment`s run in **session mode** (no `job:` field) on the stock
  `flink:1.19-scala_2.12-java17` image -- no custom image or registry needed.
- An `initContainer` (same stock image) copies the image's own `/opt/flink/lib` into a shared
  `emptyDir`, then downloads the Kafka/Iceberg/JDBC/S3/Postgres connector jars into it. That
  `emptyDir` is mounted over `/opt/flink/lib` in the jobmanager, taskmanager, and sql-gateway
  containers, so every process sees the same, complete classpath (mounting a plain `emptyDir`
  straight over `/opt/flink/lib` without first copying the originals would hide the bundled
  Flink jars and the cluster wouldn't start).
- The Flink SQL Gateway (`bin/sql-gateway.sh`, bundled in every Flink 1.19 image) runs as a
  sidecar container on the **jobmanager** pod only (`jobManager.podTemplate`), listening on
  port 8083, submitting jobs to the co-located JobManager over `localhost:8081`. A plain
  `Service` (`*-sql-gateway`) exposes it.
- `sql-submit.py` (mounted via ConfigMap into a one-shot `Job`, sync-wave after the
  `FlinkDeployment`) opens a gateway session and submits `sql/raw_ingest.sql` /
  `sql/silver_upsert.sql` statement-by-statement, waiting for each DDL statement to finish but
  treating the final `EXECUTE STATEMENT SET` as a fire-and-forget streaming job (it never
  reaches `FINISHED` by design). `${VAR_NAME}` placeholders in the `.sql` files (Polaris
  OAuth credential, changelog-pg username/password) are substituted from the Job's own env
  (`secretKeyRef`s), so no credential is ever committed to git.
