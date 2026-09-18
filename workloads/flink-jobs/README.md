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
  treating each `INSERT INTO ...` as a fire-and-forget streaming job (it never reaches
  `FINISHED` by design). `${VAR_NAME}` placeholders in the `.sql` files (Polaris OAuth
  credential, changelog-pg username/password) are substituted from the Job's own env
  (`secretKeyRef`s), so no credential is ever committed to git.

## KNOWN ISSUE (Day-0, unresolved) — RAW/SILVER ingestion doesn't actually run

`flink-sql-submit-raw`/`-silver` are currently `suspend: true` in `sql-submit-jobs.yaml`. All the
DDL (`CREATE CATALOG`/`CREATE TABLE`) succeeds, but the very first `INSERT INTO ...` fails:
Polaris returns `401 Not authorized` on the `loadTable` call the Iceberg REST client makes while
resolving the INSERT's target table -- even for one single, non-concurrent statement, using the
exact same token/session that just succeeded moments earlier on the exact same table.

Investigated and ruled out:
- **Iceberg client version**: identical failure on `iceberg-flink-runtime` 1.6.1 and 1.10.2
  (latest available).
- **STATEMENT SET concurrency**: originally suspected Flink's planner resolving a multi-table
  `STATEMENT SET` concurrently; splitting into separate single-table `INSERT` statements did not
  fix it -- a lone `INSERT` fails the same way.
- **HTTP keep-alive/connection reuse**: adding `header.Connection = close` to force a fresh
  connection per request made no difference.
- **Polaris version**: already on 1.7.0, the latest release as of Day-0.

What Polaris's access log shows: every *failing* request lands on a Vert.x **event-loop** thread
with no authenticated user (`- -`); every *succeeding* request (including the CREATE TABLE for
the identical table moments before) lands on a worker **executor-thread** as `root`. This points
to a Polaris-side bug where its auth filter isn't applied consistently for some
reactive-routed requests, not a Flink/Iceberg-client bug -- though the trigger hasn't been
pinned down further (a `GET /namespaces/polaris` 404 -- the client apparently checking for a
namespace named after the catalog itself -- shows up right before every failure, but isn't
confirmed as the cause).

Spark's GOLD `SparkApplication`s authenticate to and write into the same Polaris catalog
successfully, so this is specific to Flink's REST client / request pattern, not Polaris auth as
a whole -- **update**: also reproduced independently on StarRocks's external catalog
(`workloads/starrocks/external-catalog-init.yaml`), a completely different client codebase, with
the identical signature (failing `GET` on a Vert.x event-loop thread, no authenticated user).
Two unrelated clients hitting the same symptom, while Spark's REST client doesn't, is strong
evidence this is a genuine Polaris server-side bug (something about the request pattern/timing
those two clients share and Spark's doesn't), not a Flink- or StarRocks-specific issue. Next
step, if picked back up: file an upstream Polaris issue with this exact repro (single
GET/INSERT-resolution call, immediately after a successful call in the same session), or dig
into what Spark's iceberg-spark-runtime HTTP client does differently (connection pooling/
threading model) that avoids triggering it.
