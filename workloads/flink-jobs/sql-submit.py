"""Submits a mounted .sql file to a co-located Flink SQL Gateway, statement by statement.

Exists because the FlinkDeployments run stock flink:1.19 (no custom sql-runner image -- see
README.md for why that approach was dropped) and instead expose the SQL Gateway as a sidecar
on the JobManager pod. This script is the client: it opens a gateway session, splits the SQL
file into individual statements (treating an `EXECUTE STATEMENT SET ... END;` block as one
atomic statement, since it contains internal semicolons), and submits each in order.

The final STATEMENT SET is an unbounded streaming job -- it never reaches FINISHED, so it's
only checked for an immediate ERROR and then left running in the background, unlike the DDL
statements before it which are waited on to completion.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

GATEWAY = os.environ["GATEWAY_URL"].rstrip("/")
SQL_FILE = os.environ["SQL_FILE"]


def _request(path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        GATEWAY + path, data=data, method="POST" if data is not None else "GET",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=90) as r:
        return json.loads(r.read())


def split_statements(text):
    statements, buf, in_set = [], [], False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        buf.append(line)
        if stripped.upper().startswith("EXECUTE STATEMENT SET"):
            in_set = True
            continue
        if in_set:
            if stripped.upper() == "END;":
                statements.append(("\n".join(buf), True))  # (sql, is_streaming)
                buf, in_set = [], False
            continue
        if stripped.endswith(";"):
            # A bare INSERT INTO ... SELECT (reading an unbounded Kafka source) is itself a
            # streaming job that never reaches FINISHED, same as a STATEMENT SET block.
            is_insert = "\n".join(buf).strip().upper().startswith("INSERT INTO")
            statements.append(("\n".join(buf), is_insert))
            buf = []
    if buf:
        statements.append(("\n".join(buf), False))
    return statements


for _ in range(60):
    try:
        _request("/v1/info")
        break
    except Exception:
        time.sleep(5)
else:
    sys.exit("gateway never became ready")

session = _request("/v1/sessions", {})["sessionHandle"]
print("session:", session, flush=True)

with open(SQL_FILE) as f:
    text = f.read()

# ${VAR_NAME} placeholders in the .sql file are substituted from this pod's own env (Polaris
# and changelog-pg credentials, injected via secretKeyRef -- see the Job spec) so no credential
# ever lands in git in plaintext, matching the Spark jobs' env-var-injected credential pattern.
for key, value in os.environ.items():
    text = text.replace("${" + key + "}", value)

statements = split_statements(text)

for i, (stmt, is_streaming) in enumerate(statements):
    print(f"--- statement {i + 1}/{len(statements)} ---\n{stmt}", flush=True)
    op_handle = _request(f"/v1/sessions/{session}/statements", {"statement": stmt})["operationHandle"]

    if is_streaming:
        time.sleep(10)
        status = _request(f"/v1/sessions/{session}/operations/{op_handle}/status")["status"]
        if status == "ERROR":
            try:
                result = _request(f"/v1/sessions/{session}/operations/{op_handle}/result/0")
                print("ERROR result:", result, flush=True)
            except Exception as e:
                print("could not fetch error result:", e, flush=True)
            sys.exit(f"streaming statement failed immediately: {status}")
        print(f"statement {i + 1} -> {status} (streaming job left running)", flush=True)
        continue

    while True:
        status = _request(f"/v1/sessions/{session}/operations/{op_handle}/status")["status"]
        if status in ("FINISHED", "ERROR", "CANCELED"):
            break
        time.sleep(2)
    if status != "FINISHED":
        try:
            result = _request(f"/v1/sessions/{session}/operations/{op_handle}/result/0")
            print("ERROR result:", result, flush=True)
        except Exception as e:
            print("could not fetch error result:", e, flush=True)
        if "IF NOT EXISTS" in stmt.upper():
            # This iceberg-flink-runtime version's ignoreIfExists handling is broken against
            # Polaris's REST responses for an entity that already exists (confirmed: a fresh
            # namespace/table create succeeds, a repeat one throws instead of no-op'ing) --
            # treat as non-fatal so redeploys/reruns stay idempotent despite the library bug.
            print(f"statement {i + 1} failed but is IF NOT EXISTS -- treating as already-exists, continuing", flush=True)
            continue
        sys.exit(f"statement {i + 1} failed: {status}")
    print(f"statement {i + 1} -> {status}", flush=True)

print("all statements submitted successfully", flush=True)
