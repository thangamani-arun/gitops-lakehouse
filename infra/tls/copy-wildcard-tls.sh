#!/usr/bin/env bash
# Day-0 step: copy the existing *.nimbus.my wildcard TLS secret into each new
# lakehouse-* namespace. Rancher's project-scoped secret sync (field.cattle.io/projectId,
# see infra/namespaces/namespaces.yaml) should pick these namespaces up automatically since
# they're annotated into project local:p-xxdfk, but this script is the explicit fallback so
# Day-0 doesn't depend on Rancher's sync timing.
set -euo pipefail

SOURCE_NS="fusionx-kafka"
SECRET_NAME="wildcard-nimbus-tls"
TARGET_NAMESPACES=(
  lakehouse-streaming
  lakehouse-catalog
  lakehouse-orchestration
  lakehouse-compute
  lakehouse-governance
  lakehouse-bi
  argocd
)

for ns in "${TARGET_NAMESPACES[@]}"; do
  if kubectl get secret "$SECRET_NAME" -n "$ns" >/dev/null 2>&1; then
    echo "==> $SECRET_NAME already present in $ns, skipping"
    continue
  fi
  echo "==> copying $SECRET_NAME into $ns"
  kubectl get secret "$SECRET_NAME" -n "$SOURCE_NS" -o json \
    | jq 'del(.metadata.namespace, .metadata.resourceVersion, .metadata.uid, .metadata.creationTimestamp, .metadata.ownerReferences)' \
    | jq --arg ns "$ns" '.metadata.namespace = $ns' \
    | kubectl apply -f -
done
