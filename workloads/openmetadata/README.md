# OpenMetadata — Day-0 note

All 5 ingestion CronJobs (`ingestion/`) reference a `om-bot-token` Secret (`OM_BOT_TOKEN` key)
that doesn't exist until after OpenMetadata's first boot: it's a bot JWT generated from the
running OpenMetadata instance (Settings → Bots → ingestion-bot → generate token), the same
category of Day-0 manual step as `mssql-credentials` — create it once, out-of-band:

```
kubectl create secret generic om-bot-token -n lakehouse-governance \
  --from-literal=OM_BOT_TOKEN=<token from the OpenMetadata UI>
```

The ingestion CronJobs will fail until this exists; that's expected on a fresh install and is
one of the Post-Deployment "Catalog & lineage management" checks (see the plan).
