# Day 08 — Batch Consumer

Batching amortizes offset commit and per-batch overhead. The Consumer collects up to `--batch-size` records, handles every record, then commits the highest successfully handled offset **for each partition**.

## Run a batch Consumer

```powershell
dotnet run --project src/IncidentDetector -- `
  --group day-08-batch `
  --batch-size 50
```

The output prints `processing batch: ...`. A smaller final batch is normal when the fetch timeout expires before the batch fills.

## Safety rule

Never commit a batch until every record in that batch has completed its required work. If batch item 37 fails, committing the offset for item 50 would skip 37–49 after restart.

The lab commits one highest offset per partition, not a single global offset, because Kafka offsets are partition-scoped.
