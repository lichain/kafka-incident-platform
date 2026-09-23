# Day 09 — Poison messages and DLQ

A poison message is a record that repeatedly fails processing. Leaving it uncommitted blocks progress for that partition; committing it without recording the failure loses data.

This lab simulates a failure whenever `Level` is `Error`. The Consumer wraps the original key, payload, topic, partition, offset and failure reason in a `DeadLetterEvent`, publishes it to `app-events.dlq`, and commits the original record only after that publish succeeds.

## Run

Start a fresh group:

```powershell
dotnet run --project src/IncidentDetector -- `
  --group day-09-dlq `
  --failure-mode dlq-on-error
```

Start a Producer in another terminal. Every fourth generated event has `Level = Error`, so the Consumer prints `sent to DLQ` roughly once per four records.

```powershell
dotnet run --project src/EventGenerator -- --key service
```

Inspect `app-events.dlq` in Kafka UI. The topic is auto-created by this local broker when the first failed event is published.

## Safety rule

Commit the original record only after the DLQ publish succeeds. Otherwise a transient DLQ outage can turn a recoverable failure into lost data.
