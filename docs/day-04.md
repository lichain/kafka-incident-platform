# Day 04 — Idempotent Consumer

Kafka consumers commonly use at-least-once delivery: a record may be handled successfully but delivered again if the process stops before its offset is committed. The consumer must therefore make duplicate processing safe.

This lab records each processed `EventId` in SQLite. `event_id` is the primary key, so only the first attempt inserts a row. Later attempts are logged as `duplicate` and skipped.

## Run without committing offsets

```powershell
dotnet run --project src/IncidentDetector -- --group day-04-idempotency --commit none --idempotency sqlite
```

Let it handle a few events and stop it with `Ctrl+C`. Run the exact same command again.

The same Kafka offsets replay because no offset was committed, but their `EventId` values already exist in `data/processed-events.db`, so the consumer prints `duplicate ... skipped`.

## Important production rule

In a real Incident service, saving `ProcessedEvent` and writing the Incident must occur in the same database transaction. If they are separate operations, a failure between them can mark an event as processed without applying its business effect.
