# IncidentProjection

This service consumes `incident-events` and builds a SQLite read model.

## Normal mode

Continue from the committed offset of the stable consumer group:

```powershell
dotnet run --project src/IncidentProjection `
  --no-launch-profile `
  --urls http://localhost:5203
```

## Rebuild mode

Delete only the local read model and processed-message records, then use a new
consumer group to replay `incident-events` from the earliest available offset:

```powershell
dotnet run --project src/IncidentProjection `
  --no-launch-profile `
  --urls http://localhost:5203 `
  -- --rebuild
```

Run only one projection instance while rebuilding. The source Incident database
and Kafka topic are not deleted.

## Inspect the result

```powershell
Invoke-RestMethod http://localhost:5203/projection/stats
Invoke-RestMethod http://localhost:5203/incidents
Invoke-RestMethod "http://localhost:5203/incidents?status=Resolved"
```

The read-model update and `(topic, partition, offset)` idempotency record are
stored in the same SQLite transaction. Kafka offset commit happens afterward.
