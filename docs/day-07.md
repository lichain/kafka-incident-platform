# Day 07 — Consumer lag

Consumer lag is the difference between a partition's log-end offset and a consumer group's committed offset. It measures unprocessed work, not a time duration.

## Create lag

Start a deliberately slow Consumer:

```powershell
dotnet run --project src/IncidentDetector -- `
  --group day-07-lag `
  --delay-ms 2000
```

Start a faster Producer in another terminal:

```powershell
dotnet run --project src/EventGenerator -- `
  --key service `
  --interval-ms 200
```

The producer creates five records per second while the consumer handles roughly one record every two seconds. Open Kafka UI at http://localhost:8080 and inspect consumer group `day-07-lag`; its lag should grow.

## Drain lag

Stop the Producer but keep the Consumer running. The lag should decrease toward zero as the consumer catches up.

## Interpretation

Growing lag means the system's intake rate exceeds its processing rate. Common responses are to improve processing speed, add Consumer instances (up to the partition count), increase partition count only after considering key ordering, or apply backpressure upstream.
