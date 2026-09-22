# Day 01 — Producer, Consumer, Key and Offset

## Run

```powershell
docker compose up -d
dotnet restore
dotnet build
```

Open terminal A and start the consumer:

```powershell
dotnet run --project src/IncidentDetector
```

Open terminal B and start the producer:

```powershell
dotnet run --project src/EventGenerator
```

## Observe

The producer uses the service name as the Kafka key. A message with the same key is routed consistently to the same partition; ordering is therefore preserved for one service within that partition. The consumer commits only after handling a message, which is the starting point for at-least-once delivery.

## Experiments

1. Stop and restart the consumer. Does it receive events already committed?
2. Run a second consumer with the same `GroupId`. Which partitions does each instance receive?
3. Change the `GroupId` to `incident-detector-v2`. Why are earlier records read again?
4. Replace the producer key with `EventId.ToString()`. What changes about ordering?

## Force a partition (learning experiment)

Normally let Kafka select the partition from the message key. To deliberately send events to partition 2 and wake the Consumer that owns it, run:

```powershell
dotnet run --project src/EventGenerator -- --partition 2
```

This is an experiment, not the normal production pattern: forcing a partition bypasses key-based routing and can break the ordering strategy for an entity.

## Shutdown

```powershell
docker compose down
```

Use `docker compose down -v` only when you intend to delete Kafka's persisted local data.
