# Day 03 — Consumer offsets and commits

A Kafka offset is stored per **consumer group** and per partition. It is not stored per consumer process.

## Experiment A: manual commit survives restart

Start a consumer in Terminal A:

```powershell
dotnet run --project src/IncidentDetector -- --group day-03-manual --commit manual
```

Let it consume several messages, then press `Ctrl+C`. Restart exactly the same command. It continues after its committed offsets rather than replaying old records.

Inspect the stored positions:

```powershell
docker exec kafka-lab /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server localhost:9092 `
  --describe `
  --group day-03-manual
```

## Experiment B: a new group starts fresh

Use a different group name:

```powershell
dotnet run --project src/IncidentDetector -- --group day-03-fresh --commit manual
```

Because `day-03-fresh` has no stored offsets, `AutoOffsetReset = Earliest` makes it replay every retained message.

## Experiment C: handle but do not commit

```powershell
dotnet run --project src/IncidentDetector -- --group day-03-no-commit --commit none
```

Stop it after several records, then run the same command again. The records reappear because they were handled but no group offset was committed. This is the beginning of at-least-once delivery and the reason later labs need idempotency.
