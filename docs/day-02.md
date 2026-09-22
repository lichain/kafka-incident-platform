# Day 02 — Kafka message key

Kafka preserves record order only within one partition. The producer key controls which partition is selected, so choosing a key is a business decision, not a formatting detail.

## Experiment A: service key

```powershell
dotnet run --project src/EventGenerator -- --key service
```

All events for one service use the same key. This preserves service-level ordering, but a high-volume service can make one partition hot.

## Experiment B: event key

```powershell
dotnet run --project src/EventGenerator -- --key event
```

Every event uses a new GUID, so records should spread more evenly across the three partitions. The trade-off: there is no longer an ordering guarantee for a service.

## Decision guide

| Ordering required for | Typical key |
| --- | --- |
| One incident lifecycle | `IncidentId` |
| One order lifecycle | `OrderId` |
| One user's actions | `UserId` |
| All events from a service | `ServiceName` |
| No entity-level ordering | Random event ID or no key |

Avoid choosing a key just because it happens to distribute well. First identify the smallest business entity whose event order must be preserved.
