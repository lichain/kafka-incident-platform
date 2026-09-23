# Day 06 — Safe commit failure handling

When a Consumer exceeds `max.poll.interval.ms`, Kafka revokes its partition ownership. A later commit from that old membership can fail with `Unknown member` or a rebalance-related error.

The lab now catches commit failures and records that the message can be delivered again. It does **not** retry a commit using stale partition ownership. The consumer must rejoin the group and use a fresh assignment.

## Repeat the slow-consumer run

```powershell
dotnet run --project src/IncidentDetector -- `
  --group day-06-safe-commit `
  --delay-ms 12000 `
  --session-timeout-ms 6000 `
  --max-poll-interval-ms 10000
```

After `MAXPOLL` and `revoked`, the program should log `commit failed ... may be delivered again` instead of throwing an unhandled exception.

## Design takeaway

Consumer correctness requires both layers:

1. Keep polling within `max.poll.interval.ms`.
2. Make processing idempotent, because a failed commit can cause redelivery.
