# Day 05 — Slow consumer and rebalance

`max.poll.interval.ms` is the maximum time your application may spend between calls to `Consume`. Heartbeats alone do not save a consumer that is not polling: if message processing exceeds this interval, Kafka removes it from the group and reassigns its partitions.

## Intentionally exceed the poll interval

Keep a Producer running, then start this Consumer:

```powershell
dotnet run --project src/IncidentDetector -- `
  --group day-05-slow `
  --delay-ms 12000 `
  --session-timeout-ms 6000 `
  --max-poll-interval-ms 10000
```

The Consumer waits twelve seconds after processing each record but must poll every ten seconds. The session timeout is six seconds, which satisfies the client constraint that `max.poll.interval.ms` must be at least `session.timeout.ms`. Watch for `assigned` and `revoked` messages. It may be removed from the group and rejoin; records that were not committed before the rebalance can be delivered again.

## Normal configuration

For a real service, keep record processing short, batch work carefully, or hand work to a bounded worker pool while the consumer continues polling. Set `max.poll.interval.ms` above the *worst-case bounded processing time*, not above an unbounded operation.
