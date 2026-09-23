# Kafka Incident Platform

A hands-on .NET and Apache Kafka distributed-systems learning lab.

## Day 01

```text
EventGenerator --(app-events)--> Kafka --(consumer group)--> IncidentDetector
```

See [Day 01 guide](docs/day-01.md).

See [Day 02 guide](docs/day-02.md) for key and partitioning experiments.

See [Day 03 guide](docs/day-03.md) for consumer offsets and commit experiments.

## Kafka UI

Run `docker compose up -d`, then open http://localhost:8080 to inspect the local broker, topics, partitions, messages and consumer groups.
