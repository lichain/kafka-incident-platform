using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Data.Sqlite;

const string Topic = "app-events";
var options = ParseOptions(args);
var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
using var processedEventStore = options.IdempotencyMode == "sqlite"
    ? new SqliteProcessedEventStore(Path.Combine("data", "processed-events.db"))
    : null;
using var dlqProducer = options.FailureMode is "dlq-on-error" or "retry-on-error"
    ? new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = bootstrapServers,
        ClientId = "incident-detector-dlq"
    }).Build()
    : null;
using var retryProducer = options.FailureMode == "retry-on-error"
    ? new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrapServers, ClientId = "incident-detector-retry" }).Build()
    : null;

using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = options.GroupId,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    SessionTimeoutMs = options.SessionTimeoutMs,
    MaxPollIntervalMs = options.MaxPollIntervalMs
})
    .SetPartitionsAssignedHandler((_, partitions) =>
        Console.WriteLine($"assigned: {string.Join(", ", partitions)}"))
    .SetPartitionsRevokedHandler((_, partitions) =>
        Console.WriteLine($"revoked: {string.Join(", ", partitions)}"))
    .Build();

consumer.Subscribe(Topic);
Console.WriteLine($"Consuming '{Topic}' as '{options.GroupId}' via {bootstrapServers} (batch: {options.BatchSize}, commit: {options.CommitMode}, idempotency: {options.IdempotencyMode}, failure mode: {options.FailureMode}, delay: {options.ProcessingDelayMs}ms, session timeout: {options.SessionTimeoutMs}ms, max.poll.interval: {options.MaxPollIntervalMs}ms). Press Ctrl+C to stop.");

try
{
    while (true)
    {
        var batch = new List<ConsumeResult<string, string>> { consumer.Consume() };
        while (batch.Count < options.BatchSize)
        {
            var next = consumer.Consume(TimeSpan.FromMilliseconds(25));
            if (next is null)
                break;
            batch.Add(next);
        }

        Console.WriteLine($"processing batch: {batch.Count} record(s)");
        var commitOffsets = new Dictionary<TopicPartition, Offset>();
        foreach (var result in batch)
        {
            var appEvent = JsonSerializer.Deserialize<AppEvent>(result.Message.Value)
                ?? throw new JsonException("Kafka message did not contain an app event.");

            if (options.FailureMode == "dlq-on-error" && appEvent.Level == "Error")
            {
                var deadLetter = new DeadLetterEvent(
                    Topic,
                    result.Partition.Value,
                    result.Offset.Value,
                    result.Message.Key,
                    result.Message.Value,
                    "Simulated processing failure for an Error event.",
                    DateTimeOffset.UtcNow);
                await dlqProducer!.ProduceAsync($"{Topic}.dlq", new Message<string, string>
                {
                    Key = result.Message.Key,
                    Value = JsonSerializer.Serialize(deadLetter)
                });
                Console.WriteLine($"sent to DLQ: partition={result.Partition} offset={result.Offset} reason=simulated-error");
                commitOffsets[result.TopicPartition] = result.Offset + 1;
                continue;
            }

            if (options.FailureMode == "retry-on-error" && appEvent.Level == "Error")
            {
                var attempts = result.Message.Headers?.TryGetLastBytes("retry-count", out var countBytes) == true
                    ? int.Parse(System.Text.Encoding.UTF8.GetString(countBytes))
                    : 0;
                if (attempts >= 2)
                {
                    await dlqProducer!.ProduceAsync($"{Topic}.dlq", new Message<string, string> { Key = result.Message.Key, Value = result.Message.Value });
                    Console.WriteLine($"sent to DLQ after {attempts} retries: offset={result.Offset}");
                    commitOffsets[result.TopicPartition] = result.Offset + 1;
                    continue;
                }
                await retryProducer!.ProduceAsync($"{Topic}.retry", new Message<string, string>
                {
                    Key = result.Message.Key,
                    Value = result.Message.Value,
                    Headers = new Headers { { "retry-count", System.Text.Encoding.UTF8.GetBytes((attempts + 1).ToString()) } }
                });
                Console.WriteLine($"sent to retry: partition={result.Partition} offset={result.Offset}");
                commitOffsets[result.TopicPartition] = result.Offset + 1;
                continue;
            }

            if (processedEventStore is not null && !processedEventStore.TryMarkProcessed(appEvent.EventId))
            {
                Console.WriteLine($"duplicate eventId={appEvent.EventId} partition={result.Partition} offset={result.Offset}; skipped");
            }
            else
            {
                Console.WriteLine($"key={result.Message.Key,-12} partition={result.Partition} offset={result.Offset} level={appEvent.Level} message={appEvent.Message}");
                if (options.ProcessingDelayMs > 0)
                    await Task.Delay(options.ProcessingDelayMs);
            }

            commitOffsets[result.TopicPartition] = result.Offset + 1;
        }

        if (options.CommitMode == "manual")
            TryCommit(consumer, commitOffsets.Select(item => new TopicPartitionOffset(item.Key, item.Value)));
    }
}
finally
{
    consumer.Close();
}

static ConsumerOptions ParseOptions(string[] arguments)
{
    var groupId = "incident-detector";
    var commitMode = "manual";
    var idempotencyMode = "none";
    var processingDelayMs = 0;
    var sessionTimeoutMs = 45_000;
    var maxPollIntervalMs = 300_000;
    var batchSize = 1;
    var failureMode = "none";

    for (var index = 0; index < arguments.Length; index += 2)
    {
        if (index + 1 >= arguments.Length)
            throw new ArgumentException("Each option needs a value.");

        switch (arguments[index], arguments[index + 1])
        {
            case ("--group", var value) when !string.IsNullOrWhiteSpace(value):
                groupId = value;
                break;
            case ("--commit", "manual" or "none"):
                commitMode = arguments[index + 1];
                break;
            case ("--idempotency", "none" or "sqlite"):
                idempotencyMode = arguments[index + 1];
                break;
            case ("--delay-ms", var value) when int.TryParse(value, out var delay) && delay >= 0:
                processingDelayMs = delay;
                break;
            case ("--session-timeout-ms", var value) when int.TryParse(value, out var timeout) && timeout > 0:
                sessionTimeoutMs = timeout;
                break;
            case ("--max-poll-interval-ms", var value) when int.TryParse(value, out var interval) && interval > 0:
                maxPollIntervalMs = interval;
                break;
            case ("--batch-size", var value) when int.TryParse(value, out var size) && size > 0:
                batchSize = size;
                break;
            case ("--failure-mode", "none" or "dlq-on-error" or "retry-on-error"):
                failureMode = arguments[index + 1];
                break;
            default:
                throw new ArgumentException("Usage: dotnet run --project src/IncidentDetector -- [--group <name>] [--commit manual|none] [--idempotency none|sqlite] [--delay-ms <number>] [--session-timeout-ms <number>] [--max-poll-interval-ms <number>] [--batch-size <number>] [--failure-mode none|dlq-on-error]");
        }
    }

    return new ConsumerOptions(groupId, commitMode, idempotencyMode, processingDelayMs, sessionTimeoutMs, maxPollIntervalMs, batchSize, failureMode);
}

static void TryCommit(IConsumer<string, string> consumer, IEnumerable<TopicPartitionOffset> offsets)
{
    try
    {
        consumer.Commit(offsets);
    }
    catch (KafkaException exception)
    {
        Console.WriteLine($"batch commit failed: {exception.Error.Reason}. Records may be delivered again after rebalance.");
    }
}

internal sealed record AppEvent(Guid EventId, string Service, string Level, string Message, DateTimeOffset OccurredAt);

internal sealed record ConsumerOptions(
    string GroupId,
    string CommitMode,
    string IdempotencyMode,
    int ProcessingDelayMs,
    int SessionTimeoutMs,
    int MaxPollIntervalMs,
    int BatchSize,
    string FailureMode);

internal sealed record DeadLetterEvent(
    string OriginalTopic,
    int OriginalPartition,
    long OriginalOffset,
    string OriginalKey,
    string OriginalValue,
    string FailureReason,
    DateTimeOffset FailedAt);

internal sealed class SqliteProcessedEventStore : IDisposable
{
    private readonly SqliteConnection _connection;

    public SqliteProcessedEventStore(string databasePath)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(databasePath)!);
        _connection = new SqliteConnection($"Data Source={databasePath}");
        _connection.Open();

        using var command = _connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id TEXT PRIMARY KEY,
                processed_at TEXT NOT NULL
            );
            """;
        command.ExecuteNonQuery();
    }

    public bool TryMarkProcessed(Guid eventId)
    {
        using var command = _connection.CreateCommand();
        command.CommandText = "INSERT OR IGNORE INTO processed_events (event_id, processed_at) VALUES ($eventId, $processedAt);";
        command.Parameters.AddWithValue("$eventId", eventId.ToString());
        command.Parameters.AddWithValue("$processedAt", DateTimeOffset.UtcNow.ToString("O"));
        return command.ExecuteNonQuery() == 1;
    }

    public void Dispose() => _connection.Dispose();
}
