using System.Text.Json;
using Confluent.Kafka;

const string Topic = "app-events";
var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
{
    BootstrapServers = bootstrapServers, GroupId = "incident-detector",
    AutoOffsetReset = AutoOffsetReset.Earliest, EnableAutoCommit = false
}).Build();
consumer.Subscribe(Topic);
Console.WriteLine($"Consuming '{Topic}' as 'incident-detector' via {bootstrapServers}. Press Ctrl+C to stop.");
try
{
    while (true)
    {
        var result = consumer.Consume();
        var appEvent = JsonSerializer.Deserialize<AppEvent>(result.Message.Value)
            ?? throw new JsonException("Kafka message did not contain an app event.");
        Console.WriteLine($"key={result.Message.Key,-12} partition={result.Partition} offset={result.Offset} level={appEvent.Level} message={appEvent.Message}");
        consumer.Commit(result);
    }
}
finally { consumer.Close(); }

internal sealed record AppEvent(Guid EventId, string Service, string Level, string Message, DateTimeOffset OccurredAt);
