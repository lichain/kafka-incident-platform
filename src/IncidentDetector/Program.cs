using System.Text.Json;
using Confluent.Kafka;

const string Topic = "app-events";
var options = ParseOptions(args);
var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
{
    BootstrapServers = bootstrapServers, GroupId = options.GroupId,
    AutoOffsetReset = AutoOffsetReset.Earliest, EnableAutoCommit = false
}).Build();
consumer.Subscribe(Topic);
Console.WriteLine($"Consuming '{Topic}' as '{options.GroupId}' via {bootstrapServers} (commit: {options.CommitMode}). Press Ctrl+C to stop.");
try
{
    while (true)
    {
        var result = consumer.Consume();
        var appEvent = JsonSerializer.Deserialize<AppEvent>(result.Message.Value)
            ?? throw new JsonException("Kafka message did not contain an app event.");
        Console.WriteLine($"key={result.Message.Key,-12} partition={result.Partition} offset={result.Offset} level={appEvent.Level} message={appEvent.Message}");
        if (options.CommitMode == "manual")
            consumer.Commit(result);
    }
}
finally { consumer.Close(); }

static ConsumerOptions ParseOptions(string[] arguments)
{
    var groupId = "incident-detector";
    var commitMode = "manual";

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
            default:
                throw new ArgumentException("Usage: dotnet run --project src/IncidentDetector -- [--group <name>] [--commit manual|none]");
        }
    }

    return new ConsumerOptions(groupId, commitMode);
}

internal sealed record AppEvent(Guid EventId, string Service, string Level, string Message, DateTimeOffset OccurredAt);

internal sealed record ConsumerOptions(string GroupId, string CommitMode);
