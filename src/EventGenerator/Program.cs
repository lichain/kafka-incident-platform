using System.Text.Json;
using Confluent.Kafka;

const string Topic = "app-events";
var options = ParseOptions(args);

var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
using var producer = new ProducerBuilder<string, string>(new ProducerConfig
{
    BootstrapServers = bootstrapServers, ClientId = "event-generator", Acks = Acks.All
}).Build();

var routing = options.ForcedPartition is null
    ? $"key-based partitioning ({options.KeyMode} key)"
    : $"forced partition {options.ForcedPartition}";
Console.WriteLine($"Producing to '{Topic}' via {bootstrapServers} ({routing}, interval: {options.IntervalMs}ms). Press Ctrl+C to stop.");
for (var sequence = 1; ; sequence++)
{
    var service = (sequence % 3) switch { 0 => "payment-api", 1 => "order-api", _ => "user-api" };
    var appEvent = new AppEvent(Guid.NewGuid(), service,
        sequence % 4 == 0 ? "Error" : "Information",
        sequence % 4 == 0 ? "Database timeout" : "Request completed", DateTimeOffset.UtcNow);
    var key = options.KeyMode == "service" ? appEvent.Service : appEvent.EventId.ToString();
    var message = new Message<string, string>
    {
        Key = key, Value = JsonSerializer.Serialize(appEvent)
    };
    var delivery = options.ForcedPartition is null
        ? await producer.ProduceAsync(Topic, message)
        : await producer.ProduceAsync(new TopicPartition(Topic, new Partition(options.ForcedPartition.Value)), message);
    Console.WriteLine($"#{sequence} key={key,-36} partition={delivery.Partition} offset={delivery.Offset} level={appEvent.Level}");
    await Task.Delay(options.IntervalMs);
}

static GeneratorOptions ParseOptions(string[] arguments)
{
    int? forcedPartition = null;
    var keyMode = "service";
    var intervalMs = 1000;

    for (var index = 0; index < arguments.Length; index += 2)
    {
        if (index + 1 >= arguments.Length)
            throw new ArgumentException("Each option needs a value.");

        switch (arguments[index], arguments[index + 1])
        {
            case ("--partition", var value) when int.TryParse(value, out var partition) && partition >= 0:
                forcedPartition = partition;
                break;
            case ("--key", "service" or "event"):
                keyMode = arguments[index + 1];
                break;
            case ("--interval-ms", var value) when int.TryParse(value, out var interval) && interval >= 0:
                intervalMs = interval;
                break;
            default:
                throw new ArgumentException("Usage: dotnet run --project src/EventGenerator -- [--key service|event] [--partition <non-negative number>] [--interval-ms <number>]");
        }
    }

    return new GeneratorOptions(forcedPartition, keyMode, intervalMs);
}

internal sealed record AppEvent(Guid EventId, string Service, string Level, string Message, DateTimeOffset OccurredAt);

internal sealed record GeneratorOptions(int? ForcedPartition, string KeyMode, int IntervalMs);
