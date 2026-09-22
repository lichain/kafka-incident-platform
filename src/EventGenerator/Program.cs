using System.Text.Json;
using Confluent.Kafka;

const string Topic = "app-events";
int? forcedPartition = args switch
{
    [] => null,
    ["--partition", var value] when int.TryParse(value, out var partition) && partition >= 0 => partition,
    _ => throw new ArgumentException("Usage: dotnet run --project src/EventGenerator -- [--partition <non-negative number>]")
};

var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
using var producer = new ProducerBuilder<string, string>(new ProducerConfig
{
    BootstrapServers = bootstrapServers, ClientId = "event-generator", Acks = Acks.All
}).Build();

var routing = forcedPartition is null ? "key-based partitioning" : $"forced partition {forcedPartition}";
Console.WriteLine($"Producing to '{Topic}' via {bootstrapServers} ({routing}). Press Ctrl+C to stop.");
for (var sequence = 1; ; sequence++)
{
    var service = (sequence % 3) switch { 0 => "payment-api", 1 => "order-api", _ => "user-api" };
    var appEvent = new AppEvent(Guid.NewGuid(), service,
        sequence % 4 == 0 ? "Error" : "Information",
        sequence % 4 == 0 ? "Database timeout" : "Request completed", DateTimeOffset.UtcNow);
    var message = new Message<string, string>
    {
        Key = appEvent.Service, Value = JsonSerializer.Serialize(appEvent)
    };
    var delivery = forcedPartition is null
        ? await producer.ProduceAsync(Topic, message)
        : await producer.ProduceAsync(new TopicPartition(Topic, new Partition(forcedPartition.Value)), message);
    Console.WriteLine($"#{sequence} key={appEvent.Service,-12} partition={delivery.Partition} offset={delivery.Offset} level={appEvent.Level}");
    await Task.Delay(TimeSpan.FromSeconds(1));
}

internal sealed record AppEvent(Guid EventId, string Service, string Level, string Message, DateTimeOffset OccurredAt);
