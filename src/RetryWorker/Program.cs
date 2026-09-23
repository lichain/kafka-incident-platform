using Confluent.Kafka;
const string RetryTopic = "app-events.retry";
const string MainTopic = "app-events";
var servers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig { BootstrapServers = servers, GroupId = "retry-worker", AutoOffsetReset = AutoOffsetReset.Earliest, EnableAutoCommit = false }).Build();
using var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = servers }).Build();
consumer.Subscribe(RetryTopic);
Console.WriteLine("RetryWorker: app-events.retry → wait 5 seconds → app-events");
while (true)
{
    var result = consumer.Consume();
    await Task.Delay(TimeSpan.FromSeconds(5));
    await producer.ProduceAsync(MainTopic, new Message<string, string> { Key = result.Message.Key, Value = result.Message.Value, Headers = result.Message.Headers });
    consumer.Commit(result);
    Console.WriteLine($"retried partition={result.Partition} offset={result.Offset}");
}
