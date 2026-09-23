using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Data.Sqlite;

const string IncidentEventsTopic = "incident-events";

var builder = WebApplication.CreateBuilder(args);
var dbPath = Path.Combine("data", "incident-service.db");
Directory.CreateDirectory("data");
var connectionString = $"Data Source={dbPath}";

await InitializeDatabaseAsync(connectionString);

builder.Services.AddSingleton(new OutboxOptions(
    connectionString,
    Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092"));
builder.Services.AddHostedService<OutboxPublisher>();

var app = builder.Build();

app.MapPost("/incidents", async (CreateIncident request, OutboxOptions options) =>
{
    if (string.IsNullOrWhiteSpace(request.Service) || request.Severity is < 1 or > 5)
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["service"] = string.IsNullOrWhiteSpace(request.Service)
                ? ["Service is required."]
                : [],
            ["severity"] = request.Severity is < 1 or > 5
                ? ["Severity must be between 1 and 5."]
                : []
        }.Where(item => item.Value.Length > 0).ToDictionary());
    }

    var now = DateTimeOffset.UtcNow;
    var incident = new Incident(Guid.NewGuid(), request.Service.Trim(), request.Severity, IncidentStatus.Open, now);
    var domainEvent = new IncidentCreatedEvent(
        "IncidentCreated",
        incident.Id,
        incident.Service,
        incident.Severity,
        incident.Status,
        now);
    var outbox = CreateOutboxMessage(IncidentEventsTopic, domainEvent, now);

    await using var connection = new SqliteConnection(options.ConnectionString);
    await connection.OpenAsync();
    using var transaction = connection.BeginTransaction();

    var insertIncident = connection.CreateCommand();
    insertIncident.Transaction = transaction;
    insertIncident.CommandText = """
        INSERT INTO incidents (id, service, severity, status, created_at)
        VALUES ($id, $service, $severity, $status, $created_at)
        """;
    insertIncident.Parameters.AddWithValue("$id", incident.Id.ToString());
    insertIncident.Parameters.AddWithValue("$service", incident.Service);
    insertIncident.Parameters.AddWithValue("$severity", incident.Severity);
    insertIncident.Parameters.AddWithValue("$status", incident.Status);
    insertIncident.Parameters.AddWithValue("$created_at", incident.CreatedAt.ToString("O"));
    await insertIncident.ExecuteNonQueryAsync();

    await CreateOutboxCommand(connection, transaction, outbox).ExecuteNonQueryAsync();
    transaction.Commit();

    return Results.Created($"/incidents/{incident.Id}", incident);
});

app.MapGet("/incidents/{id:guid}", async (Guid id, OutboxOptions options) =>
{
    await using var connection = new SqliteConnection(options.ConnectionString);
    await connection.OpenAsync();
    var incident = await LoadIncidentAsync(connection, null, id);
    return incident is null ? Results.NotFound() : Results.Ok(incident);
});

app.MapPatch("/incidents/{id:guid}/acknowledge", (Guid id, OutboxOptions options) =>
    TransitionIncidentAsync(id, IncidentStatus.Open, IncidentStatus.Acknowledged, "IncidentAcknowledged", options));

app.MapPatch("/incidents/{id:guid}/resolve", (Guid id, OutboxOptions options) =>
    TransitionIncidentAsync(id, IncidentStatus.Acknowledged, IncidentStatus.Resolved, "IncidentResolved", options));

app.MapGet("/outbox", async (OutboxOptions options) =>
{
    await using var connection = new SqliteConnection(options.ConnectionString);
    await connection.OpenAsync();
    var command = connection.CreateCommand();
    command.CommandText = """
        SELECT id, topic, payload, created_at, published_at
        FROM outbox_messages
        ORDER BY created_at DESC
        """;
    await using var rows = await command.ExecuteReaderAsync();
    var items = new List<object>();
    while (await rows.ReadAsync())
    {
        items.Add(new
        {
            Id = rows.GetString(0),
            Topic = rows.GetString(1),
            Payload = JsonSerializer.Deserialize<JsonElement>(rows.GetString(2)),
            CreatedAt = rows.GetString(3),
            PublishedAt = rows.IsDBNull(4) ? null : rows.GetString(4)
        });
    }

    return Results.Ok(items);
});

app.Run();

static async Task InitializeDatabaseAsync(string connectionString)
{
    await using var connection = new SqliteConnection(connectionString);
    await connection.OpenAsync();

    var create = connection.CreateCommand();
    create.CommandText = """
        CREATE TABLE IF NOT EXISTS incidents (
            id TEXT PRIMARY KEY,
            service TEXT NOT NULL,
            severity INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'Open',
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS outbox_messages (
            id TEXT PRIMARY KEY,
            topic TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL,
            published_at TEXT NULL
        );
        """;
    await create.ExecuteNonQueryAsync();

    var tableInfo = connection.CreateCommand();
    tableInfo.CommandText = "PRAGMA table_info(incidents)";
    var hasStatus = false;
    await using (var columns = await tableInfo.ExecuteReaderAsync())
    {
        while (await columns.ReadAsync())
        {
            if (string.Equals(columns.GetString(1), "status", StringComparison.OrdinalIgnoreCase))
            {
                hasStatus = true;
                break;
            }
        }
    }

    if (!hasStatus)
    {
        var migrate = connection.CreateCommand();
        migrate.CommandText = "ALTER TABLE incidents ADD COLUMN status TEXT NOT NULL DEFAULT 'Open'";
        await migrate.ExecuteNonQueryAsync();
    }
}

static async Task<IResult> TransitionIncidentAsync(
    Guid id,
    string expectedStatus,
    string nextStatus,
    string eventType,
    OutboxOptions options)
{
    await using var connection = new SqliteConnection(options.ConnectionString);
    await connection.OpenAsync();
    using var transaction = connection.BeginTransaction();

    var incident = await LoadIncidentAsync(connection, transaction, id);
    if (incident is null)
    {
        transaction.Rollback();
        return Results.NotFound();
    }

    if (incident.Status != expectedStatus)
    {
        transaction.Rollback();
        return Results.Conflict(new
        {
            Message = $"Incident must be '{expectedStatus}' before it can become '{nextStatus}'.",
            CurrentStatus = incident.Status
        });
    }

    var update = connection.CreateCommand();
    update.Transaction = transaction;
    update.CommandText = """
        UPDATE incidents
        SET status = $next_status
        WHERE id = $id AND status = $expected_status
        """;
    update.Parameters.AddWithValue("$next_status", nextStatus);
    update.Parameters.AddWithValue("$id", id.ToString());
    update.Parameters.AddWithValue("$expected_status", expectedStatus);
    if (await update.ExecuteNonQueryAsync() != 1)
    {
        transaction.Rollback();
        return Results.Conflict(new { Message = "Incident status changed concurrently. Please retry." });
    }

    var now = DateTimeOffset.UtcNow;
    var updated = incident with { Status = nextStatus };
    var domainEvent = new IncidentStatusChangedEvent(eventType, id, expectedStatus, nextStatus, now);
    var outbox = CreateOutboxMessage("incident-events", domainEvent, now);
    await CreateOutboxCommand(connection, transaction, outbox).ExecuteNonQueryAsync();
    transaction.Commit();

    return Results.Ok(updated);
}

static async Task<Incident?> LoadIncidentAsync(
    SqliteConnection connection,
    SqliteTransaction? transaction,
    Guid id)
{
    var command = connection.CreateCommand();
    command.Transaction = transaction;
    command.CommandText = """
        SELECT id, service, severity, status, created_at
        FROM incidents
        WHERE id = $id
        """;
    command.Parameters.AddWithValue("$id", id.ToString());
    await using var row = await command.ExecuteReaderAsync();
    if (!await row.ReadAsync())
    {
        return null;
    }

    return new Incident(
        Guid.Parse(row.GetString(0)),
        row.GetString(1),
        row.GetInt32(2),
        row.GetString(3),
        DateTimeOffset.Parse(row.GetString(4)));
}

static OutboxMessage CreateOutboxMessage(string topic, object payload, DateTimeOffset now) =>
    new(Guid.NewGuid(), topic, JsonSerializer.Serialize(payload), now);

static SqliteCommand CreateOutboxCommand(
    SqliteConnection connection,
    SqliteTransaction transaction,
    OutboxMessage outbox)
{
    var command = connection.CreateCommand();
    command.Transaction = transaction;
    command.CommandText = """
        INSERT INTO outbox_messages (id, topic, payload, created_at)
        VALUES ($id, $topic, $payload, $created_at)
        """;
    command.Parameters.AddWithValue("$id", outbox.Id.ToString());
    command.Parameters.AddWithValue("$topic", outbox.Topic);
    command.Parameters.AddWithValue("$payload", outbox.Payload);
    command.Parameters.AddWithValue("$created_at", outbox.CreatedAt.ToString("O"));
    return command;
}

static class IncidentStatus
{
    public const string Open = "Open";
    public const string Acknowledged = "Acknowledged";
    public const string Resolved = "Resolved";
}

record CreateIncident(string Service, int Severity);
record Incident(Guid Id, string Service, int Severity, string Status, DateTimeOffset CreatedAt);
record IncidentCreatedEvent(
    string EventType,
    Guid IncidentId,
    string Service,
    int Severity,
    string Status,
    DateTimeOffset OccurredAt);
record IncidentStatusChangedEvent(
    string EventType,
    Guid IncidentId,
    string PreviousStatus,
    string Status,
    DateTimeOffset OccurredAt);
record OutboxMessage(Guid Id, string Topic, string Payload, DateTimeOffset CreatedAt);
record PendingOutboxMessage(string Id, string Topic, string Payload);
record OutboxOptions(string ConnectionString, string BootstrapServers);

sealed class OutboxPublisher(OutboxOptions options, ILogger<OutboxPublisher> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken token)
    {
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers
        }).Build();

        while (!token.IsCancellationRequested)
        {
            try
            {
                var pending = await LoadPendingMessagesAsync(token);
                foreach (var message in pending)
                {
                    using var document = JsonDocument.Parse(message.Payload);
                    var root = document.RootElement;
                    var incidentId = root.TryGetProperty("IncidentId", out var currentId)
                        ? currentId.GetString()!
                        : root.GetProperty("Id").GetString()!;

                    await producer.ProduceAsync(
                        message.Topic,
                        new Message<string, string> { Key = incidentId, Value = message.Payload },
                        token);
                    await MarkPublishedAsync(message.Id, token);
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Outbox publishing failed; unpublished messages will be retried.");
            }

            await Task.Delay(1000, token);
        }
    }

    private async Task<List<PendingOutboxMessage>> LoadPendingMessagesAsync(CancellationToken token)
    {
        await using var connection = new SqliteConnection(options.ConnectionString);
        await connection.OpenAsync(token);
        var command = connection.CreateCommand();
        command.CommandText = """
            SELECT id, topic, payload
            FROM outbox_messages
            WHERE published_at IS NULL
            ORDER BY created_at
            LIMIT 10
            """;
        await using var rows = await command.ExecuteReaderAsync(token);
        var messages = new List<PendingOutboxMessage>();
        while (await rows.ReadAsync(token))
        {
            messages.Add(new PendingOutboxMessage(rows.GetString(0), rows.GetString(1), rows.GetString(2)));
        }

        return messages;
    }

    private async Task MarkPublishedAsync(string id, CancellationToken token)
    {
        await using var connection = new SqliteConnection(options.ConnectionString);
        await connection.OpenAsync(token);
        var command = connection.CreateCommand();
        command.CommandText = "UPDATE outbox_messages SET published_at = $now WHERE id = $id";
        command.Parameters.AddWithValue("$now", DateTimeOffset.UtcNow.ToString("O"));
        command.Parameters.AddWithValue("$id", id);
        await command.ExecuteNonQueryAsync(token);
    }
}
