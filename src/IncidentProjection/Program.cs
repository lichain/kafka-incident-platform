using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Data.Sqlite;

var rebuildRequested = args.Contains("--rebuild", StringComparer.OrdinalIgnoreCase)
    || bool.TryParse(Environment.GetEnvironmentVariable("REBUILD_PROJECTION"), out var rebuildFromEnvironment)
       && rebuildFromEnvironment;
var applicationArgs = args
    .Where(argument => !string.Equals(argument, "--rebuild", StringComparison.OrdinalIgnoreCase))
    .ToArray();
var builder = WebApplication.CreateBuilder(applicationArgs);
var dataDirectory = Path.Combine("data");
Directory.CreateDirectory(dataDirectory);

var baseGroupId = Environment.GetEnvironmentVariable("INCIDENT_PROJECTION_GROUP") ?? "incident-projection-v1";
var groupId = rebuildRequested
    ? $"{baseGroupId}-rebuild-{DateTimeOffset.UtcNow:yyyyMMddHHmmssfff}"
    : baseGroupId;
var options = new ProjectionOptions(
    $"Data Source={Path.Combine(dataDirectory, "incident-projection.db")};Cache=Shared",
    Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092",
    groupId,
    "incident-events",
    rebuildRequested);
var database = new ProjectionDatabase(options.ConnectionString);
await database.InitializeAsync(options.RebuildMode);

builder.Services.AddSingleton(options);
builder.Services.AddSingleton(database);
builder.Services.AddHostedService<IncidentProjectionWorker>();

var app = builder.Build();

app.MapGet("/incidents", async (string? status, ProjectionDatabase projection, CancellationToken token) =>
    Results.Ok(await projection.ListIncidentsAsync(status, token)));

app.MapGet("/incidents/{id:guid}", async (Guid id, ProjectionDatabase projection, CancellationToken token) =>
{
    var incident = await projection.GetIncidentAsync(id, token);
    return incident is null ? Results.NotFound() : Results.Ok(incident);
});

app.MapGet("/projection/stats", async (ProjectionDatabase projection, ProjectionOptions current, CancellationToken token) =>
{
    var stats = await projection.GetStatsAsync(token);
    return Results.Ok(new
    {
        current.GroupId,
        current.RebuildMode,
        stats.Incidents,
        stats.ProcessedEvents
    });
});

app.Run();

record ProjectionOptions(
    string ConnectionString,
    string BootstrapServers,
    string GroupId,
    string Topic,
    bool RebuildMode);
record IncidentView(
    Guid Id,
    string Service,
    int Severity,
    string Status,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);
record ProjectionStats(long Incidents, long ProcessedEvents);

sealed class ProjectionDatabase(string connectionString)
{
    public async Task InitializeAsync(bool rebuild)
    {
        await using var connection = await OpenConnectionAsync(CancellationToken.None);
        var command = connection.CreateCommand();
        command.CommandText = """
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS incident_read_model (
                id TEXT PRIMARY KEY,
                service TEXT NOT NULL,
                severity INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS processed_messages (
                topic TEXT NOT NULL,
                partition_id INTEGER NOT NULL,
                offset_value INTEGER NOT NULL,
                processed_at TEXT NOT NULL,
                PRIMARY KEY (topic, partition_id, offset_value)
            );
            """;
        await command.ExecuteNonQueryAsync();

        if (rebuild)
        {
            using var transaction = connection.BeginTransaction();
            var reset = connection.CreateCommand();
            reset.Transaction = transaction;
            reset.CommandText = """
                DELETE FROM incident_read_model;
                DELETE FROM processed_messages;
                """;
            await reset.ExecuteNonQueryAsync();
            transaction.Commit();
        }
    }

    public async Task<bool> ApplyAsync(ConsumeResult<string, string> message, CancellationToken token)
    {
        await using var connection = await OpenConnectionAsync(token);
        using var transaction = connection.BeginTransaction();

        var remember = connection.CreateCommand();
        remember.Transaction = transaction;
        remember.CommandText = """
            INSERT OR IGNORE INTO processed_messages
                (topic, partition_id, offset_value, processed_at)
            VALUES
                ($topic, $partition, $offset, $processed_at)
            """;
        remember.Parameters.AddWithValue("$topic", message.Topic);
        remember.Parameters.AddWithValue("$partition", message.Partition.Value);
        remember.Parameters.AddWithValue("$offset", message.Offset.Value);
        remember.Parameters.AddWithValue("$processed_at", DateTimeOffset.UtcNow.ToString("O"));

        if (await remember.ExecuteNonQueryAsync(token) == 0)
        {
            transaction.Commit();
            return false;
        }

        using var document = JsonDocument.Parse(message.Message.Value);
        var root = document.RootElement;
        if (root.TryGetProperty("EventType", out var eventTypeElement))
        {
            var eventType = eventTypeElement.GetString();
            switch (eventType)
            {
                case "IncidentCreated":
                    await UpsertCreatedIncidentAsync(connection, transaction, root, token);
                    break;
                case "IncidentAcknowledged":
                case "IncidentResolved":
                    await UpdateStatusAsync(connection, transaction, root, token);
                    break;
                default:
                    throw new InvalidOperationException($"Unsupported event type '{eventType}'.");
            }
        }
        else
        {
            await UpsertLegacyIncidentAsync(connection, transaction, root, token);
        }

        transaction.Commit();
        return true;
    }

    public async Task<IReadOnlyList<IncidentView>> ListIncidentsAsync(string? status, CancellationToken token)
    {
        await using var connection = await OpenConnectionAsync(token);
        var command = connection.CreateCommand();
        command.CommandText = string.IsNullOrWhiteSpace(status)
            ? "SELECT id, service, severity, status, created_at, updated_at FROM incident_read_model ORDER BY updated_at DESC"
            : "SELECT id, service, severity, status, created_at, updated_at FROM incident_read_model WHERE status = $status ORDER BY updated_at DESC";
        if (!string.IsNullOrWhiteSpace(status))
        {
            command.Parameters.AddWithValue("$status", status);
        }

        await using var rows = await command.ExecuteReaderAsync(token);
        var incidents = new List<IncidentView>();
        while (await rows.ReadAsync(token))
        {
            incidents.Add(ReadIncident(rows));
        }

        return incidents;
    }

    public async Task<IncidentView?> GetIncidentAsync(Guid id, CancellationToken token)
    {
        await using var connection = await OpenConnectionAsync(token);
        var command = connection.CreateCommand();
        command.CommandText = """
            SELECT id, service, severity, status, created_at, updated_at
            FROM incident_read_model
            WHERE id = $id
            """;
        command.Parameters.AddWithValue("$id", id.ToString());
        await using var row = await command.ExecuteReaderAsync(token);
        return await row.ReadAsync(token) ? ReadIncident(row) : null;
    }

    public async Task<ProjectionStats> GetStatsAsync(CancellationToken token)
    {
        await using var connection = await OpenConnectionAsync(token);
        var command = connection.CreateCommand();
        command.CommandText = """
            SELECT
                (SELECT COUNT(*) FROM incident_read_model),
                (SELECT COUNT(*) FROM processed_messages)
            """;
        await using var row = await command.ExecuteReaderAsync(token);
        await row.ReadAsync(token);
        return new ProjectionStats(row.GetInt64(0), row.GetInt64(1));
    }

    private static async Task UpsertCreatedIncidentAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        JsonElement root,
        CancellationToken token)
    {
        var occurredAt = root.GetProperty("OccurredAt").GetDateTimeOffset();
        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO incident_read_model
                (id, service, severity, status, created_at, updated_at)
            VALUES
                ($id, $service, $severity, $status, $created_at, $updated_at)
            ON CONFLICT(id) DO UPDATE SET
                service = excluded.service,
                severity = excluded.severity,
                status = excluded.status,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            """;
        command.Parameters.AddWithValue("$id", root.GetProperty("IncidentId").GetGuid().ToString());
        command.Parameters.AddWithValue("$service", root.GetProperty("Service").GetString()!);
        command.Parameters.AddWithValue("$severity", root.GetProperty("Severity").GetInt32());
        command.Parameters.AddWithValue("$status", root.GetProperty("Status").GetString()!);
        command.Parameters.AddWithValue("$created_at", occurredAt.ToString("O"));
        command.Parameters.AddWithValue("$updated_at", occurredAt.ToString("O"));
        await command.ExecuteNonQueryAsync(token);
    }

    private static async Task UpsertLegacyIncidentAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        JsonElement root,
        CancellationToken token)
    {
        var createdAt = root.GetProperty("CreatedAt").GetDateTimeOffset();
        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO incident_read_model
                (id, service, severity, status, created_at, updated_at)
            VALUES
                ($id, $service, $severity, 'Open', $created_at, $updated_at)
            ON CONFLICT(id) DO NOTHING
            """;
        command.Parameters.AddWithValue("$id", root.GetProperty("Id").GetGuid().ToString());
        command.Parameters.AddWithValue("$service", root.GetProperty("Service").GetString()!);
        command.Parameters.AddWithValue("$severity", root.GetProperty("Severity").GetInt32());
        command.Parameters.AddWithValue("$created_at", createdAt.ToString("O"));
        command.Parameters.AddWithValue("$updated_at", createdAt.ToString("O"));
        await command.ExecuteNonQueryAsync(token);
    }

    private static async Task UpdateStatusAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        JsonElement root,
        CancellationToken token)
    {
        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE incident_read_model
            SET status = $status, updated_at = $updated_at
            WHERE id = $id
            """;
        command.Parameters.AddWithValue("$status", root.GetProperty("Status").GetString()!);
        command.Parameters.AddWithValue("$updated_at", root.GetProperty("OccurredAt").GetDateTimeOffset().ToString("O"));
        command.Parameters.AddWithValue("$id", root.GetProperty("IncidentId").GetGuid().ToString());
        if (await command.ExecuteNonQueryAsync(token) != 1)
        {
            throw new InvalidOperationException("Status event arrived before its IncidentCreated event.");
        }
    }

    private async Task<SqliteConnection> OpenConnectionAsync(CancellationToken token)
    {
        var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(token);
        var configure = connection.CreateCommand();
        configure.CommandText = "PRAGMA busy_timeout = 5000";
        await configure.ExecuteNonQueryAsync(token);
        return connection;
    }

    private static IncidentView ReadIncident(SqliteDataReader row) => new(
        Guid.Parse(row.GetString(0)),
        row.GetString(1),
        row.GetInt32(2),
        row.GetString(3),
        DateTimeOffset.Parse(row.GetString(4)),
        DateTimeOffset.Parse(row.GetString(5)));
}

sealed class IncidentProjectionWorker(
    ProjectionOptions options,
    ProjectionDatabase database,
    ILogger<IncidentProjectionWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken token)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetPartitionsAssignedHandler((_, partitions) =>
                logger.LogInformation("Assigned: {Partitions}", string.Join(", ", partitions)))
            .SetPartitionsRevokedHandler((_, partitions) =>
                logger.LogInformation("Revoked: {Partitions}", string.Join(", ", partitions)))
            .Build();
        consumer.Subscribe(options.Topic);
        logger.LogInformation(
            "Projecting {Topic} as consumer group {GroupId} (rebuild: {RebuildMode})",
            options.Topic,
            options.GroupId,
            options.RebuildMode);

        try
        {
            while (!token.IsCancellationRequested)
            {
                var message = consumer.Consume(token);
                var applied = await database.ApplyAsync(message, token);
                consumer.Commit(message);
                logger.LogInformation(
                    "{Result} {Topic}[{Partition}]@{Offset} key={Key}",
                    applied ? "Applied" : "Duplicate skipped",
                    message.Topic,
                    message.Partition.Value,
                    message.Offset.Value,
                    message.Message.Key);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        finally
        {
            consumer.Close();
        }
    }
}
