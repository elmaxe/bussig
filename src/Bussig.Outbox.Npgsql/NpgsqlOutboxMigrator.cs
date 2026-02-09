using System.Globalization;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Bussig.Outbox.Npgsql;

internal sealed class NpgsqlOutboxMigrator(
    [FromKeyedServices(OutboxServiceKeys.OutboxNpgsqlDataSource)] NpgsqlDataSource dataSource,
    IOptions<NpgsqlOutboxOptions> options,
    ILogger<NpgsqlOutboxMigrator> logger
)
{
    private static readonly CompositeFormat CreateSchemaSqlCommand = CompositeFormat.Parse(
        """CREATE SCHEMA IF NOT EXISTS "{0}";"""
    );

    private static readonly CompositeFormat CreateOutboxSqlCommand = CompositeFormat.Parse(
        LoadSqlResource()
    );

    public async Task MigrateAsync(CancellationToken cancellationToken)
    {
        var schema = options.Value.Schema;

        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        // Ensure schema exists
        var schemaSql = string.Format(CultureInfo.InvariantCulture, CreateSchemaSqlCommand, schema);
        await using (var schemaCmd = new NpgsqlCommand(schemaSql, conn, tx))
        {
            await schemaCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        // Create outbox table and indexes
        var outboxSql = string.Format(CultureInfo.InvariantCulture, CreateOutboxSqlCommand, schema);
        await using (var outboxCmd = new NpgsqlCommand(outboxSql, conn, tx))
        {
            await outboxCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await tx.CommitAsync(cancellationToken);

        logger.LogInformation("Outbox migration completed for schema {Schema}", schema);
    }

    private static string LoadSqlResource()
    {
        var assemblyLocation = Path.GetDirectoryName(
            typeof(NpgsqlOutboxMigrator).Assembly.Location
        )!;
        var filePath = Path.Join(assemblyLocation, "outbox.sql");
        return File.ReadAllText(filePath);
    }
}
