using Bussig.Abstractions;
using Bussig.Constants;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Bussig.Postgres;

public class PostgresMigrator(
    [FromKeyedServices(ServiceKeys.BussigNpgsql)] NpgsqlDataSource npgsqlDataSource,
    ILogger<PostgresMigrator> logger
)
{
    public async Task CreateSchema(IPostgresSettings options, CancellationToken cancellationToken)
    {
        await using var connection = await npgsqlDataSource.OpenConnectionAsync(cancellationToken);

        await using var command = new NpgsqlCommand(
            string.Format(CreateSchemaSqlCommand, options.Schema),
            connection
        );
        await command.ExecuteScalarAsync(cancellationToken);

        logger.LogDebug("Schema {Schema} created", options.Schema);
    }

    public async Task CreateInfrastructure(
        IPostgresSettings options,
        CancellationToken cancellationToken
    )
    {
        await using var connection = await npgsqlDataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var formattedCmdStr = string.Format(CreateInfrastructureSqlCommand, options.Schema);
        await using (var command = new NpgsqlCommand(formattedCmdStr, connection, transaction))
        {
            await command.ExecuteScalarAsync(cancellationToken);
        }

        await transaction.CommitAsync(CancellationToken.None);

        logger.LogDebug("Transport infrastructure in schema {Schema} upserted", options.Schema);
    }

    private const string CreateSchemaSqlCommand =
        //lang=postgresql
        """
            CREATE SCHEMA IF NOT EXISTS "{0}";
            """;

    private static readonly string CreateInfrastructureSqlCommand = LoadSqlResource();

    private static string LoadSqlResource()
    {
        var fileStream = File.OpenRead(
            Path.Join(Directory.GetCurrentDirectory(), "infrastructure.sql")
        );
        using var reader = new StreamReader(fileStream);
        return reader.ReadToEnd();
    }
}
