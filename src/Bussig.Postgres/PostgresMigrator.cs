using System.Globalization;
using System.Text;
using Bussig.Constants;
using Bussig.Postgres.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Bussig.Postgres;

public class PostgresMigrator(
    [FromKeyedServices(ServiceKeys.BussigNpgsql)] NpgsqlDataSource npgsqlDataSource,
    IOptions<PostgresSettings> settings,
    ILogger<PostgresMigrator> logger
)
{
    private readonly PostgresSettings _settings = settings.Value;

    private static readonly CompositeFormat DatabaseExistsSqlCommand = CompositeFormat.Parse(
        """SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname = '{0}';"""
    );
    private static readonly CompositeFormat CreateDatabaseSqlCommand = CompositeFormat.Parse(
        """CREATE DATABASE "{0}";"""
    );
    private static readonly CompositeFormat CreateSchemaSqlCommand = CompositeFormat.Parse(
        """CREATE SCHEMA IF NOT EXISTS "{0}";"""
    );
    private static readonly CompositeFormat CreateInfrastructureSqlCommand = CompositeFormat.Parse(
        LoadSqlResource()
    );
    private static readonly CompositeFormat DropDatabaseSqlCommand = CompositeFormat.Parse(
        """DROP DATABASE IF EXISTS "{0}" WITH (force)"""
    );

    public async Task CreateDatabase(CancellationToken cancellationToken)
    {
        var connectionString = new NpgsqlConnectionStringBuilder(_settings.ConnectionString)
        {
            Database = "postgres",
        }.ToString();

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var checkCommand = new NpgsqlCommand(
            string.Format(
                CultureInfo.InvariantCulture,
                DatabaseExistsSqlCommand,
                _settings.Database
            ),
            connection
        );
        var result = (long?)await checkCommand.ExecuteScalarAsync(cancellationToken);
        if (result is 1)
        {
            logger.LogInformation("Database {Database} already exists", _settings.Database);
        }
        else
        {
            await using var createCommand = new NpgsqlCommand(
                string.Format(
                    CultureInfo.InvariantCulture,
                    CreateDatabaseSqlCommand,
                    _settings.Database
                ),
                connection
            );
            await createCommand.ExecuteNonQueryAsync(cancellationToken);

            logger.LogDebug("Database {Database} created", _settings.Database);
        }
    }

    public async Task CreateSchema(CancellationToken cancellationToken)
    {
        await using var connection = await npgsqlDataSource.OpenConnectionAsync(cancellationToken);

        await using var command = new NpgsqlCommand(
            string.Format(CultureInfo.InvariantCulture, CreateSchemaSqlCommand, _settings.Schema),
            connection
        );
        await command.ExecuteScalarAsync(cancellationToken);

        logger.LogDebug("Schema {Schema} created", _settings.Schema);
    }

    public async Task CreateInfrastructure(CancellationToken cancellationToken)
    {
        await using var connection = await npgsqlDataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var formattedCmdStr = string.Format(
            CultureInfo.InvariantCulture,
            CreateInfrastructureSqlCommand,
            _settings.Schema
        );
        await using (var command = new NpgsqlCommand(formattedCmdStr, connection, transaction))
        {
            await command.ExecuteScalarAsync(cancellationToken);
        }

        await transaction.CommitAsync(CancellationToken.None);

        logger.LogDebug("Transport infrastructure in schema {Schema} upserted", _settings.Schema);
    }

    public async Task DeleteDatabase(CancellationToken cancellationToken)
    {
        await using var connection = await npgsqlDataSource.OpenConnectionAsync(cancellationToken);

        await using var command = new NpgsqlCommand(
            string.Format(CultureInfo.InvariantCulture, DropDatabaseSqlCommand, _settings.Database)
        );

        await command.ExecuteScalarAsync(cancellationToken);

        logger.LogInformation("Database {Database} deleted", _settings.Database);
    }

    private static string LoadSqlResource()
    {
        var assemblyLocation = Path.GetDirectoryName(typeof(PostgresMigrator).Assembly.Location)!;
        var filePath = Path.Join(assemblyLocation, "infrastructure.sql");
        return File.ReadAllText(filePath);
    }
}
