using Bussig.Abstractions.Host;
using Bussig.Postgres;
using Bussig.Postgres.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Bussig;

public sealed class MigrationHostedService(
    IOptions<MigrationOptions> options,
    IOptions<PostgresSettings> transportOptions,
    PostgresMigrator migrator,
    ILogger<MigrationHostedService> logger
) : IHostedService
{
    private readonly MigrationOptions _options = options.Value;
    private readonly PostgresSettings _postgresSettings = transportOptions.Value;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var database = _postgresSettings.Database;
        var schema = _postgresSettings.Schema;

        if (_options.CreateDatabase)
        {
            logger.LogInformation("Creating database {Database}", database);

            await migrator.CreateDatabase(cancellationToken);
        }

        if (_options.CreateSchema)
        {
            logger.LogInformation(
                "Creating schema {Schema} for database {Database}",
                schema,
                database
            );

            await migrator.CreateSchema(cancellationToken);
        }

        if (_options.CreateInfrastructure)
        {
            logger.LogInformation(
                "Creating infrastructure for schema {Schema} and database {Database}",
                schema,
                database
            );

            await migrator.CreateInfrastructure(cancellationToken);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_options.DeleteDatabase)
        {
            logger.LogInformation("Deleting database {Database}", _postgresSettings.Database);

            await migrator.DeleteDatabase(cancellationToken);
        }
    }
}
