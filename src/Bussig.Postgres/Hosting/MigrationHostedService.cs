using Bussig.Abstractions;
using Bussig.Abstractions.Host;
using Bussig.Postgres;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Bussig;

public sealed class MigrationHostedService(
    IOptions<MigrationOptions> options,
    IOptions<TransportOptions> transportOptions,
    PostgresMigrator migrator,
    ILogger<MigrationHostedService> logger
) : IHostedService
{
    private readonly MigrationOptions _options = options.Value;
    private readonly TransportOptions _transportOptions = transportOptions.Value;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var database = _transportOptions.Database;
        var schema = _transportOptions.Schema;

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
            logger.LogInformation("Deleting database {Database}", _transportOptions.Database);

            await migrator.DeleteDatabase(cancellationToken);
        }
    }
}
