using Bussig.Abstractions;
using Bussig.Abstractions.Host;
using Bussig.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Bussig.Hosting;

public sealed class MigrationHostedService(
    IOptions<MigrationOptions> options,
    IOptions<PostgresSettings> transportOptions,
    IServiceScopeFactory serviceScopeFactory,
    ILogger<MigrationHostedService> logger
) : IHostedService
{
    private readonly MigrationOptions _options = options.Value;
    private readonly PostgresSettings _postgresSettings = transportOptions.Value;

    private readonly IPostgresMigrator _migrator = serviceScopeFactory
        .CreateScope()
        .ServiceProvider.GetRequiredService<IPostgresMigrator>();

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var database = _postgresSettings.Database;
        var schema = _postgresSettings.Schema;

        if (_options.CreateDatabase)
        {
            logger.LogInformation("Creating database {Database}", database);

            await _migrator.CreateDatabase(cancellationToken);
        }

        if (_options.CreateSchema)
        {
            logger.LogInformation(
                "Creating schema {Schema} for database {Database}",
                schema,
                database
            );

            await _migrator.CreateSchema(cancellationToken);
        }

        if (_options.CreateInfrastructure)
        {
            logger.LogInformation(
                "Creating infrastructure for schema {Schema} and database {Database}",
                schema,
                database
            );

            await _migrator.CreateInfrastructure(cancellationToken);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_options.DeleteDatabase)
        {
            logger.LogInformation("Deleting database {Database}", _postgresSettings.Database);

            await _migrator.DeleteDatabase(cancellationToken);
        }
    }
}
