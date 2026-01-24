using Bussig.Abstractions;
using Bussig.Abstractions.Host;
using Bussig.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace Bussig;

public static class MigrationConfigurationExtensions
{
    public static IServiceCollection AddMigrationHostedService(
        this IServiceCollection services,
        Action<MigrationOptions>? configure = null
    )
    {
        services
            .AddOptions<MigrationOptions>()
            .Configure(options =>
            {
                options.CreateDatabase = true;
                options.CreateInfrastructure = true;
                options.CreateSchema = true;
                options.DeleteDatabase = false;

                configure?.Invoke(options);
            });

        services.AddHostedService<MigrationHostedService>();
        services.AddScoped<IPostgresMigrator, PostgresMigrator>();

        return services;
    }
}
