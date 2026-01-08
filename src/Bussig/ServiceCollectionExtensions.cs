using Bussig.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace Bussig;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddBussig(
        this IServiceCollection services,
        Action<IBussigRegistrationConfigurator>? options = null
    )
    {
        var configurator = new BussigRegistrationConfigurator();
        options?.Invoke(configurator);

        services.AddBussigCore(configurator);

        services.AddBussigHostedService();

        return services;
    }

    private static void AddBussigHostedService(this IServiceCollection services)
    {
        services.AddHostedService<BussigHostedService>();
    }

    private static void AddBussigCore(
        this IServiceCollection services,
        IBussigRegistrationConfigurator configurator
    )
    {
        services.AddSingleton<IPostgresSettings, PostgresSettings>(_ => new PostgresSettings(
            configurator.ConnectionString
        ));
        services.AddNpgsqlDataSource(
            configurator.ConnectionString,
            serviceKey: BussigServiceKeys.BussigNpgsql
        );
    }
}
