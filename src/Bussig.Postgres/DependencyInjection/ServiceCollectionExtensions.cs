using Bussig.Abstractions;
using Bussig.Constants;
using Bussig.Postgres;
using Bussig.Postgres.Configuration;
using Bussig.Postgres.Serialization;
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

        services.AddBussigPostgres(configurator);

        if (configurator.CreateQueuesOnStartup)
        {
            services.AddBussigStartupHostedService();
        }

        return services;
    }

    public static IServiceCollection AddBussigHostedService(this IServiceCollection services)
    {
        services.AddHostedService<BussigHostedService>();
        return services;
    }

    private static void AddBussigStartupHostedService(this IServiceCollection services)
    {
        services.AddHostedService<BussigStartupHostedService>();
    }

    private static void AddBussigPostgres(
        this IServiceCollection services,
        IBussigRegistrationConfigurator configurator
    )
    {
        if (string.IsNullOrWhiteSpace(configurator.ConnectionString))
        {
            throw new InvalidOperationException(
                "Bussig requires a Postgres connection string. Set configurator.ConnectionString."
            );
        }

        services.AddSingleton<IBussigRegistrationConfigurator>(configurator);

        var settings =
            configurator.Settings
            ?? new PostgresSettings(configurator.ConnectionString, configurator.Schema);

        services.AddSingleton<IPostgresSettings>(settings);
        services.AddNpgsqlDataSource(
            configurator.ConnectionString,
            serviceKey: ServiceKeys.BussigNpgsql
        );

        services.AddSingleton<PostgresConnectionContext>();
        services.AddSingleton<IPostgresConnectionContext>(sp =>
            sp.GetRequiredService<PostgresConnectionContext>()
        );
        services.AddSingleton<IPostgresTransactionAccessor, PostgresTransactionAccessor>();
        services.AddSingleton<IMessageSerializer, SystemTextJsonMessageSerializer>();
        services.AddSingleton<PostgresQueueCreator>();
        services.AddSingleton<IOutgoingMessageSender, PostgresOutgoingMessageSender>();
        services.AddSingleton<PostgresMigrator>();
        services.AddSingleton<BussigStartup>();
        services.AddSingleton<IBus, Bus>();
    }
}
