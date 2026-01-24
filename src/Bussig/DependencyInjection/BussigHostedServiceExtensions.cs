using Bussig.Abstractions;
using Bussig.Configuration;
using Bussig.Constants;
using Bussig.Hosting;
using Bussig.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Bussig;

public static class BussigHostedServiceExtensions
{
    public static IServiceCollection AddBussig(
        this IServiceCollection services,
        Action<IBussigRegistrationConfigurator>? options = null
    )
    {
        var configurator = new BussigRegistrationConfigurator();
        options?.Invoke(configurator);

        services.AddBussigCore(configurator);

        return services;
    }

    public static IServiceCollection AddBussigHostedService(this IServiceCollection services)
    {
        services.AddHostedService<BussigHostedService>();
        return services;
    }

    private static void AddBussigCore(
        this IServiceCollection services,
        BussigRegistrationConfigurator configurator
    )
    {
        services.AddSingleton<IBussigRegistrationConfigurator>(configurator);

        // Post-configure to extract values from connection string
        services.ConfigureOptions<PostgresSettingsPostConfigure>();

        services.AddKeyedSingleton<NpgsqlDataSource>(
            ServiceKeys.BussigNpgsql,
            (provider, _) =>
            {
                var settings = provider.GetRequiredService<IOptions<PostgresSettings>>().Value;
                var builder = new NpgsqlConnectionStringBuilder(settings.ConnectionString)
                {
                    Database = settings.Database,
                    SearchPath = settings.Schema,
                };

                return NpgsqlDataSource.Create(builder.ToString());
            }
        );

        services.AddSingleton<IPostgresTransactionAccessor, PostgresTransactionAccessor>();
        services.AddSingleton<IMessageSerializer, SystemTextJsonMessageSerializer>();
        services.AddSingleton<PostgresQueueCreator>();
        services.AddSingleton<IOutgoingMessageSender, PostgresOutgoingMessageSender>();
        services.AddSingleton<PostgresMigrator>();
        services.AddSingleton<IBus, Bus>();
    }
}
