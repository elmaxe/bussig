using Bussig.Abstractions;
using Bussig.Constants;
using Bussig.Postgres;
using Bussig.Postgres.Configuration;
using Bussig.Postgres.Serialization;
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
        // if (services.All(x => x.ServiceType != typeof(IOptions<PostgresSettings>)))
        // {
        //     throw new InvalidOperationException($"No {nameof(PostgresSettings)} registered.");
        // }
        services.AddSingleton<IBussigRegistrationConfigurator>(configurator);

        services.AddKeyedSingleton<NpgsqlDataSource>(
            ServiceKeys.BussigNpgsql,
            (provider, _) =>
            {
                var settings = provider.GetRequiredService<IOptions<PostgresSettings>>().Value;
                var builder = new NpgsqlConnectionStringBuilder(settings.ConnectionString);

                if (!string.IsNullOrWhiteSpace(settings.Database))
                {
                    builder.Database = settings.Database;
                }

                builder.SearchPath = !string.IsNullOrWhiteSpace(settings.Schema)
                    ? settings.Schema
                    : TransportConstants.DefaultSchemaName;

                return NpgsqlDataSource.Create(builder.ToString());
            }
        );
        // TODO FIX THIS
        services.AddSingleton<TransportOptions>(provider =>
        {
            var settings = provider.GetRequiredService<IOptions<PostgresSettings>>().Value;
            var builder = new NpgsqlConnectionStringBuilder(settings.ConnectionString);
            builder.SearchPath = settings.Schema ?? TransportConstants.DefaultSchemaName;

            return new TransportOptions
            {
                Database = builder.Database!,
                Schema = builder.SearchPath,
            };
        });

        services.AddSingleton<IPostgresTransactionAccessor, PostgresTransactionAccessor>();
        services.AddSingleton<IMessageSerializer, SystemTextJsonMessageSerializer>();
        services.AddSingleton<PostgresQueueCreator>();
        services.AddSingleton<IOutgoingMessageSender, PostgresOutgoingMessageSender>();
        services.AddSingleton<PostgresMigrator>();
        services.AddSingleton<IBus, Bus>();
    }
}
