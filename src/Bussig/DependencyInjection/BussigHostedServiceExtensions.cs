using Bussig.Abstractions;
using Bussig.Configuration;
using Bussig.Constants;
using Bussig.Hosting;
using Bussig.Processing;
using Bussig.Processing.Middleware;
using Bussig.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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
        services.AddSingleton(configurator);

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
        services.AddSingleton<IBus, Bus>();

        // Register message receiver for consuming messages
        services.AddSingleton<PostgresMessageReceiver>();

        // Register built-in middleware (unified pipeline for both single and batch)
        RegisterBuiltInMiddleware(services);

        // Register user-defined global middleware
        RegisterUserMiddleware(services, configurator);

        // Register consumer factory for creating queue consumers
        services.AddSingleton<QueueConsumerFactory>(provider =>
        {
            var scopeFactory = provider.GetRequiredService<IServiceScopeFactory>();
            var receiver = provider.GetRequiredService<PostgresMessageReceiver>();
            var loggerFactory = provider.GetRequiredService<ILoggerFactory>();

            return new QueueConsumerFactory(
                scopeFactory,
                receiver,
                loggerFactory,
                configurator.GlobalMiddleware
            );
        });

        // Register each processor as scoped service
        foreach (var registration in configurator.ProcessorRegistrations)
        {
            services.AddScoped(registration.ProcessorType);
        }
    }

    private static void RegisterBuiltInMiddleware(IServiceCollection services)
    {
        // Unified middleware (works for both single-message and batch processing)
        services.AddScoped<ErrorHandlingMiddleware>();
        services.AddScoped<LockRenewalMiddleware>();
        services.AddScoped<DeserializationMiddleware>();
        services.AddScoped<EnvelopeMiddleware>();
        services.AddScoped<ProcessorInvocationMiddleware>();
    }

    private static void RegisterUserMiddleware(
        IServiceCollection services,
        BussigRegistrationConfigurator configurator
    )
    {
        // Register global middleware types
        foreach (var middlewareType in configurator.GlobalMiddleware)
        {
            services.AddScoped(middlewareType);
        }

        // Register per-processor middleware types
        foreach (var registration in configurator.ProcessorRegistrations)
        {
            foreach (var middlewareType in registration.Options.Middleware.MiddlewareTypes)
            {
                if (services.All(d => d.ServiceType != middlewareType))
                {
                    services.AddScoped(middlewareType);
                }
            }
        }
    }
}
