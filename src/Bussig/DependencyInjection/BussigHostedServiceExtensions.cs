using Bussig.Abstractions;
using Bussig.Attachments;
using Bussig.Configuration;
using Bussig.Constants;
using Bussig.Hosting;
using Bussig.Processing;
using Bussig.Processing.Middleware;
using Bussig.Sending;
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
        Action<IBussigRegistrationConfigurator, IServiceCollection>? options = null
    )
    {
        var configurator = new BussigRegistrationConfigurator();
        options?.Invoke(configurator, services);

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
        // services.AddSingleton<IMessageAttachmentRepository, InMemoryMessageAttachmentRepository>();

        // Register message receiver for consuming messages
        services.AddSingleton<PostgresMessageReceiver>();
        services.AddSingleton<IMessageLockRenewer>(sp =>
            sp.GetRequiredService<PostgresMessageReceiver>()
        );

        // Register attachment options
        if (configurator.AttachmentsEnabled)
        {
            if (configurator.ConfigureAttachmentOptions is not null)
            {
                services.Configure(configurator.ConfigureAttachmentOptions);
            }
            else
            {
                services.Configure<AttachmentOptions>(_ => { });
            }
        }

        // Register built-in middleware (unified pipeline for both single and batch)
        RegisterBuiltInMiddleware(services, configurator.AttachmentsEnabled);

        // Register user-defined global middleware
        RegisterUserMiddleware(services, configurator);

        // Register built-in outgoing middleware
        RegisterBuiltInOutgoingMiddleware(services, configurator.AttachmentsEnabled);

        // Register user-defined outgoing middleware
        RegisterUserOutgoingMiddleware(services, configurator);

        // Register the outgoing message pipeline
        services.AddSingleton(provider =>
            OutgoingMessageMiddlewarePipeline.CreateDefault(
                configurator.GlobalSendMiddleware,
                configurator.AttachmentsEnabled
            )
        );

        // Register distributed lock manager
        services.AddSingleton<IDistributedLockManager, DistributedLockManager>();

        // Register consumer factory for creating queue consumers
        services.AddSingleton<QueueConsumerFactory>(provider =>
        {
            var scopeFactory = provider.GetRequiredService<IServiceScopeFactory>();
            var receiver = provider.GetRequiredService<PostgresMessageReceiver>();
            var lockManager = provider.GetRequiredService<IDistributedLockManager>();
            var loggerFactory = provider.GetRequiredService<ILoggerFactory>();

            return new QueueConsumerFactory(
                scopeFactory,
                receiver,
                lockManager,
                loggerFactory,
                configurator.GlobalMiddleware,
                configurator.AttachmentsEnabled
            );
        });

        // Register each processor as scoped service
        foreach (var registration in configurator.ProcessorRegistrations)
        {
            services.AddScoped(registration.ProcessorType);
        }
    }

    private static void RegisterBuiltInMiddleware(
        IServiceCollection services,
        bool attachmentsEnabled
    )
    {
        // Unified middleware (works for both single-message and batch processing)
        services.AddScoped<ErrorHandlingMiddleware>();
        services.AddScoped<LockRenewalMiddleware>();
        services.AddScoped<DeserializationMiddleware>();
        services.AddScoped<EnvelopeMiddleware>();
        if (attachmentsEnabled)
        {
            services.AddScoped<AttachmentMiddleware>();
        }
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

    private static void RegisterBuiltInOutgoingMiddleware(
        IServiceCollection services,
        bool attachmentsEnabled
    )
    {
        if (attachmentsEnabled)
        {
            services.AddScoped<OutgoingAttachmentMiddleware>();
        }
        services.AddScoped<OutgoingSerializationMiddleware>();
        services.AddScoped<OutgoingSenderMiddleware>();
    }

    private static void RegisterUserOutgoingMiddleware(
        IServiceCollection services,
        BussigRegistrationConfigurator configurator
    )
    {
        foreach (var middlewareType in configurator.GlobalSendMiddleware)
        {
            if (services.All(d => d.ServiceType != middlewareType))
            {
                services.AddScoped(middlewareType);
            }
        }
    }
}
