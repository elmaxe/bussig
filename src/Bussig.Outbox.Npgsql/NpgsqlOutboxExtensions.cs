using Bussig.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Npgsql;

namespace Bussig.Outbox.Npgsql;

public static class NpgsqlOutboxExtensions
{
    public static IServiceCollection AddBussigNpgsqlOutbox(
        this IServiceCollection services,
        Action<NpgsqlOutboxOptions> configure
    )
    {
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions<NpgsqlOutboxOptions>().Configure(configure);

        // Move the existing IOutgoingMessageSender registration to a keyed service
        // so the decorator can resolve the original ("inner") sender.
        var existingDescriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(IOutgoingMessageSender) && !d.IsKeyedService
        );

        if (existingDescriptor is null)
        {
            throw new InvalidOperationException(
                "No IOutgoingMessageSender registration found. Call AddBussig() before AddBussigNpgsqlOutbox()."
            );
        }

        services.Remove(existingDescriptor);

        // Re-register the original sender as a keyed service
        if (existingDescriptor.ImplementationType is not null)
        {
            services.AddKeyedSingleton(
                typeof(IOutgoingMessageSender),
                OutboxServiceKeys.InnerSender,
                existingDescriptor.ImplementationType
            );
        }
        else if (existingDescriptor.ImplementationFactory is not null)
        {
            services.AddKeyedSingleton<IOutgoingMessageSender>(
                OutboxServiceKeys.InnerSender,
                (sp, _) => (IOutgoingMessageSender)existingDescriptor.ImplementationFactory(sp)
            );
        }
        else if (existingDescriptor.ImplementationInstance is not null)
        {
            services.AddKeyedSingleton(
                typeof(IOutgoingMessageSender),
                OutboxServiceKeys.InnerSender,
                existingDescriptor.ImplementationInstance
            );
        }

        // Register the outbox decorator as the primary IOutgoingMessageSender
        services.AddSingleton<IOutgoingMessageSender, NpgsqlOutboxSender>();

        // Register a keyed NpgsqlDataSource for the outbox database
        services.AddKeyedSingleton<NpgsqlDataSource>(
            OutboxServiceKeys.OutboxNpgsqlDataSource,
            (provider, _) =>
            {
                var options = provider
                    .GetRequiredService<Microsoft.Extensions.Options.IOptions<NpgsqlOutboxOptions>>()
                    .Value;
                return NpgsqlDataSource.Create(options.ConnectionString);
            }
        );

        // Register the transaction context as singleton (AsyncLocal-based)
        services.TryAddSingleton<NpgsqlOutboxTransactionContext>();

        // Register migrator and migration hosted service
        services.AddSingleton<NpgsqlOutboxMigrator>();
        services.AddHostedService<NpgsqlOutboxHostedService>();

        // Register the forwarder background service
        services.AddHostedService<NpgsqlOutboxForwarder>();

        return services;
    }
}
