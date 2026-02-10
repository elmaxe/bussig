using Bussig.Abstractions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Bussig.EntityFrameworkCore;

public static class BussigOutboxExtensions
{
    public static OutboxBuilder AddBussigOutbox<TDbContext>(
        this IServiceCollection services,
        Action<OutboxOptions>? configure = null
    )
        where TDbContext : DbContext
    {
        if (configure is not null)
        {
            services.AddOptions<OutboxOptions>().Configure(configure);
        }

        // Move the existing IOutgoingMessageSender registration to a keyed service
        var existingDescriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(IOutgoingMessageSender) && !d.IsKeyedService
        );

        if (existingDescriptor is null)
        {
            throw new InvalidOperationException(
                "No IOutgoingMessageSender registration found. Call AddBussig() before AddBussigOutbox()."
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
        services.AddSingleton<IOutgoingMessageSender, OutboxSender>();

        // Register the transaction context as singleton (AsyncLocal-based)
        services.TryAddSingleton<OutboxTransactionContext>();

        // Register the forwarder background service
        services.AddHostedService<OutboxForwarder<TDbContext>>();

        return new OutboxBuilder(services);
    }
}
