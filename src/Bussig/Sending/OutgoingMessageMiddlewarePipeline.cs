using Bussig.Abstractions.Middleware;
using Microsoft.Extensions.DependencyInjection;

namespace Bussig.Sending;

/// <summary>
/// Builds and executes the outgoing message middleware pipeline.
/// </summary>
public sealed class OutgoingMessageMiddlewarePipeline
{
    private readonly IReadOnlyList<Type> _middlewareTypes;

    public OutgoingMessageMiddlewarePipeline(IReadOnlyList<Type> middlewareTypes)
    {
        _middlewareTypes = middlewareTypes;
    }

    /// <summary>
    /// Executes the middleware pipeline for the given context.
    /// </summary>
    public Task ExecuteAsync(OutgoingMessageContext context)
    {
        var pipeline = BuildPipeline();
        return pipeline(context);
    }

    private OutgoingMessageMiddlewareDelegate BuildPipeline()
    {
        // Start with a terminal delegate that does nothing
        OutgoingMessageMiddlewareDelegate current = _ => Task.CompletedTask;

        // Build the pipeline in reverse order so the first middleware in the list runs first
        for (var i = _middlewareTypes.Count - 1; i >= 0; i--)
        {
            var middlewareType = _middlewareTypes[i];
            var next = current;

            current = context =>
            {
                var middleware = ResolveMiddleware(context.ServiceProvider, middlewareType);
                return middleware.InvokeAsync(context, next);
            };
        }

        return current;
    }

    private static IOutgoingMessageMiddleware ResolveMiddleware(
        IServiceProvider serviceProvider,
        Type middlewareType
    )
    {
        var middleware = serviceProvider.GetService(middlewareType);
        if (middleware is IOutgoingMessageMiddleware outgoingMiddleware)
        {
            return outgoingMiddleware;
        }

        // Try to activate directly if not registered
        middleware = ActivatorUtilities.CreateInstance(serviceProvider, middlewareType);
        if (middleware is IOutgoingMessageMiddleware activated)
        {
            return activated;
        }

        throw new InvalidOperationException(
            $"Could not resolve middleware of type {middlewareType.Name}"
        );
    }

    /// <summary>
    /// Creates a pipeline with the default middleware order.
    /// </summary>
    public static OutgoingMessageMiddlewarePipeline CreateDefault(
        IReadOnlyList<Type> userMiddleware,
        bool attachmentsEnabled
    )
    {
        var allMiddleware = new List<Type>();

        // Default pipeline order:
        // 1. User middleware (logging, tracing, etc.)
        // 2. OutgoingAttachmentMiddleware (optional - uploads MessageData streams)
        // 3. OutgoingSerializationMiddleware (serializes body, merges headers)
        // 4. OutgoingSenderMiddleware (terminal - builds OutgoingMessage and sends)

        allMiddleware.AddRange(userMiddleware);
        if (attachmentsEnabled)
        {
            allMiddleware.Add(typeof(OutgoingAttachmentMiddleware));
        }
        allMiddleware.Add(typeof(OutgoingSerializationMiddleware));
        allMiddleware.Add(typeof(OutgoingSenderMiddleware));

        return new OutgoingMessageMiddlewarePipeline(allMiddleware);
    }
}
