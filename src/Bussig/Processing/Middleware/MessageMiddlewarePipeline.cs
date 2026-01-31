using Bussig.Abstractions.Middleware;
using Microsoft.Extensions.DependencyInjection;

namespace Bussig.Processing.Middleware;

/// <summary>
/// Builds and executes the message processing middleware pipeline.
/// </summary>
internal sealed class MessageMiddlewarePipeline
{
    private readonly IReadOnlyList<Type> _middlewareTypes;
    private readonly IServiceProvider _serviceProvider;

    public MessageMiddlewarePipeline(
        IReadOnlyList<Type> middlewareTypes,
        IServiceProvider serviceProvider
    )
    {
        _middlewareTypes = middlewareTypes;
        _serviceProvider = serviceProvider;
    }

    /// <summary>
    /// Executes the middleware pipeline for the given context.
    /// </summary>
    public Task ExecuteAsync(MessageContext context)
    {
        var pipeline = BuildPipeline();
        return pipeline(context);
    }

    private MessageMiddlewareDelegate BuildPipeline()
    {
        // Start with a terminal delegate that does nothing
        MessageMiddlewareDelegate current = _ => Task.CompletedTask;

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

    private static IMessageMiddleware ResolveMiddleware(
        IServiceProvider serviceProvider,
        Type middlewareType
    )
    {
        var middleware = serviceProvider.GetService(middlewareType);
        if (middleware is IMessageMiddleware messageMiddleware)
        {
            return messageMiddleware;
        }

        // Try to activate directly if not registered
        middleware = ActivatorUtilities.CreateInstance(serviceProvider, middlewareType);
        if (middleware is IMessageMiddleware activated)
        {
            return activated;
        }

        throw new InvalidOperationException(
            $"Could not resolve middleware of type {middlewareType.Name}"
        );
    }

    /// <summary>
    /// Creates a pipeline builder with the default middleware order.
    /// </summary>
    public static MessageMiddlewarePipeline CreateDefault(
        IReadOnlyList<Type> globalMiddleware,
        IReadOnlyList<Type> processorMiddleware,
        IServiceProvider serviceProvider
    )
    {
        var allMiddleware = new List<Type>();

        // Default pipeline order:
        // 1. ErrorHandlingMiddleware (outermost - catches exceptions)
        // 2. LockRenewalMiddleware (renews lock during processing)
        // 3. Global user middleware
        // 4. Per-processor user middleware
        // 5. DeserializationMiddleware
        // 6. EnvelopeMiddleware
        // 7. ProcessorInvocationMiddleware (terminal)

        allMiddleware.Add(typeof(ErrorHandlingMiddleware));
        allMiddleware.Add(typeof(LockRenewalMiddleware));
        allMiddleware.AddRange(globalMiddleware);
        allMiddleware.AddRange(processorMiddleware);
        allMiddleware.Add(typeof(DeserializationMiddleware));
        allMiddleware.Add(typeof(EnvelopeMiddleware));
        allMiddleware.Add(typeof(ProcessorInvocationMiddleware));

        return new MessageMiddlewarePipeline(allMiddleware, serviceProvider);
    }
}
