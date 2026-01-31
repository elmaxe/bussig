using System.Reflection;
using Bussig.Abstractions.Messages;
using Bussig.Abstractions.Middleware;
using Bussig.Abstractions.Options;

namespace Bussig.Abstractions;

public interface IBussigRegistrationConfigurator
{
    IReadOnlyCollection<Type> Messages { get; }

    void AddMessage<TMessage>(Action<QueueOptions>? configure = null);
    void AddMessage(Type message, Action<QueueOptions>? configure = null);
    bool TryGetQueueOptions(Type message, out QueueOptions options);

    void AddProcessor<TMessage, TProcessor>(Action<ProcessorOptions>? configure = null)
        where TMessage : class, IMessage
        where TProcessor : class, IProcessor<TMessage>;

    void AddProcessor(
        Type messageType,
        Type processorType,
        Action<ProcessorOptions>? configure = null
    );

    /// <summary>
    /// Registers a processor, inferring the message type from the processor's IProcessor interface.
    /// </summary>
    void AddProcessor<TProcessor>(Action<ProcessorOptions>? configure = null)
        where TProcessor : class, IProcessor;

    /// <summary>
    /// Registers a processor, inferring the message type from the processor's IProcessor interface.
    /// </summary>
    void AddProcessor(Type processorType, Action<ProcessorOptions>? configure = null);

    /// <summary>
    /// Scans an assembly for all IProcessor implementations and registers them.
    /// </summary>
    void AddProcessorsFromAssembly(
        Assembly assembly,
        Action<ProcessorOptions>? defaultConfigure = null
    );

    /// <summary>
    /// Adds a global middleware that runs for all processors (both single-message and batch).
    /// Middleware is executed in the order it is added.
    /// </summary>
    /// <typeparam name="TMiddleware">The middleware type implementing IMessageMiddleware.</typeparam>
    void UseMiddleware<TMiddleware>()
        where TMiddleware : class, IMessageMiddleware;

    /// <summary>
    /// Adds a global middleware that runs for all processors (both single-message and batch).
    /// Middleware is executed in the order it is added.
    /// </summary>
    /// <param name="middlewareType">The middleware type implementing IMessageMiddleware.</param>
    void UseMiddleware(Type middlewareType);
}
