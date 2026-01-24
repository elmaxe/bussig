using System.Reflection;

namespace Bussig.Abstractions;

public interface IBussigRegistrationConfigurator
{
    IReadOnlyCollection<Type> Messages { get; }

    void AddMessage<TMessage>(Action<QueueOptions>? configure = null);
    void AddMessage(Type message, Action<QueueOptions>? configure = null);
    bool TryGetQueueOptions(Type message, out QueueOptions options);

    void AddProcessor<TMessage, TProcessor>(Action<ConsumerOptions>? configure = null)
        where TMessage : class
        where TProcessor : class, IProcessor<TMessage>;

    void AddProcessor(
        Type messageType,
        Type processorType,
        Action<ConsumerOptions>? configure = null
    );

    /// <summary>
    /// Registers a processor, inferring the message type from the processor's IProcessor interface.
    /// </summary>
    void AddProcessor<TProcessor>(Action<ConsumerOptions>? configure = null)
        where TProcessor : class, IProcessor;

    /// <summary>
    /// Registers a processor, inferring the message type from the processor's IProcessor interface.
    /// </summary>
    void AddProcessor(Type processorType, Action<ConsumerOptions>? configure = null);

    /// <summary>
    /// Scans an assembly for all IProcessor implementations and registers them.
    /// </summary>
    void AddProcessorsFromAssembly(
        Assembly assembly,
        Action<ConsumerOptions>? defaultConfigure = null
    );
}
