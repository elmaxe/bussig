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
}
