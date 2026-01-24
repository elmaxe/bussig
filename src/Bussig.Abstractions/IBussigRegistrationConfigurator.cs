namespace Bussig.Abstractions;

public interface IBussigRegistrationConfigurator
{
    IReadOnlyCollection<Type> Messages { get; }

    void AddMessage<TMessage>(Action<QueueOptions>? configure = null);
    void AddMessage(Type message, Action<QueueOptions>? configure = null);
    bool TryGetQueueOptions(Type message, out QueueOptions options);
}
