namespace Bussig.Abstractions;

/// <summary>
/// Used for internal IoC/DI, should not be used
/// </summary>
public interface IConsumer;

public interface IConsumer<in TMessage> : IConsumer
    where TMessage : class
{
    Task Consume(ConsumerContext<TMessage> context, CancellationToken cancellationToken = default);
}

public interface IConsumer<in TMessage, TSendMessage> : IConsumer
    where TMessage : class
{
    Task<TSendMessage> Consume(
        ConsumerContext<TMessage> context,
        CancellationToken cancellationToken = default
    );
}
