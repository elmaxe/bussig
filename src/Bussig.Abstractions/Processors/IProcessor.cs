using Bussig.Abstractions.Messages;

namespace Bussig.Abstractions;

/// <summary>
/// Used for internal IoC/DI, should not be used
/// </summary>
public interface IProcessor;

public interface IProcessor<in TMessage> : IProcessor
    where TMessage : class
{
    Task ProcessAsync(
        ProcessorContext<TMessage> context,
        CancellationToken cancellationToken = default
    );
}

public interface IProcessor<in TMessage, TSendMessage> : IProcessor
    where TMessage : class
    where TSendMessage : class, IMessage
{
    Task<TSendMessage> ProcessAsync(
        ProcessorContext<TMessage> context,
        CancellationToken cancellationToken = default
    );
}
