using Bussig.Abstractions.Messages;

namespace Bussig.Abstractions;

// ReSharper disable once InconsistentNaming
public interface Batch<out TMessage> : IEnumerable<ProcessorContext<TMessage>>
    where TMessage : class, IMessage
{
    int Length { get; }
}
