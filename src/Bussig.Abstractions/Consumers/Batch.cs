namespace Bussig.Abstractions;

// ReSharper disable once InconsistentNaming
public interface Batch<out TMessage> : IEnumerable<ConsumerContext<TMessage>>
    where TMessage : class
{
    int Length { get; }
}
