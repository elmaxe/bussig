namespace Bussig.Abstractions;

// ReSharper disable once InconsistentNaming
public interface ConsumerContext<out TMessage>
    where TMessage : class
{
    TMessage Message { get; }
}
