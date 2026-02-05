namespace Bussig.Abstractions;

// ReSharper disable once InconsistentNaming
public interface ProcessorContext<out TMessage>
    where TMessage : class
{
    TMessage Message { get; }
    MessageEnvelope Envelope { get; }
    DeliveryInfo Delivery { get; }
}
