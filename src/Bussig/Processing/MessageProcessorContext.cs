using Bussig.Abstractions;

namespace Bussig.Processing;

public sealed class MessageProcessorContext<TMessage> : ProcessorContext<TMessage>
    where TMessage : class
{
    internal MessageProcessorContext(
        TMessage message,
        MessageEnvelope envelope,
        DeliveryInfo delivery,
        long messageDeliveryId,
        Guid lockId
    )
    {
        Message = message;
        Envelope = envelope;
        Delivery = delivery;
        MessageDeliveryId = messageDeliveryId;
        LockId = lockId;
    }

    public TMessage Message { get; }
    public MessageEnvelope Envelope { get; }
    public DeliveryInfo Delivery { get; }

    // Convenience properties delegating to Envelope
    public Guid MessageId => Envelope.MessageId;
    public Guid? CorrelationId => Envelope.CorrelationId;

    // Convenience properties delegating to Delivery
    public int DeliveryCount => Delivery.DeliveryCount;
    public int MaxDeliveryCount => Delivery.MaxDeliveryCount;
    public DateTimeOffset EnqueuedAt => Delivery.EnqueuedAt;

    internal long MessageDeliveryId { get; }
    internal Guid LockId { get; }
}
