using Bussig.Abstractions;

namespace Bussig.Processing;

public sealed class MessageProcessorContext<TMessage> : ProcessorContext<TMessage>
    where TMessage : class
{
    internal MessageProcessorContext(
        TMessage message,
        Guid messageId,
        int deliveryCount,
        int maxDeliveryCount,
        DateTimeOffset enqueuedAt,
        long messageDeliveryId,
        Guid lockId
    )
    {
        Message = message;
        MessageId = messageId;
        DeliveryCount = deliveryCount;
        MaxDeliveryCount = maxDeliveryCount;
        EnqueuedAt = enqueuedAt;
        MessageDeliveryId = messageDeliveryId;
        LockId = lockId;
    }

    public TMessage Message { get; }
    public Guid MessageId { get; }
    public int DeliveryCount { get; }
    public int MaxDeliveryCount { get; }
    public DateTimeOffset EnqueuedAt { get; }

    internal long MessageDeliveryId { get; }
    internal Guid LockId { get; }
}
