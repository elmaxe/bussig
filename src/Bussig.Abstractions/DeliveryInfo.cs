namespace Bussig.Abstractions;

/// <summary>
/// Delivery-specific (mutable) data separate from the immutable message envelope.
/// </summary>
public sealed record DeliveryInfo
{
    /// <summary>
    /// The number of times this message has been delivered.
    /// </summary>
    public required int DeliveryCount { get; init; }

    /// <summary>
    /// The maximum number of delivery attempts before moving to dead letter queue.
    /// </summary>
    public required int MaxDeliveryCount { get; init; }

    /// <summary>
    /// When the message was enqueued.
    /// </summary>
    public required DateTimeOffset EnqueuedAt { get; init; }

    /// <summary>
    /// When the message was last delivered for processing.
    /// </summary>
    public DateTimeOffset? LastDeliveredAt { get; init; }

    /// <summary>
    /// Delivery-specific headers (e.g., error information).
    /// </summary>
    public IReadOnlyDictionary<string, string>? DeliveryHeaders { get; init; }
}
