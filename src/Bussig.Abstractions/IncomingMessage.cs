namespace Bussig.Abstractions;

/// <summary>
/// Represents a message received from a queue for processing.
/// </summary>
public sealed record IncomingMessage
{
    public required Guid MessageId { get; init; }
    public required long MessageDeliveryId { get; init; }
    public required Guid LockId { get; init; }
    public required byte[] Body { get; init; }
    public required string? Headers { get; init; }
    public required string? MessageDeliveryHeaders { get; init; }
    public required int DeliveryCount { get; init; }
    public required int MaxDeliveryCount { get; init; }
    public required int MessageVersion { get; init; }
    public required DateTimeOffset EnqueuedAt { get; init; }
    public required DateTimeOffset? LastDeliveredAt { get; init; }
    public required DateTimeOffset VisibleAt { get; init; }
    public required DateTimeOffset? ExpirationTime { get; init; }
}
