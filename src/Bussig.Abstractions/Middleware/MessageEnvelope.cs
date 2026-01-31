namespace Bussig.Abstractions.Middleware;

/// <summary>
/// Virtual envelope that combines message metadata with the deserialized payload.
/// Constructed by middleware after deserialization, not stored separately.
/// </summary>
public class MessageEnvelope
{
    /// <summary>
    /// The unique message identifier.
    /// </summary>
    public required Guid MessageId { get; init; }

    /// <summary>
    /// The message type URN from headers.
    /// </summary>
    public required string MessageType { get; init; }

    /// <summary>
    /// When the message was enqueued.
    /// </summary>
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Correlation ID for tracking related messages across services.
    /// </summary>
    public Guid? CorrelationId { get; init; }

    /// <summary>
    /// Custom headers attached to the message.
    /// </summary>
    public IReadOnlyDictionary<string, string> Headers { get; init; } =
        new Dictionary<string, string>();

    /// <summary>
    /// The deserialized message payload.
    /// </summary>
    public required object Payload { get; init; }

    /// <summary>
    /// Gets the payload cast to the specified type.
    /// </summary>
    public TMessage GetPayload<TMessage>()
        where TMessage : class => (TMessage)Payload;
}

/// <summary>
/// Strongly-typed envelope with the deserialized message payload.
/// </summary>
/// <typeparam name="TMessage">The message type.</typeparam>
public sealed class MessageEnvelope<TMessage> : MessageEnvelope
    where TMessage : class
{
    /// <summary>
    /// The strongly-typed deserialized message payload.
    /// </summary>
    public TMessage TypedPayload => (TMessage)Payload;
}
