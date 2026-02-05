namespace Bussig.Abstractions;

/// <summary>
/// Immutable envelope containing system metadata set at send time.
/// </summary>
public sealed record MessageEnvelope
{
    /// <summary>
    /// The unique message identifier.
    /// </summary>
    public required Guid MessageId { get; init; }

    /// <summary>
    /// When the message was sent.
    /// </summary>
    public required DateTimeOffset SentAt { get; init; }

    /// <summary>
    /// The message type URNs (supports polymorphism).
    /// </summary>
    public required IReadOnlyList<string> MessageTypes { get; init; }

    /// <summary>
    /// Optional correlation ID for tracking related messages across services.
    /// </summary>
    public Guid? CorrelationId { get; init; }

    /// <summary>
    /// User-defined headers attached to the message.
    /// </summary>
    public IReadOnlyDictionary<string, string> Headers { get; init; } =
        new Dictionary<string, string>();
}

/// <summary>
/// Strongly-typed envelope wrapping a message payload with metadata.
/// </summary>
/// <typeparam name="TMessage">The message type.</typeparam>
public sealed record MessageEnvelope<TMessage>
    where TMessage : class
{
    /// <summary>
    /// The envelope metadata.
    /// </summary>
    public required MessageEnvelope Envelope { get; init; }

    /// <summary>
    /// The deserialized message payload.
    /// </summary>
    public required TMessage Payload { get; init; }
}
