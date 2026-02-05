namespace Bussig.Abstractions.Middleware;

/// <summary>
/// Context passed through the outgoing message middleware pipeline.
/// Contains all information needed for sending messages.
/// </summary>
public sealed class OutgoingMessageContext
{
    /// <summary>
    /// The message object to be sent.
    /// </summary>
    public required object Message { get; init; }

    /// <summary>
    /// The type of the message.
    /// </summary>
    public required Type MessageType { get; init; }

    /// <summary>
    /// The send options for this message.
    /// </summary>
    public required MessageSendOptions Options { get; init; }

    /// <summary>
    /// The target queue name.
    /// </summary>
    public required string QueueName { get; init; }

    /// <summary>
    /// The message types (URNs) for this message.
    /// </summary>
    public required IReadOnlyList<string> MessageTypes { get; init; }

    /// <summary>
    /// The service provider for resolving dependencies.
    /// </summary>
    public required IServiceProvider ServiceProvider { get; init; }

    /// <summary>
    /// Cancellation token for the send operation.
    /// </summary>
    public required CancellationToken CancellationToken { get; init; }

    /// <summary>
    /// The serialized message body.
    /// Set by OutgoingSerializationMiddleware.
    /// </summary>
    public byte[]? SerializedBody { get; set; }

    /// <summary>
    /// The message envelope containing metadata.
    /// Set by OutgoingSerializationMiddleware.
    /// </summary>
    public MessageEnvelope? Envelope { get; set; }

    /// <summary>
    /// The serialized envelope JSON for storage.
    /// Set by OutgoingSerializationMiddleware.
    /// </summary>
    public string? EnvelopeJson { get; set; }

    /// <summary>
    /// The constructed outgoing message.
    /// Set by OutgoingSenderMiddleware before sending.
    /// </summary>
    public OutgoingMessage? OutgoingMessage { get; set; }

    /// <summary>
    /// Indicates whether the message has been sent.
    /// Set to true to skip the terminal sender middleware.
    /// Useful for outbox pattern or other custom sending strategies.
    /// </summary>
    public bool IsSent { get; set; }

    /// <summary>
    /// Dictionary for middleware to share data through the pipeline.
    /// </summary>
    public IDictionary<string, object?> Items { get; } = new Dictionary<string, object?>();

    /// <summary>
    /// Gets a typed item from the Items dictionary.
    /// </summary>
    public T? GetItem<T>(string key)
        where T : class
    {
        return Items.TryGetValue(key, out var value) ? value as T : null;
    }

    /// <summary>
    /// Sets an item in the Items dictionary.
    /// </summary>
    public void SetItem<T>(string key, T? value)
        where T : class
    {
        Items[key] = value;
    }
}
