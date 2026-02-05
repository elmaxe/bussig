using Bussig.Abstractions.Options;

namespace Bussig.Abstractions.Middleware;

/// <summary>
/// Context passed through the unified message processing middleware pipeline.
/// Contains all information needed for processing messages (single or batch).
/// Single-message processing is treated as a batch of one message.
/// </summary>
public sealed class MessageContext
{
    /// <summary>
    /// The raw incoming messages from the queue.
    /// Always a collection - single-message processing uses a batch of 1.
    /// </summary>
    public required IReadOnlyList<IncomingMessage> Messages { get; init; }

    /// <summary>
    /// Convenience accessor for single-message processing.
    /// Returns the first (and typically only) message.
    /// </summary>
    public IncomingMessage Message => Messages[0];

    /// <summary>
    /// The name of the queue the messages were received from.
    /// </summary>
    public required string QueueName { get; init; }

    /// <summary>
    /// The processor type that will handle these messages.
    /// </summary>
    public required Type ProcessorType { get; init; }

    /// <summary>
    /// The expected message type for deserialization.
    /// For batch processors, this is the inner message type (TMessage in Batch&lt;TMessage&gt;).
    /// </summary>
    public required Type MessageType { get; init; }

    /// <summary>
    /// The response message type for request-reply processors.
    /// Null for fire-and-forget processors.
    /// </summary>
    public Type? ResponseMessageType { get; init; }

    /// <summary>
    /// The processor options containing configuration for this processor.
    /// </summary>
    public required ProcessorOptions Options { get; init; }

    /// <summary>
    /// The service provider for resolving dependencies.
    /// </summary>
    public required IServiceProvider ServiceProvider { get; init; }

    /// <summary>
    /// Cancellation token for the processing operation.
    /// </summary>
    public required CancellationToken CancellationToken { get; init; }

    /// <summary>
    /// Whether this context is for a batch processor (IProcessor&lt;Batch&lt;T&gt;&gt;).
    /// When true, the processor receives all messages as a Batch object.
    /// When false, the processor is called once per message.
    /// </summary>
    public required bool IsBatchProcessor { get; init; }

    /// <summary>
    /// Delegate to complete all messages atomically on success.
    /// Set by the processing strategy.
    /// </summary>
    public required Func<Task> CompleteAllAsync { get; init; }

    /// <summary>
    /// Delegate to abandon all messages atomically on failure.
    /// Set by the processing strategy.
    /// </summary>
    public required Func<
        TimeSpan,
        Exception?,
        string?,
        string?,
        Task
    > AbandonAllAsync { get; init; }

    /// <summary>
    /// The message envelopes containing metadata and payloads.
    /// Set by EnvelopeMiddleware after deserialization.
    /// </summary>
    public IReadOnlyList<MessageEnvelope<object>>? Envelopes { get; set; }

    /// <summary>
    /// Convenience accessor for single-message envelope.
    /// Returns the first envelope when set.
    /// </summary>
    public MessageEnvelope<object>? Envelope => Envelopes?[0];

    /// <summary>
    /// The delivery information for each message.
    /// Set by EnvelopeMiddleware after deserialization.
    /// </summary>
    public IReadOnlyList<DeliveryInfo>? DeliveryInfos { get; set; }

    /// <summary>
    /// Convenience accessor for single-message delivery info.
    /// Returns the first delivery info when set.
    /// </summary>
    public DeliveryInfo? DeliveryInfo => DeliveryInfos?[0];

    /// <summary>
    /// The deserialized message bodies.
    /// Set by DeserializationMiddleware.
    /// </summary>
    public IReadOnlyList<object>? DeserializedMessages { get; set; }

    /// <summary>
    /// Convenience accessor for single-message deserialized body.
    /// Returns the first deserialized message when set.
    /// </summary>
    public object? DeserializedMessage => DeserializedMessages?[0];

    /// <summary>
    /// The typed processor contexts for each message.
    /// Set by ProcessorInvocationMiddleware.
    /// </summary>
    public IReadOnlyList<object>? ProcessorContexts { get; set; }

    /// <summary>
    /// Convenience accessor for single-message processor context.
    /// Returns the first processor context when set.
    /// </summary>
    public object? ProcessorContext => ProcessorContexts?[0];

    /// <summary>
    /// The response message from a request-reply processor.
    /// Set by ProcessorInvocationMiddleware for IProcessor&lt;TMessage, TSend&gt;.
    /// </summary>
    public object? ResponseMessage { get; set; }

    /// <summary>
    /// Exception that occurred during processing.
    /// Set by middleware that catches exceptions.
    /// </summary>
    public Exception? Exception { get; set; }

    /// <summary>
    /// Indicates whether the messages have been fully handled.
    /// Set to true to short-circuit the pipeline.
    /// </summary>
    public bool IsHandled { get; set; }

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
