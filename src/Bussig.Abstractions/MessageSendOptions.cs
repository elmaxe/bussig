namespace Bussig.Abstractions;

/// <summary>
/// Options for sending or scheduling a message.
/// </summary>
public record MessageSendOptions
{
    /// <summary>
    /// Optional message ID. If not specified, a new ID will be generated.
    /// </summary>
    public Guid? MessageId { get; init; }

    /// <summary>
    /// Optional delay before the message becomes visible for processing.
    /// </summary>
    public TimeSpan? Delay { get; init; }

    /// <summary>
    /// Optional message priority. Higher values are processed first.
    /// </summary>
    public int? Priority { get; init; }

    /// <summary>
    /// Message version for versioning support.
    /// </summary>
    public int MessageVersion { get; init; }

    /// <summary>
    /// Optional scheduling token for cancelling scheduled messages.
    /// </summary>
    public Guid? SchedulingToken { get; init; }

    /// <summary>
    /// Optional correlation ID for tracking related messages across services.
    /// </summary>
    public Guid? CorrelationId { get; init; }

    /// <summary>
    /// Custom headers to include with the message envelope.
    /// </summary>
    public Dictionary<string, string> Headers { get; init; } = new();
}
