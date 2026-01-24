namespace Bussig.Abstractions;

/// <summary>
/// Context provided to custom retry delay calculators.
/// </summary>
public sealed record RetryContext
{
    /// <summary>Current delivery attempt number (1-based).</summary>
    public required int DeliveryCount { get; init; }

    /// <summary>Maximum delivery attempts before dead-lettering.</summary>
    public required int MaxDeliveryCount { get; init; }

    /// <summary>When the message was first enqueued.</summary>
    public required DateTimeOffset EnqueuedAt { get; init; }

    /// <summary>When the message was last delivered (null on first attempt).</summary>
    public required DateTimeOffset? LastDeliveredAt { get; init; }

    /// <summary>When the message expires (null if no expiration).</summary>
    public required DateTimeOffset? ExpirationTime { get; init; }

    /// <summary>The exception that caused the retry (null for batch failures).</summary>
    public Exception? Exception { get; init; }

    /// <summary>The configured base retry delay.</summary>
    public required TimeSpan BaseDelay { get; init; }
}
