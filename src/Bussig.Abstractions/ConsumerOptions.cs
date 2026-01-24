namespace Bussig.Abstractions;

/// <summary>
/// Configuration options for message consumers.
/// </summary>
public sealed class ConsumerOptions
{
    /// <summary>
    /// The interval between polls when no messages are available.
    /// Default: 500ms
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Number of messages to fetch per poll operation.
    /// Default: 10
    /// </summary>
    public int PrefetchCount { get; set; } = 10;

    /// <summary>
    /// Maximum number of messages to process concurrently.
    /// Default: 5
    /// </summary>
    public int MaxConcurrency { get; set; } = 5;

    /// <summary>
    /// Duration to lock messages for processing.
    /// Should be longer than expected processing time.
    /// Default: 5 minutes
    /// </summary>
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Interval between lock renewal attempts.
    /// Should be less than LockDuration.
    /// Default: 2 minutes
    /// </summary>
    public TimeSpan LockRenewalInterval { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Delay before retrying a failed message.
    /// Default: 30 seconds
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(30);
}
