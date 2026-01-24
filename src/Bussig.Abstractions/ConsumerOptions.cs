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
    /// Base delay before retrying a failed message.
    /// For Fixed strategy, this is the exact delay.
    /// For Exponential strategy, this is the initial delay that grows with each retry.
    /// Default: 30 seconds
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Strategy for calculating retry delays.
    /// Default: Fixed
    /// </summary>
    public RetryStrategy RetryStrategy { get; set; } = RetryStrategy.Fixed;

    /// <summary>
    /// Maximum retry delay when using exponential backoff.
    /// Default: 5 minutes
    /// </summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Custom retry delay calculator. Used when RetryStrategy is Custom.
    /// Receives full retry context including message metadata and exception.
    /// </summary>
    public Func<RetryContext, TimeSpan>? CustomRetryDelayCalculator { get; set; }

    /// <summary>
    /// Whether to automatically renew message locks during processing.
    /// Default: true
    /// </summary>
    public bool EnableLockRenewal { get; set; } = true;

    /// <summary>
    /// Maximum number of times a lock can be renewed.
    /// Default: null (unlimited)
    /// </summary>
    public int? MaxLockRenewalCount { get; set; }

    // Batch processing options (only used when processor is a batch processor)

    /// <summary>
    /// Time limit for collecting messages into a batch.
    /// If less than BatchMessageLimit messages arrive within this time, process the batch early.
    /// Default: 5 seconds
    /// </summary>
    public TimeSpan BatchTimeLimit { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Maximum number of messages in a batch.
    /// Default: 100
    /// </summary>
    public uint BatchMessageLimit { get; set; } = 100;
}
