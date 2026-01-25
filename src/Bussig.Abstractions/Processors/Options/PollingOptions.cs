namespace Bussig.Abstractions.Options;

/// <summary>
/// Configuration options for message polling behavior.
/// </summary>
public sealed class PollingOptions
{
    /// <summary>
    /// The interval between polls when no messages are available.
    /// Default: 500ms
    /// </summary>
    public TimeSpan Interval { get; set; } = TimeSpan.FromMilliseconds(500);

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
}
