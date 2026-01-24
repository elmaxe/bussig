namespace Bussig.Abstractions.Options;

/// <summary>
/// Configuration options for message retry behavior.
/// </summary>
public sealed class RetryOptions
{
    /// <summary>
    /// Base delay before retrying a failed message.
    /// For Fixed strategy, this is the exact delay.
    /// For Exponential strategy, this is the initial delay that grows with each retry.
    /// Default: 30 seconds
    /// </summary>
    public TimeSpan Delay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Strategy for calculating retry delays.
    /// Default: Fixed
    /// </summary>
    public RetryStrategy Strategy { get; set; } = RetryStrategy.Fixed;

    /// <summary>
    /// Maximum retry delay when using exponential backoff.
    /// Default: 5 minutes
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Custom retry delay calculator. Used when Strategy is Custom.
    /// Receives full retry context including message metadata and exception.
    /// </summary>
    public Func<RetryContext, TimeSpan>? CustomDelayCalculator { get; set; }
}
