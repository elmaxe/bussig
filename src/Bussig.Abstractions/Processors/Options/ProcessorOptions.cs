namespace Bussig.Abstractions.Options;

/// <summary>
/// Configuration options for message processors.
/// </summary>
public sealed class ProcessorOptions
{
    /// <summary>
    /// Options for message polling behavior.
    /// </summary>
    public PollingOptions Polling { get; } = new();

    /// <summary>
    /// Options for message lock management.
    /// </summary>
    public LockOptions Lock { get; } = new();

    /// <summary>
    /// Options for message retry behavior.
    /// </summary>
    public RetryOptions Retry { get; } = new();

    /// <summary>
    /// Options for batch message processing.
    /// Only used when processor is a batch processor.
    /// </summary>
    public BatchProcessingOptions Batch { get; } = new();
}
