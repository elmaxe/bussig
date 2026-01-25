namespace Bussig.Abstractions.Options;

/// <summary>
/// Configuration options for batch message processing.
/// </summary>
public sealed class BatchProcessingOptions
{
    /// <summary>
    /// Time limit for collecting messages into a batch.
    /// If less than MessageLimit messages arrive within this time, process the batch early.
    /// Default: 5 seconds
    /// </summary>
    public TimeSpan TimeLimit { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Maximum number of messages in a batch.
    /// Default: 100
    /// </summary>
    public uint MessageLimit { get; set; } = 100;
}
