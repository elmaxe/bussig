namespace Bussig.Abstractions.Options;

/// <summary>
/// Configuration options for message lock management.
/// </summary>
public sealed class LockOptions
{
    /// <summary>
    /// Duration to lock messages for processing.
    /// Should be longer than expected processing time.
    /// Default: 5 minutes
    /// </summary>
    public TimeSpan Duration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Interval between lock renewal attempts.
    /// Should be less than Duration.
    /// Default: 2 minutes
    /// </summary>
    public TimeSpan RenewalInterval { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Whether to automatically renew message locks during processing.
    /// Default: true
    /// </summary>
    public bool EnableRenewal { get; set; } = true;

    /// <summary>
    /// Maximum number of times a lock can be renewed.
    /// Default: null (unlimited)
    /// </summary>
    public int? MaxRenewalCount { get; set; }
}
