namespace Bussig.Abstractions.Options;

/// <summary>
/// Configuration options for message singleton lock behavior.
/// </summary>
public sealed class SingletonProcessingOptions
{
    /// <summary>
    /// When true, singleton processing is enabled, i.e. only one processor will globally process messages
    /// </summary>
    public bool EnableSingletonProcessing { get; set; }

    /// <summary>
    /// Duration to hold the singleton lock. Defaults to 1 minute.
    /// <remarks><see cref="EnableSingletonProcessing"/> should be <c>true</c> for this to take any effect</remarks>
    /// </summary>
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// The interval to renew the lock when acquired. Should be less than <see cref="LockDuration"/>
    /// Defaults to 45 seconds
    /// <remarks><see cref="EnableSingletonProcessing"/> should be <c>true</c> for this to take any effect</remarks>
    /// </summary>
    public TimeSpan RenewalInterval { get; set; } = TimeSpan.FromSeconds(45);

    /// <summary>
    /// Interval to retry acquisition of the lock. Defaults to 10 seconds
    /// <remarks><see cref="EnableSingletonProcessing"/> should be <c>true</c> for this to take any effect</remarks>
    /// </summary>
    public TimeSpan AcquisitionRetryInterval { get; set; } = TimeSpan.FromSeconds(10);
}
