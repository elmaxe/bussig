namespace Bussig.Processing.Middleware;

/// <summary>
/// Constants for middleware item keys.
/// </summary>
internal static class MiddlewareConstants
{
    /// <summary>
    /// Key indicating deserialization failed.
    /// </summary>
    public const string DeserializationFailed = "bussig:deserialization-failed";

    /// <summary>
    /// Key for error message string.
    /// </summary>
    public const string ErrorMessage = "bussig:error-message";

    /// <summary>
    /// Key for error code string.
    /// </summary>
    public const string ErrorCode = "bussig:error-code";

    /// <summary>
    /// Key for the lock renewal cancellation token source.
    /// </summary>
    public const string LockRenewalCts = "bussig:lock-renewal-cts";

    /// <summary>
    /// Key for the lock renewal task.
    /// </summary>
    public const string LockRenewalTask = "bussig:lock-renewal-task";

    /// <summary>
    /// Key indicating message should be deadlettered.
    /// </summary>
    public const string ShouldDeadletter = "bussig:should-deadletter";
}
