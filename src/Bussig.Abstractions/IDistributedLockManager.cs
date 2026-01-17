namespace Bussig.Abstractions;

public interface IDistributedLockManager
{
    Task<bool> TryLockAsync(
        string lockId,
        Guid ownerToken,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    );

    Task<bool> TryRenewAsync(
        string lockId,
        Guid ownerToken,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    );

    Task<bool> TryReleaseAsync(
        string lockId,
        Guid ownerToken,
        CancellationToken cancellationToken = default
    );
}
