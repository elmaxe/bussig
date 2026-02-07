using Bussig.Abstractions;
using Bussig.Abstractions.Options;
using Microsoft.Extensions.Logging;

namespace Bussig.Processing.Internal.Strategies;

internal sealed class SingletonProcessingStrategy : IMessageProcessingStrategy
{
    private readonly IMessageProcessingStrategy _inner;
    private readonly IDistributedLockManager _lockManager;
    private readonly string _lockId;
    private readonly ILogger _logger;
    private readonly TimeSpan _lockDuration;
    private readonly TimeSpan _renewalInterval;
    private readonly TimeSpan _acquisitionRetryInterval;

    public SingletonProcessingStrategy(
        IMessageProcessingStrategy inner,
        IDistributedLockManager lockManager,
        string queueName,
        ILogger logger,
        SingletonProcessingOptions options
    )
    {
        _inner = inner;
        _lockManager = lockManager;
        _lockId = $"singleton:{queueName}";
        _logger = logger;
        _lockDuration = options.LockDuration;
        _renewalInterval = options.RenewalInterval;
        _acquisitionRetryInterval = options.AcquisitionRetryInterval;
    }

    public async Task PollAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var ownerToken = Guid.NewGuid();

            // Step 1: Acquire lock (retry loop)
            if (!await TryAcquireLockWithRetryAsync(ownerToken, stoppingToken))
                break; // Cancelled

            _logger.LogInformation("Acquired singleton lock {LockId} for processing", _lockId);

            // Step 2: Start background lock renewal and run inner strategy
            using var renewalCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            var renewalTask = RenewLockLoopAsync(ownerToken, renewalCts.Token);

            using var innerCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            var lockLost = false;

            try
            {
                // Run inner strategy and renewal concurrently
                var innerTask = _inner.PollAsync(innerCts.Token);

                var completedTask = await Task.WhenAny(innerTask, renewalTask);

                if (completedTask == renewalTask)
                {
                    // Lock renewal stopped — lock was lost or renewal failed
                    lockLost = true;
                    _logger.LogWarning(
                        "Singleton lock {LockId} was lost, stopping inner strategy",
                        _lockId
                    );

                    // Cancel inner strategy and wait for it to wind down
                    await innerCts.CancelAsync();
                    try
                    {
                        await innerTask;
                    }
                    catch (OperationCanceledException)
                    {
                        // Expected
                    }
                }
                else
                {
                    // Inner strategy completed (graceful shutdown via stoppingToken)
                    await renewalCts.CancelAsync();
                    try
                    {
                        await renewalTask;
                    }
                    catch (OperationCanceledException)
                    {
                        // Expected
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Graceful shutdown
                await renewalCts.CancelAsync();
            }

            // Step 3: Release lock (best-effort)
            await TryReleaseLockAsync(ownerToken);

            if (!lockLost)
                break; // Graceful shutdown, don't re-acquire

            // Lock was lost — loop back to re-acquire
        }
    }

    private async Task<bool> TryAcquireLockWithRetryAsync(
        Guid ownerToken,
        CancellationToken cancellationToken
    )
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var acquired = await _lockManager.TryLockAsync(
                    _lockId,
                    ownerToken,
                    _lockDuration,
                    cancellationToken
                );

                if (acquired)
                    return true;

                _logger.LogDebug(
                    "Singleton lock {LockId} is held by another instance, retrying in {Interval}",
                    _lockId,
                    _acquisitionRetryInterval
                );
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Error acquiring singleton lock {LockId}, retrying in {Interval}",
                    _lockId,
                    _acquisitionRetryInterval
                );
            }

            await Task.Delay(_acquisitionRetryInterval, cancellationToken);
        }

        return false;
    }

    /// <summary>
    /// Renews the lock periodically. Returns (completes) when the lock is lost or renewal fails.
    /// </summary>
    private async Task RenewLockLoopAsync(Guid ownerToken, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(_renewalInterval, cancellationToken);

            try
            {
                var renewed = await _lockManager.TryRenewAsync(
                    _lockId,
                    ownerToken,
                    _lockDuration,
                    cancellationToken
                );

                if (!renewed)
                {
                    _logger.LogWarning(
                        "Failed to renew singleton lock {LockId}, lock may have been lost",
                        _lockId
                    );
                    return;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Error renewing singleton lock {LockId}, treating as lock lost",
                    _lockId
                );
                return;
            }
        }
    }

    private async Task TryReleaseLockAsync(Guid ownerToken)
    {
        try
        {
            await _lockManager.TryReleaseAsync(_lockId, ownerToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to release singleton lock {LockId} (best-effort)",
                _lockId
            );
        }
    }
}
