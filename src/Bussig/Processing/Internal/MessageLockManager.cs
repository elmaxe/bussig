using Bussig.Abstractions;
using Bussig.Abstractions.Options;
using Microsoft.Extensions.Logging;

namespace Bussig.Processing.Internal;

/// <summary>
/// Manages message lock renewal during processing.
/// </summary>
internal sealed class MessageLockManager
{
    private readonly IMessageLockRenewer _renewer;
    private readonly LockOptions _options;
    private readonly ILogger _logger;

    public MessageLockManager(IMessageLockRenewer renewer, LockOptions options, ILogger logger)
    {
        _renewer = renewer;
        _options = options;
        _logger = logger;
    }

    public async Task RunLockRenewalAsync(
        long messageDeliveryId,
        Guid lockId,
        CancellationToken cancellationToken
    )
    {
        if (!_options.EnableRenewal)
        {
            return;
        }

        var renewalCount = 0;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(_options.RenewalInterval, cancellationToken);

                // Check max renewal count
                if (
                    _options.MaxRenewalCount.HasValue
                    && renewalCount >= _options.MaxRenewalCount.Value
                )
                {
                    _logger.LogWarning(
                        "Lock renewal limit ({MaxCount}) reached for message delivery {MessageDeliveryId}",
                        _options.MaxRenewalCount.Value,
                        messageDeliveryId
                    );
                    break;
                }

                var renewed = await _renewer.RenewLockAsync(
                    messageDeliveryId,
                    lockId,
                    _options.Duration,
                    cancellationToken
                );

                if (!renewed)
                {
                    _logger.LogWarning(
                        "Failed to renew lock for message delivery {MessageDeliveryId}",
                        messageDeliveryId
                    );
                    break;
                }

                renewalCount++;

                _logger.LogDebug(
                    "Renewed lock for message delivery {MessageDeliveryId} (renewal #{Count})",
                    messageDeliveryId,
                    renewalCount
                );
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Expected when processing completes
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Error renewing lock for message delivery {MessageDeliveryId}",
                messageDeliveryId
            );
        }
    }
}
