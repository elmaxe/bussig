using Bussig.Abstractions;
using Bussig.Abstractions.Options;

namespace Bussig.Processing.Internal;

/// <summary>
/// Calculates retry delays based on the configured retry strategy.
/// </summary>
internal sealed class RetryDelayCalculator
{
    private readonly RetryOptions _options;

    public RetryDelayCalculator(RetryOptions options)
    {
        _options = options;
    }

    public TimeSpan CalculateDelay(IncomingMessage message, Exception? exception = null)
    {
        return _options.Strategy switch
        {
            RetryStrategy.Immediate => TimeSpan.Zero,
            RetryStrategy.Fixed => _options.Delay,
            RetryStrategy.Exponential => CalculateExponentialDelay(message.DeliveryCount),
            RetryStrategy.Custom => _options.CustomDelayCalculator?.Invoke(
                new RetryContext
                {
                    DeliveryCount = message.DeliveryCount,
                    MaxDeliveryCount = message.MaxDeliveryCount,
                    EnqueuedAt = message.EnqueuedAt,
                    LastDeliveredAt = message.LastDeliveredAt,
                    ExpirationTime = message.ExpirationTime,
                    Exception = exception,
                    BaseDelay = _options.Delay,
                }
            ) ?? _options.Delay,
            _ => _options.Delay,
        };
    }

    private TimeSpan CalculateExponentialDelay(int deliveryCount)
    {
        // 2^(deliveryCount-1) * baseDelay, capped at MaxRetryDelay
        var multiplier = Math.Pow(2, deliveryCount - 1);
        var delay = TimeSpan.FromTicks((long)(_options.Delay.Ticks * multiplier));
        return delay > _options.MaxDelay ? _options.MaxDelay : delay;
    }
}
