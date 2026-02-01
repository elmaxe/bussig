using Bussig.Abstractions;
using Bussig.Abstractions.Options;

namespace Bussig.Tests.Unit;

public class RetryContextTests
{
    [Test]
    public async Task RetryContext_CanBeCreatedWithAllRequiredProperties()
    {
        // Arrange
        var enqueuedAt = DateTimeOffset.UtcNow.AddMinutes(-5);
        var lastDeliveredAt = DateTimeOffset.UtcNow.AddSeconds(-30);
        var expirationTime = DateTimeOffset.UtcNow.AddHours(1);
        var baseDelay = TimeSpan.FromSeconds(30);

        // Act
        var context = new RetryContext
        {
            DeliveryCount = 3,
            MaxDeliveryCount = 5,
            EnqueuedAt = enqueuedAt,
            LastDeliveredAt = lastDeliveredAt,
            ExpirationTime = expirationTime,
            BaseDelay = baseDelay,
        };

        // Assert
        await Assert.That(context.DeliveryCount).IsEqualTo(3);
        await Assert.That(context.MaxDeliveryCount).IsEqualTo(5);
        await Assert.That(context.EnqueuedAt).IsEqualTo(enqueuedAt);
        await Assert.That(context.LastDeliveredAt).IsEqualTo(lastDeliveredAt);
        await Assert.That(context.ExpirationTime).IsEqualTo(expirationTime);
        await Assert.That(context.BaseDelay).IsEqualTo(baseDelay);
        await Assert.That(context.Exception).IsNull();
    }

    [Test]
    public async Task RetryContext_CanIncludeException()
    {
        // Arrange
        var exception = new InvalidOperationException("Test error");

        // Act
        var context = new RetryContext
        {
            DeliveryCount = 1,
            MaxDeliveryCount = 3,
            EnqueuedAt = DateTimeOffset.UtcNow,
            LastDeliveredAt = null,
            ExpirationTime = null,
            BaseDelay = TimeSpan.FromSeconds(10),
            Exception = exception,
        };

        // Assert
        await Assert.That(context.Exception).IsNotNull();
        await Assert.That(context.Exception).IsEqualTo(exception);
        await Assert.That(context.Exception!.Message).IsEqualTo("Test error");
    }

    [Test]
    public async Task RetryContext_NullablePropertiesCanBeNull()
    {
        // Arrange & Act
        var context = new RetryContext
        {
            DeliveryCount = 1,
            MaxDeliveryCount = 3,
            EnqueuedAt = DateTimeOffset.UtcNow,
            LastDeliveredAt = null,
            ExpirationTime = null,
            BaseDelay = TimeSpan.FromSeconds(10),
        };

        // Assert
        await Assert.That(context.LastDeliveredAt).IsNull();
        await Assert.That(context.ExpirationTime).IsNull();
        await Assert.That(context.Exception).IsNull();
    }

    [Test]
    public async Task RetryContext_IsRecord_SupportsEquality()
    {
        // Arrange
        var now = DateTimeOffset.UtcNow;
        var context1 = new RetryContext
        {
            DeliveryCount = 2,
            MaxDeliveryCount = 5,
            EnqueuedAt = now,
            LastDeliveredAt = null,
            ExpirationTime = null,
            BaseDelay = TimeSpan.FromSeconds(30),
        };
        var context2 = new RetryContext
        {
            DeliveryCount = 2,
            MaxDeliveryCount = 5,
            EnqueuedAt = now,
            LastDeliveredAt = null,
            ExpirationTime = null,
            BaseDelay = TimeSpan.FromSeconds(30),
        };

        // Assert
        await Assert.That(context1).IsEqualTo(context2);
    }

    [Test]
    public async Task RetryContext_WithDifferentValues_AreNotEqual()
    {
        // Arrange
        var now = DateTimeOffset.UtcNow;
        var context1 = new RetryContext
        {
            DeliveryCount = 1,
            MaxDeliveryCount = 5,
            EnqueuedAt = now,
            LastDeliveredAt = null,
            ExpirationTime = null,
            BaseDelay = TimeSpan.FromSeconds(30),
        };
        var context2 = new RetryContext
        {
            DeliveryCount = 2,
            MaxDeliveryCount = 5,
            EnqueuedAt = now,
            LastDeliveredAt = null,
            ExpirationTime = null,
            BaseDelay = TimeSpan.FromSeconds(30),
        };

        // Assert
        await Assert.That(context1).IsNotEqualTo(context2);
    }
}

public class CustomRetryDelayCalculatorTests
{
    [Test]
    public async Task ProcessorOptions_Retry_CustomDelayCalculator_DefaultIsNull()
    {
        // Arrange & Act
        var options = new ProcessorOptions();

        // Assert
        await Assert.That(options.Retry.CustomDelayCalculator).IsNull();
    }

    [Test]
    public async Task ProcessorOptions_Retry_CustomDelayCalculator_CanBeConfigured()
    {
        // Arrange
        var calculator = (RetryContext ctx) => ctx.BaseDelay * ctx.DeliveryCount;

        // Act
        var options = new ProcessorOptions();
        options.Retry.CustomDelayCalculator = calculator;

        // Assert
        await Assert.That(options.Retry.CustomDelayCalculator).IsNotNull();
    }

    [Test]
    public async Task CustomDelayCalculator_ReceivesCorrectContext()
    {
        // Arrange
        RetryContext? capturedContext = null;
        var calculator = (RetryContext ctx) =>
        {
            capturedContext = ctx;
            return TimeSpan.FromSeconds(60);
        };

        var options = new ProcessorOptions();
        options.Retry.CustomDelayCalculator = calculator;
        options.Retry.Delay = TimeSpan.FromSeconds(30);

        var testContext = new RetryContext
        {
            DeliveryCount = 3,
            MaxDeliveryCount = 5,
            EnqueuedAt = DateTimeOffset.UtcNow.AddMinutes(-5),
            LastDeliveredAt = DateTimeOffset.UtcNow.AddSeconds(-10),
            ExpirationTime = DateTimeOffset.UtcNow.AddHours(1),
            BaseDelay = options.Retry.Delay,
            Exception = new TimeoutException("Connection timed out"),
        };

        // Act
        var result = options.Retry.CustomDelayCalculator!(testContext);

        // Assert
        await Assert.That(capturedContext).IsNotNull();
        await Assert.That(capturedContext!.DeliveryCount).IsEqualTo(3);
        await Assert.That(capturedContext.MaxDeliveryCount).IsEqualTo(5);
        await Assert.That(capturedContext.Exception).IsTypeOf<TimeoutException>();
        await Assert.That(result).IsEqualTo(TimeSpan.FromSeconds(60));
    }

    [Test]
    public async Task CustomDelayCalculator_CanImplementLinearBackoff()
    {
        // Arrange
        var calculator = (RetryContext ctx) => ctx.BaseDelay * ctx.DeliveryCount;

        var options = new ProcessorOptions();
        options.Retry.CustomDelayCalculator = calculator;
        options.Retry.Delay = TimeSpan.FromSeconds(10);

        // Act & Assert
        var delay1 = options.Retry.CustomDelayCalculator!(
            CreateContext(deliveryCount: 1, baseDelay: options.Retry.Delay)
        );
        var delay2 = options.Retry.CustomDelayCalculator!(
            CreateContext(deliveryCount: 2, baseDelay: options.Retry.Delay)
        );
        var delay3 = options.Retry.CustomDelayCalculator!(
            CreateContext(deliveryCount: 3, baseDelay: options.Retry.Delay)
        );

        await Assert.That(delay1).IsEqualTo(TimeSpan.FromSeconds(10));
        await Assert.That(delay2).IsEqualTo(TimeSpan.FromSeconds(20));
        await Assert.That(delay3).IsEqualTo(TimeSpan.FromSeconds(30));
    }

    [Test]
    public async Task CustomDelayCalculator_CanImplementExponentialBackoff()
    {
        // Arrange
        var calculator = (RetryContext ctx) =>
            TimeSpan.FromTicks((long)(ctx.BaseDelay.Ticks * Math.Pow(2, ctx.DeliveryCount - 1)));

        var options = new ProcessorOptions();
        options.Retry.CustomDelayCalculator = calculator;
        options.Retry.Delay = TimeSpan.FromSeconds(5);

        // Act & Assert
        var delay1 = options.Retry.CustomDelayCalculator!(
            CreateContext(deliveryCount: 1, baseDelay: options.Retry.Delay)
        );
        var delay2 = options.Retry.CustomDelayCalculator!(
            CreateContext(deliveryCount: 2, baseDelay: options.Retry.Delay)
        );
        var delay3 = options.Retry.CustomDelayCalculator!(
            CreateContext(deliveryCount: 3, baseDelay: options.Retry.Delay)
        );

        await Assert.That(delay1).IsEqualTo(TimeSpan.FromSeconds(5)); // 5 * 2^0
        await Assert.That(delay2).IsEqualTo(TimeSpan.FromSeconds(10)); // 5 * 2^1
        await Assert.That(delay3).IsEqualTo(TimeSpan.FromSeconds(20)); // 5 * 2^2
    }

    [Test]
    public async Task CustomDelayCalculator_CanUseExceptionType()
    {
        // Arrange
        var calculator = (RetryContext ctx) =>
        {
            return ctx.Exception switch
            {
                TimeoutException => TimeSpan.FromSeconds(5),
                InvalidOperationException => TimeSpan.FromSeconds(30),
                _ => ctx.BaseDelay,
            };
        };

        var options = new ProcessorOptions();
        options.Retry.CustomDelayCalculator = calculator;
        options.Retry.Delay = TimeSpan.FromSeconds(15);

        // Act
        var delayForTimeout = options.Retry.CustomDelayCalculator!(
            CreateContext(exception: new TimeoutException())
        );
        var delayForInvalidOp = options.Retry.CustomDelayCalculator!(
            CreateContext(exception: new InvalidOperationException())
        );
        var delayForOther = options.Retry.CustomDelayCalculator!(
            CreateContext(exception: new ArgumentException())
        );

        // Assert
        await Assert.That(delayForTimeout).IsEqualTo(TimeSpan.FromSeconds(5));
        await Assert.That(delayForInvalidOp).IsEqualTo(TimeSpan.FromSeconds(30));
        await Assert.That(delayForOther).IsEqualTo(TimeSpan.FromSeconds(15));
    }

    [Test]
    public async Task CustomDelayCalculator_CanCheckRemainingAttempts()
    {
        // Arrange - use longer delay when close to max attempts
        var calculator = (RetryContext ctx) =>
        {
            var remainingAttempts = ctx.MaxDeliveryCount - ctx.DeliveryCount;
            return remainingAttempts <= 1
                ? TimeSpan.FromMinutes(5) // Last chance, wait longer
                : ctx.BaseDelay;
        };

        var options = new ProcessorOptions();
        options.Retry.CustomDelayCalculator = calculator;
        options.Retry.Delay = TimeSpan.FromSeconds(30);

        // Act
        var delayEarlyAttempt = options.Retry.CustomDelayCalculator!(
            CreateContext(deliveryCount: 1, maxDeliveryCount: 5, baseDelay: options.Retry.Delay)
        );
        var delayLastAttempt = options.Retry.CustomDelayCalculator!(
            CreateContext(deliveryCount: 4, maxDeliveryCount: 5, baseDelay: options.Retry.Delay)
        );

        // Assert
        await Assert.That(delayEarlyAttempt).IsEqualTo(TimeSpan.FromSeconds(30));
        await Assert.That(delayLastAttempt).IsEqualTo(TimeSpan.FromMinutes(5));
    }

    [Test]
    public async Task CustomDelayCalculator_CanUseMessageAge()
    {
        // Arrange - longer delay for older messages
        var calculator = (RetryContext ctx) =>
        {
            var messageAge = DateTimeOffset.UtcNow - ctx.EnqueuedAt;
            return messageAge > TimeSpan.FromMinutes(10)
                ? TimeSpan.FromMinutes(1) // Old message, back off
                : ctx.BaseDelay;
        };

        var options = new ProcessorOptions();
        options.Retry.CustomDelayCalculator = calculator;
        options.Retry.Delay = TimeSpan.FromSeconds(10);

        // Act
        var delayForRecentMessage = options.Retry.CustomDelayCalculator!(
            CreateContext(
                enqueuedAt: DateTimeOffset.UtcNow.AddMinutes(-1),
                baseDelay: options.Retry.Delay
            )
        );
        var delayForOldMessage = options.Retry.CustomDelayCalculator!(
            CreateContext(
                enqueuedAt: DateTimeOffset.UtcNow.AddMinutes(-15),
                baseDelay: options.Retry.Delay
            )
        );

        // Assert
        await Assert.That(delayForRecentMessage).IsEqualTo(TimeSpan.FromSeconds(10));
        await Assert.That(delayForOldMessage).IsEqualTo(TimeSpan.FromMinutes(1));
    }

    [Test]
    public async Task CustomDelayCalculator_CanRespectExpiration()
    {
        // Arrange - don't wait too long if message is about to expire
        var calculator = (RetryContext ctx) =>
        {
            if (ctx.ExpirationTime is null)
                return ctx.BaseDelay;

            var timeUntilExpiration = ctx.ExpirationTime.Value - DateTimeOffset.UtcNow;
            if (timeUntilExpiration < ctx.BaseDelay)
                return TimeSpan.FromSeconds(1); // Hurry up
            return ctx.BaseDelay;
        };

        var options = new ProcessorOptions();
        options.Retry.CustomDelayCalculator = calculator;
        options.Retry.Delay = TimeSpan.FromMinutes(1);

        // Act
        var delayForNonExpiring = options.Retry.CustomDelayCalculator!(
            CreateContext(expirationTime: null, baseDelay: options.Retry.Delay)
        );
        var delayForSoonExpiring = options.Retry.CustomDelayCalculator!(
            CreateContext(
                expirationTime: DateTimeOffset.UtcNow.AddSeconds(30),
                baseDelay: options.Retry.Delay
            )
        );
        var delayForLaterExpiring = options.Retry.CustomDelayCalculator!(
            CreateContext(
                expirationTime: DateTimeOffset.UtcNow.AddHours(1),
                baseDelay: options.Retry.Delay
            )
        );

        // Assert
        await Assert.That(delayForNonExpiring).IsEqualTo(TimeSpan.FromMinutes(1));
        await Assert.That(delayForSoonExpiring).IsEqualTo(TimeSpan.FromSeconds(1));
        await Assert.That(delayForLaterExpiring).IsEqualTo(TimeSpan.FromMinutes(1));
    }

    private static RetryContext CreateContext(
        int deliveryCount = 1,
        int maxDeliveryCount = 5,
        DateTimeOffset? enqueuedAt = null,
        DateTimeOffset? lastDeliveredAt = null,
        DateTimeOffset? expirationTime = null,
        Exception? exception = null,
        TimeSpan? baseDelay = null
    )
    {
        return new RetryContext
        {
            DeliveryCount = deliveryCount,
            MaxDeliveryCount = maxDeliveryCount,
            EnqueuedAt = enqueuedAt ?? DateTimeOffset.UtcNow,
            LastDeliveredAt = lastDeliveredAt,
            ExpirationTime = expirationTime,
            Exception = exception,
            BaseDelay = baseDelay ?? TimeSpan.FromSeconds(15),
        };
    }
}

public class RetryStrategyTests
{
    [Test]
    public async Task ProcessorOptions_DefaultRetryStrategy_IsFixed()
    {
        // Arrange & Act
        var options = new ProcessorOptions();

        // Assert
        await Assert.That(options.Retry.Strategy).IsEqualTo(RetryStrategy.Fixed);
    }

    [Test]
    public async Task ProcessorOptions_DefaultRetryDelay_Is30Seconds()
    {
        // Arrange & Act
        var options = new ProcessorOptions();

        // Assert
        await Assert.That(options.Retry.Delay).IsEqualTo(TimeSpan.FromSeconds(30));
    }

    [Test]
    public async Task ProcessorOptions_DefaultMaxRetryDelay_Is5Minutes()
    {
        // Arrange & Act
        var options = new ProcessorOptions();

        // Assert
        await Assert.That(options.Retry.MaxDelay).IsEqualTo(TimeSpan.FromMinutes(5));
    }

    [Test]
    [Arguments(RetryStrategy.Immediate)]
    [Arguments(RetryStrategy.Fixed)]
    [Arguments(RetryStrategy.Exponential)]
    [Arguments(RetryStrategy.Custom)]
    public async Task ProcessorOptions_RetryStrategy_CanBeConfigured(RetryStrategy strategy)
    {
        // Arrange & Act
        var options = new ProcessorOptions();
        options.Retry.Strategy = strategy;

        // Assert
        await Assert.That(options.Retry.Strategy).IsEqualTo(strategy);
    }

    [Test]
    public async Task ProcessorOptions_RetryDelay_CanBeConfigured()
    {
        // Arrange & Act
        var options = new ProcessorOptions();
        options.Retry.Delay = TimeSpan.FromSeconds(45);

        // Assert
        await Assert.That(options.Retry.Delay).IsEqualTo(TimeSpan.FromSeconds(45));
    }

    [Test]
    public async Task ProcessorOptions_MaxRetryDelay_CanBeConfigured()
    {
        // Arrange & Act
        var options = new ProcessorOptions();
        options.Retry.MaxDelay = TimeSpan.FromMinutes(10);

        // Assert
        await Assert.That(options.Retry.MaxDelay).IsEqualTo(TimeSpan.FromMinutes(10));
    }
}
