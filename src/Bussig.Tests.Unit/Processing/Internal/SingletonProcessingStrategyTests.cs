using Bussig.Abstractions;
using Bussig.Abstractions.Options;
using Bussig.Processing.Internal;
using Bussig.Processing.Internal.Strategies;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Bussig.Tests.Unit.Processing.Internal;

public class SingletonProcessingStrategyTests
{
    private const string QueueName = "test-queue";
    private const string ExpectedLockId = "singleton:test-queue";
    private static readonly TimeSpan LockDuration = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan RenewalInterval = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan AcquisitionRetryInterval = TimeSpan.FromMilliseconds(50);

    [Test]
    public async Task PollAsync_AcquiresLock_ThenDelegatesToInnerStrategy()
    {
        // Arrange
        var lockManager = new Mock<IDistributedLockManager>();
        lockManager
            .Setup(l =>
                l.TryLockAsync(
                    ExpectedLockId,
                    It.IsAny<Guid>(),
                    LockDuration,
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(true);
        lockManager
            .Setup(l =>
                l.TryRenewAsync(
                    ExpectedLockId,
                    It.IsAny<Guid>(),
                    LockDuration,
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(true);

        var innerCalled = false;
        var innerStrategy = new Mock<IMessageProcessingStrategy>();
        innerStrategy
            .Setup(s => s.PollAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct =>
            {
                innerCalled = true;
                // Simulate the inner strategy running briefly then stopping
                await Task.Delay(100, ct);
            });

        using var cts = new CancellationTokenSource();
        var strategy = CreateStrategy(innerStrategy.Object, lockManager.Object);

        // Act - cancel after a short delay
        cts.CancelAfter(TimeSpan.FromMilliseconds(300));
        await strategy.PollAsync(cts.Token);

        // Assert
        await Assert.That(innerCalled).IsTrue();
        lockManager.Verify(
            l =>
                l.TryLockAsync(
                    ExpectedLockId,
                    It.IsAny<Guid>(),
                    LockDuration,
                    It.IsAny<CancellationToken>()
                ),
            Times.AtLeastOnce
        );
    }

    [Test]
    public async Task PollAsync_RetriesAcquisition_WhenLockUnavailable()
    {
        // Arrange
        var acquireCount = 0;
        var lockManager = new Mock<IDistributedLockManager>();
        lockManager
            .Setup(l =>
                l.TryLockAsync(
                    ExpectedLockId,
                    It.IsAny<Guid>(),
                    LockDuration,
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(() =>
            {
                acquireCount++;
                return acquireCount >= 3; // Succeed on 3rd attempt
            });
        lockManager
            .Setup(l =>
                l.TryRenewAsync(
                    ExpectedLockId,
                    It.IsAny<Guid>(),
                    LockDuration,
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(true);

        var innerStrategy = new Mock<IMessageProcessingStrategy>();
        innerStrategy
            .Setup(s => s.PollAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(ct => Task.Delay(100, ct));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var strategy = CreateStrategy(innerStrategy.Object, lockManager.Object);

        // Act
        await strategy.PollAsync(cts.Token);

        // Assert
        await Assert.That(acquireCount).IsGreaterThanOrEqualTo(3);
        innerStrategy.Verify(s => s.PollAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Test]
    public async Task PollAsync_CancelsInnerStrategy_WhenRenewalFails()
    {
        // Arrange
        var lockManager = new Mock<IDistributedLockManager>();
        var acquireCount = 0;
        lockManager
            .Setup(l =>
                l.TryLockAsync(
                    ExpectedLockId,
                    It.IsAny<Guid>(),
                    LockDuration,
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(() =>
            {
                acquireCount++;
                return true;
            });

        // First renewal succeeds, second fails (lock lost)
        var renewCount = 0;
        lockManager
            .Setup(l =>
                l.TryRenewAsync(
                    ExpectedLockId,
                    It.IsAny<Guid>(),
                    LockDuration,
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(() =>
            {
                renewCount++;
                return renewCount <= 1; // Fail on 2nd renewal
            });

        CancellationToken capturedToken = default;
        var innerStrategy = new Mock<IMessageProcessingStrategy>();
        innerStrategy
            .Setup(s => s.PollAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct =>
            {
                capturedToken = ct;
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                catch (OperationCanceledException)
                {
                    // Expected when lock is lost
                }
            });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var strategy = CreateStrategy(innerStrategy.Object, lockManager.Object);

        // Act
        await strategy.PollAsync(cts.Token);

        // Assert - inner strategy should have been cancelled, and lock re-acquired
        await Assert.That(capturedToken.IsCancellationRequested).IsTrue();
        await Assert.That(acquireCount).IsGreaterThanOrEqualTo(2);
    }

    [Test]
    public async Task PollAsync_ReleasesLock_OnGracefulShutdown()
    {
        // Arrange
        var lockManager = new Mock<IDistributedLockManager>();
        lockManager
            .Setup(l =>
                l.TryLockAsync(
                    ExpectedLockId,
                    It.IsAny<Guid>(),
                    LockDuration,
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(true);
        lockManager
            .Setup(l =>
                l.TryRenewAsync(
                    ExpectedLockId,
                    It.IsAny<Guid>(),
                    LockDuration,
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(true);
        lockManager
            .Setup(l =>
                l.TryReleaseAsync(ExpectedLockId, It.IsAny<Guid>(), It.IsAny<CancellationToken>())
            )
            .ReturnsAsync(true);

        var innerStrategy = new Mock<IMessageProcessingStrategy>();
        innerStrategy
            .Setup(s => s.PollAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(ct => Task.Delay(50, ct));

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        var strategy = CreateStrategy(innerStrategy.Object, lockManager.Object);

        // Act
        await strategy.PollAsync(cts.Token);

        // Assert
        lockManager.Verify(
            l => l.TryReleaseAsync(ExpectedLockId, It.IsAny<Guid>(), It.IsAny<CancellationToken>()),
            Times.AtLeastOnce
        );
    }

    [Test]
    public async Task PollAsync_HandlesAcquisitionException_Gracefully()
    {
        // Arrange
        var attemptCount = 0;
        var lockManager = new Mock<IDistributedLockManager>();
        lockManager
            .Setup(l =>
                l.TryLockAsync(
                    ExpectedLockId,
                    It.IsAny<Guid>(),
                    LockDuration,
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(() =>
            {
                attemptCount++;
                if (attemptCount == 1)
                    throw new InvalidOperationException("DB connection failed");
                return true;
            });
        lockManager
            .Setup(l =>
                l.TryRenewAsync(
                    ExpectedLockId,
                    It.IsAny<Guid>(),
                    LockDuration,
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(true);

        var innerStrategy = new Mock<IMessageProcessingStrategy>();
        innerStrategy
            .Setup(s => s.PollAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(ct => Task.Delay(50, ct));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var strategy = CreateStrategy(innerStrategy.Object, lockManager.Object);

        // Act - should not throw, should retry and eventually succeed
        await strategy.PollAsync(cts.Token);

        // Assert
        await Assert.That(attemptCount).IsGreaterThanOrEqualTo(2);
        innerStrategy.Verify(s => s.PollAsync(It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    [Test]
    public async Task PollAsync_HandlesRenewalException_AsLockLost()
    {
        // Arrange
        var lockManager = new Mock<IDistributedLockManager>();
        var acquireCount = 0;
        lockManager
            .Setup(l =>
                l.TryLockAsync(
                    ExpectedLockId,
                    It.IsAny<Guid>(),
                    LockDuration,
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(() =>
            {
                acquireCount++;
                return true;
            });
        lockManager
            .Setup(l =>
                l.TryRenewAsync(
                    ExpectedLockId,
                    It.IsAny<Guid>(),
                    LockDuration,
                    It.IsAny<CancellationToken>()
                )
            )
            .ThrowsAsync(new InvalidOperationException("DB connection failed"));

        var innerStrategy = new Mock<IMessageProcessingStrategy>();
        innerStrategy
            .Setup(s => s.PollAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct =>
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                catch (OperationCanceledException)
                {
                    // Expected
                }
            });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var strategy = CreateStrategy(innerStrategy.Object, lockManager.Object);

        // Act
        await strategy.PollAsync(cts.Token);

        // Assert - should re-acquire after exception-based lock loss
        await Assert.That(acquireCount).IsGreaterThanOrEqualTo(2);
    }

    private static SingletonProcessingStrategy CreateStrategy(
        IMessageProcessingStrategy inner,
        IDistributedLockManager lockManager
    )
    {
        return new SingletonProcessingStrategy(
            inner,
            lockManager,
            QueueName,
            NullLogger<SingletonProcessingStrategy>.Instance,
            new SingletonProcessingOptions
            {
                EnableSingletonProcessing = true,
                LockDuration = LockDuration,
                AcquisitionRetryInterval = AcquisitionRetryInterval,
                RenewalInterval = RenewalInterval,
            }
        );
    }
}
