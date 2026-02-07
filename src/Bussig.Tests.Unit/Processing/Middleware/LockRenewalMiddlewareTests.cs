using Bussig.Abstractions;
using Bussig.Abstractions.Middleware;
using Bussig.Abstractions.Options;
using Bussig.Processing.Middleware;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Bussig.Tests.Unit.Processing.Middleware;

public class LockRenewalMiddlewareTests
{
    [Test]
    public async Task InvokeAsync_SkipsLockRenewal_WhenIsHandledIsTrue()
    {
        // Arrange
        var renewer = new Mock<IMessageLockRenewer>();
        var middleware = new LockRenewalMiddleware(
            renewer.Object,
            NullLogger<LockRenewalMiddleware>.Instance
        );

        var context = CreateContext([CreateIncomingMessage()]);
        context.IsHandled = true;

        var nextCalled = false;

        // Act
        await middleware.InvokeAsync(
            context,
            _ =>
            {
                nextCalled = true;
                return Task.CompletedTask;
            }
        );

        // Assert
        await Assert.That(nextCalled).IsTrue();
        renewer.Verify(
            r =>
                r.RenewLockAsync(
                    It.IsAny<long>(),
                    It.IsAny<Guid>(),
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()
                ),
            Times.Never
        );
    }

    [Test]
    public async Task InvokeAsync_CallsNextMiddleware()
    {
        // Arrange
        var renewer = new Mock<IMessageLockRenewer>();
        renewer
            .Setup(r =>
                r.RenewLockAsync(
                    It.IsAny<long>(),
                    It.IsAny<Guid>(),
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(true);

        var middleware = new LockRenewalMiddleware(
            renewer.Object,
            NullLogger<LockRenewalMiddleware>.Instance
        );

        var context = CreateContext([CreateIncomingMessage()]);
        var nextCalled = false;

        // Act
        await middleware.InvokeAsync(
            context,
            _ =>
            {
                nextCalled = true;
                return Task.CompletedTask;
            }
        );

        // Assert
        await Assert.That(nextCalled).IsTrue();
    }

    [Test]
    public async Task InvokeAsync_RenewsLock_ForSingleMessage()
    {
        // Arrange
        var lockId = Guid.NewGuid();
        var deliveryId = 42L;

        var renewalTcs = new TaskCompletionSource<bool>();
        var renewCalled = new TaskCompletionSource();

        var renewer = new Mock<IMessageLockRenewer>();
        renewer
            .Setup(r =>
                r.RenewLockAsync(
                    It.IsAny<long>(),
                    lockId,
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()
                )
            )
            .Returns<long, Guid, TimeSpan, CancellationToken>(
                (id, _, _, _) =>
                {
                    renewCalled.TrySetResult();
                    return renewalTcs.Task;
                }
            );

        var middleware = new LockRenewalMiddleware(
            renewer.Object,
            NullLogger<LockRenewalMiddleware>.Instance
        );

        var message = CreateIncomingMessage(deliveryId: deliveryId, lockId: lockId);
        var context = CreateContext([message], renewalInterval: TimeSpan.FromMilliseconds(10));

        // Act
        await middleware.InvokeAsync(
            context,
            async _ =>
            {
                // Wait for the renewal to be attempted
                await renewCalled.Task.WaitAsync(TimeSpan.FromSeconds(5));
                renewalTcs.SetResult(true);
            }
        );

        // Assert
        renewer.Verify(
            r =>
                r.RenewLockAsync(
                    It.Is<long>(id => id == deliveryId),
                    lockId,
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()
                ),
            Times.AtLeastOnce
        );
    }

    [Test]
    public async Task InvokeAsync_SkipsLockRenewal_ForBatchMessages()
    {
        // Arrange
        var lockId = Guid.NewGuid();

        var renewCalled = new TaskCompletionSource();
        var renewalTcs = new TaskCompletionSource<bool>();

        var nextCalled = false;
        var renewer = new Mock<IMessageLockRenewer>();
        renewer
            .Setup(r =>
                r.RenewLockAsync(
                    It.IsAny<long>(),
                    lockId,
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()
                )
            )
            .Returns<List<long>, Guid, TimeSpan, CancellationToken>(
                (ids, _, _, _) =>
                {
                    renewCalled.TrySetResult();
                    return renewalTcs.Task;
                }
            );

        var middleware = new LockRenewalMiddleware(
            renewer.Object,
            NullLogger<LockRenewalMiddleware>.Instance
        );

        var messages = new[]
        {
            CreateIncomingMessage(deliveryId: 1, lockId: lockId),
            CreateIncomingMessage(deliveryId: 2, lockId: lockId),
            CreateIncomingMessage(deliveryId: 3, lockId: lockId),
        };
        var context = CreateContext(
            messages,
            isBatchProcessor: true,
            renewalInterval: TimeSpan.FromMilliseconds(10)
        );

        // Act
        await middleware.InvokeAsync(
            context,
            _ =>
            {
                nextCalled = true;
                return Task.CompletedTask;
            }
        );

        // Assert
        await Assert.That(nextCalled).IsTrue();
        renewer.Verify(
            r =>
                r.RenewLockAsync(
                    It.IsAny<long>(),
                    It.IsAny<Guid>(),
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()
                ),
            Times.Never
        );
    }

    [Test]
    public async Task InvokeAsync_CancelsLockRenewal_WhenNextThrows()
    {
        // Arrange
        CancellationToken capturedToken = default;
        var renewCalled = new TaskCompletionSource();

        var renewer = new Mock<IMessageLockRenewer>();
        renewer
            .Setup(r =>
                r.RenewLockAsync(
                    It.IsAny<long>(),
                    It.IsAny<Guid>(),
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()
                )
            )
            .Returns<long, Guid, TimeSpan, CancellationToken>(
                async (_, _, _, ct) =>
                {
                    capturedToken = ct;
                    renewCalled.TrySetResult();
                    await Task.Delay(Timeout.Infinite, ct);
                    return true;
                }
            );

        var middleware = new LockRenewalMiddleware(
            renewer.Object,
            NullLogger<LockRenewalMiddleware>.Instance
        );

        var context = CreateContext(
            [CreateIncomingMessage()],
            renewalInterval: TimeSpan.FromMilliseconds(10)
        );

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            middleware.InvokeAsync(
                context,
                async _ =>
                {
                    // Wait for the renewal to start so the token gets captured
                    await renewCalled.Task.WaitAsync(TimeSpan.FromSeconds(5));
                    throw new InvalidOperationException("Processing failed");
                }
            )
        );

        await Assert.That(exception!.Message).IsEqualTo("Processing failed");
        await Assert.That(capturedToken.IsCancellationRequested).IsTrue();
    }

    private static MessageContext CreateContext(
        IReadOnlyList<IncomingMessage> messages,
        bool isBatchProcessor = false,
        TimeSpan? renewalInterval = null
    )
    {
        var options = new ProcessorOptions();
        options.Lock.RenewalInterval = renewalInterval ?? TimeSpan.FromMilliseconds(10);

        return new MessageContext
        {
            Messages = messages,
            QueueName = "test-queue",
            ProcessorType = typeof(object),
            MessageType = typeof(object),
            Options = options,
            ServiceProvider = null!,
            CancellationToken = CancellationToken.None,
            IsBatchProcessor = isBatchProcessor,
            CompleteAllAsync = () => Task.CompletedTask,
            AbandonAllAsync = (_, _, _, _) => Task.CompletedTask,
        };
    }

    private static IncomingMessage CreateIncomingMessage(long deliveryId = 1, Guid? lockId = null)
    {
        return new IncomingMessage
        {
            MessageId = Guid.NewGuid(),
            MessageDeliveryId = deliveryId,
            LockId = lockId ?? Guid.NewGuid(),
            Body = [],
            Headers = null,
            MessageDeliveryHeaders = null,
            DeliveryCount = 1,
            MaxDeliveryCount = 5,
            MessageVersion = 1,
            EnqueuedAt = DateTimeOffset.UtcNow,
            LastDeliveredAt = null,
            VisibleAt = DateTimeOffset.UtcNow,
            ExpirationTime = null,
        };
    }
}
