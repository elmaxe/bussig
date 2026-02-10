using Bussig.Abstractions;
using Microsoft.EntityFrameworkCore;
using Moq;

namespace Bussig.EntityFrameworkCore.Tests;

public class OutboxSenderTests
{
    [Test]
    public async Task SendAsync_WhenInactive_DelegatesToInnerSender()
    {
        // Arrange
        var innerSender = new Mock<IOutgoingMessageSender>();
        var context = new OutboxTransactionContext();
        var sender = new OutboxSender(innerSender.Object, context);
        var message = CreateOutgoingMessage();

        // Act
        await sender.SendAsync(message, CancellationToken.None);

        // Assert
        innerSender.Verify(s => s.SendAsync(message, CancellationToken.None), Times.Once);
    }

    [Test]
    [ClassDataSource<PostgresContainerFixture>(Shared = SharedType.PerClass)]
    public async Task SendAsync_WhenActive_AddsToChangeTracker(PostgresContainerFixture fixture)
    {
        // Arrange
        var innerSender = new Mock<IOutgoingMessageSender>();
        var context = new OutboxTransactionContext();
        var sender = new OutboxSender(innerSender.Object, context);

        await using var dbContext = TestDbContextFactory.Create(fixture.ConnectionString);
        using var scope = context.Use(dbContext);

        var message = CreateOutgoingMessage();
        // Act
        await sender.SendAsync(message, CancellationToken.None);

        // Assert
        var tracked = dbContext.ChangeTracker.Entries<OutboxMessage>().ToList();
        await Assert.That(tracked.Count).IsEqualTo(1);

        var entity = tracked[0].Entity;
        await Assert.That(entity.MessageId).IsEqualTo(message.MessageId);
        await Assert.That(entity.QueueName).IsEqualTo(message.QueueName);
        await Assert.That(entity.Body).IsEqualTo(message.Body);
        await Assert.That(entity.HeadersJson).IsEqualTo(message.HeadersJson);
        await Assert.That(entity.Priority).IsEqualTo(message.Priority);
        await Assert.That(entity.Delay).IsEqualTo(message.Delay);
        await Assert.That(entity.MessageVersion).IsEqualTo(message.MessageVersion);
        await Assert.That(entity.ExpirationTime).IsEqualTo(message.ExpirationTime);
        await Assert.That(entity.SchedulingTokenId).IsEqualTo(message.SchedulingTokenId);

        innerSender.Verify(
            s => s.SendAsync(It.IsAny<OutgoingMessage>(), It.IsAny<CancellationToken>()),
            Times.Never
        );
    }

    [Test]
    public async Task CancelAsync_WhenInactive_DelegatesToInnerSender()
    {
        // Arramge
        var token = Guid.NewGuid();
        var innerSender = new Mock<IOutgoingMessageSender>();
        innerSender
            .Setup(s => s.CancelAsync(token, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        var context = new OutboxTransactionContext();
        var sender = new OutboxSender(innerSender.Object, context);

        // Act
        var result = await sender.CancelAsync(token, CancellationToken.None);

        // Assert
        await Assert.That(result).IsTrue();
        innerSender.Verify(s => s.CancelAsync(token, CancellationToken.None), Times.Once);
    }

    [Test]
    [ClassDataSource<PostgresContainerFixture>(Shared = SharedType.PerClass)]
    public async Task CancelAsync_WhenActive_MessageExists_DeletesAndReturnsTrue(
        PostgresContainerFixture fixture
    )
    {
        // Arrange
        var innerSender = new Mock<IOutgoingMessageSender>();
        var context = new OutboxTransactionContext();
        var sender = new OutboxSender(innerSender.Object, context);
        var token = Guid.NewGuid();

        await using var dbContext = TestDbContextFactory.Create(fixture.ConnectionString);
        using var scope = context.Use(dbContext);

        dbContext
            .Set<OutboxMessage>()
            .Add(
                new OutboxMessage
                {
                    MessageId = Guid.NewGuid(),
                    QueueName = "test-queue",
                    Body = [1, 2, 3],
                    SchedulingTokenId = token,
                    CreatedAt = DateTimeOffset.UtcNow,
                }
            );
        await dbContext.SaveChangesAsync();

        // Act
        var result = await sender.CancelAsync(token, CancellationToken.None);

        // Assert
        await Assert.That(result).IsTrue();
        innerSender.Verify(
            s => s.CancelAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()),
            Times.Never
        );
    }

    [Test]
    [ClassDataSource<PostgresContainerFixture>(Shared = SharedType.PerClass)]
    public async Task CancelAsync_WhenActive_NoMatch_FallsThrough(PostgresContainerFixture fixture)
    {
        // Arrange
        var innerSender = new Mock<IOutgoingMessageSender>();
        innerSender
            .Setup(s => s.CancelAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        var context = new OutboxTransactionContext();
        var sender = new OutboxSender(innerSender.Object, context);

        await using var dbContext = TestDbContextFactory.Create(fixture.ConnectionString);
        using var scope = context.Use(dbContext);

        // Act
        var result = await sender.CancelAsync(Guid.NewGuid(), CancellationToken.None);

        // Assert
        await Assert.That(result).IsFalse();
        innerSender.Verify(
            s => s.CancelAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()),
            Times.Once
        );
    }

    [Test]
    [ClassDataSource<PostgresContainerFixture>(Shared = SharedType.PerClass)]
    public async Task CancelAsync_WhenActive_AlreadyPublished_FallsThrough(
        PostgresContainerFixture fixture
    )
    {
        // Arrange
        var innerSender = new Mock<IOutgoingMessageSender>();
        innerSender
            .Setup(s => s.CancelAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        var context = new OutboxTransactionContext();
        var sender = new OutboxSender(innerSender.Object, context);
        var token = Guid.NewGuid();

        await using var dbContext = TestDbContextFactory.Create(fixture.ConnectionString);
        using var scope = context.Use(dbContext);

        dbContext
            .Set<OutboxMessage>()
            .Add(
                new OutboxMessage
                {
                    MessageId = Guid.NewGuid(),
                    QueueName = "test-queue",
                    Body = [1, 2, 3],
                    SchedulingTokenId = token,
                    PublishedAt = DateTimeOffset.UtcNow,
                    CreatedAt = DateTimeOffset.UtcNow,
                }
            );
        await dbContext.SaveChangesAsync();

        // Act
        var result = await sender.CancelAsync(token, CancellationToken.None);

        // Assert
        await Assert.That(result).IsFalse();
        innerSender.Verify(
            s => s.CancelAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()),
            Times.Once
        );
    }

    private static OutgoingMessage CreateOutgoingMessage() =>
        new(Guid.NewGuid(), "test-queue", [1, 2, 3], "{\"key\":\"value\"}")
        {
            Priority = 5,
            Delay = TimeSpan.FromMinutes(1),
            MessageVersion = 2,
            ExpirationTime = DateTimeOffset.UtcNow.AddHours(1),
            SchedulingTokenId = Guid.NewGuid(),
        };
}
