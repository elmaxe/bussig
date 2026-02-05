using Bussig.Abstractions;
using Bussig.Abstractions.Messages;
using Bussig.Abstractions.Middleware;
using Bussig.Abstractions.Options;
using Bussig.Processing.Middleware;
using Moq;

namespace Bussig.Tests.Unit.Processing.Middleware;

public class AttachmentMiddlewareTests
{
    [Test]
    public async Task InvokeAsync_DownloadsAttachment_WhenMessageHasMessageDataWithAddress()
    {
        // Arrange
        var address = new Uri("https://test.blob.core.windows.net/container/blob");
        var attachmentContent = "attachment content"u8.ToArray();
        var attachmentStream = new MemoryStream(attachmentContent);

        var repository = new Mock<IMessageAttachmentRepository>();
        repository
            .Setup(r => r.GetAsync(address, It.IsAny<CancellationToken>()))
            .ReturnsAsync(attachmentStream);

        var middleware = new AttachmentMiddleware(repository.Object);

        var message = new TestMessageWithAttachment
        {
            Name = "Test",
            Attachment = new MessageData { Address = address },
        };

        var context = CreateContext(message);
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
        repository.Verify(r => r.GetAsync(address, It.IsAny<CancellationToken>()), Times.Once);
        repository.Verify(r => r.DeleteAsync(address, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Test]
    public async Task InvokeAsync_SkipsDownload_WhenMessageDataHasNoAddress()
    {
        // Arrange
        var repository = new Mock<IMessageAttachmentRepository>();

        var middleware = new AttachmentMiddleware(repository.Object);

        var message = new TestMessageWithAttachment
        {
            Name = "Test",
            Attachment = new MessageData(), // No address
        };

        var context = CreateContext(message);
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
        repository.Verify(
            r => r.GetAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()),
            Times.Never
        );
    }

    [Test]
    public async Task InvokeAsync_SkipsDownload_WhenMessageDataIsNull()
    {
        // Arrange
        var repository = new Mock<IMessageAttachmentRepository>();

        var middleware = new AttachmentMiddleware(repository.Object);

        var message = new TestMessageWithAttachment { Name = "Test", Attachment = null };

        var context = CreateContext(message);
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
        repository.Verify(
            r => r.GetAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()),
            Times.Never
        );
    }

    [Test]
    public async Task InvokeAsync_CallsNextMiddleware_WhenMessageHasNoMessageDataProperties()
    {
        // Arrange
        var repository = new Mock<IMessageAttachmentRepository>();

        var middleware = new AttachmentMiddleware(repository.Object);

        var message = new TestMessageWithoutAttachment { Name = "Test" };

        var context = CreateContext(message, typeof(TestMessageWithoutAttachment));
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
        repository.Verify(
            r => r.GetAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()),
            Times.Never
        );
    }

    [Test]
    public async Task InvokeAsync_DoesNotCallNext_WhenIsHandledIsTrue()
    {
        // Arrange
        var repository = new Mock<IMessageAttachmentRepository>();

        var middleware = new AttachmentMiddleware(repository.Object);

        var message = new TestMessageWithAttachment { Name = "Test" };
        var context = CreateContext(message);
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
        await Assert.That(nextCalled).IsFalse();
    }

    [Test]
    public async Task InvokeAsync_DoesNotCallNext_WhenDeserializedMessageIsNull()
    {
        // Arrange
        var repository = new Mock<IMessageAttachmentRepository>();

        var middleware = new AttachmentMiddleware(repository.Object);

        var context = CreateContext(null!);
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
        await Assert.That(nextCalled).IsFalse();
    }

    [Test]
    public async Task InvokeAsync_ThrowsNotSupportedException_ForBatchProcessor()
    {
        // Arrange
        var repository = new Mock<IMessageAttachmentRepository>();

        var middleware = new AttachmentMiddleware(repository.Object);

        var message = new TestMessageWithAttachment { Name = "Test" };
        var context = CreateContext(message, isBatch: true);

        // Act & Assert
        await Assert
            .That(async () => await middleware.InvokeAsync(context, _ => Task.CompletedTask))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task InvokeAsync_DeletesAttachment_EvenWhenNextMiddlewareThrows()
    {
        // Arrange
        var address = new Uri("https://test.blob.core.windows.net/container/blob");
        var attachmentStream = new MemoryStream("content"u8.ToArray());

        var repository = new Mock<IMessageAttachmentRepository>();
        repository
            .Setup(r => r.GetAsync(address, It.IsAny<CancellationToken>()))
            .ReturnsAsync(attachmentStream);

        var middleware = new AttachmentMiddleware(repository.Object);

        var message = new TestMessageWithAttachment
        {
            Name = "Test",
            Attachment = new MessageData { Address = address },
        };

        var context = CreateContext(message);

        // Act
        try
        {
            await middleware.InvokeAsync(
                context,
                _ => throw new InvalidOperationException("Test error")
            );
        }
        catch (InvalidOperationException)
        {
            // Expected
        }

        // Assert - Delete should still be called in finally block
        repository.Verify(r => r.DeleteAsync(address, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Test]
    public async Task InvokeAsync_HandlesMultipleMessageDataProperties()
    {
        // Arrange
        var address1 = new Uri("https://test.blob.core.windows.net/container/blob1");
        var address2 = new Uri("https://test.blob.core.windows.net/container/blob2");
        var stream1 = new MemoryStream("content1"u8.ToArray());
        var stream2 = new MemoryStream("content2"u8.ToArray());

        var repository = new Mock<IMessageAttachmentRepository>();
        repository
            .Setup(r => r.GetAsync(address1, It.IsAny<CancellationToken>()))
            .ReturnsAsync(stream1);
        repository
            .Setup(r => r.GetAsync(address2, It.IsAny<CancellationToken>()))
            .ReturnsAsync(stream2);

        var middleware = new AttachmentMiddleware(repository.Object);

        var message = new TestMessageWithMultipleAttachments
        {
            Name = "Test",
            Attachment1 = new MessageData { Address = address1 },
            Attachment2 = new MessageData { Address = address2 },
        };

        var context = CreateContext(message, typeof(TestMessageWithMultipleAttachments));

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert
        repository.Verify(r => r.GetAsync(address1, It.IsAny<CancellationToken>()), Times.Once);
        repository.Verify(r => r.GetAsync(address2, It.IsAny<CancellationToken>()), Times.Once);
        repository.Verify(r => r.DeleteAsync(address1, It.IsAny<CancellationToken>()), Times.Once);
        repository.Verify(r => r.DeleteAsync(address2, It.IsAny<CancellationToken>()), Times.Once);
    }

    private static MessageContext CreateContext(
        object? deserializedMessage,
        Type? messageType = null,
        bool isBatch = false
    )
    {
        return new MessageContext
        {
            Messages = [CreateIncomingMessage()],
            QueueName = "test-queue",
            ProcessorType = typeof(object),
            MessageType = messageType ?? typeof(TestMessageWithAttachment),
            Options = new ProcessorOptions(),
            ServiceProvider = null!,
            CancellationToken = CancellationToken.None,
            IsBatchProcessor = isBatch,
            CompleteAllAsync = () => Task.CompletedTask,
            AbandonAllAsync = (_, _, _, _) => Task.CompletedTask,
            DeserializedMessages = deserializedMessage is not null ? [deserializedMessage] : null,
        };
    }

    private static IncomingMessage CreateIncomingMessage()
    {
        return new IncomingMessage
        {
            MessageId = Guid.NewGuid(),
            MessageDeliveryId = 1,
            LockId = Guid.NewGuid(),
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

    private sealed class TestMessageWithAttachment : IMessage
    {
        public string? Name { get; init; }
        public MessageData? Attachment { get; init; }
    }

    private sealed class TestMessageWithMultipleAttachments : IMessage
    {
        public string? Name { get; init; }
        public MessageData? Attachment1 { get; init; }
        public MessageData? Attachment2 { get; init; }
    }

    private sealed class TestMessageWithoutAttachment : IMessage
    {
        public string? Name { get; init; }
    }
}
