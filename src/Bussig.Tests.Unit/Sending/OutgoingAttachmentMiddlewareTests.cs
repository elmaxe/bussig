using Bussig.Abstractions;
using Bussig.Abstractions.Messages;
using Bussig.Abstractions.Middleware;
using Bussig.Attachments;
using Bussig.Sending;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;

namespace Bussig.Tests.Unit.Sending;

public class OutgoingAttachmentMiddlewareTests
{
    [Test]
    public async Task InvokeAsync_UploadsAttachment_WhenMessageHasMessageDataWithStream()
    {
        // Arrange
        var uploadedAddress = new Uri("https://test.blob.core.windows.net/container/blob");
        var attachmentContent = "attachment content"u8.ToArray();
        var attachmentStream = new MemoryStream(attachmentContent);

        var repository = new Mock<IMessageAttachmentRepository>();
        repository
            .Setup(r =>
                r.PutAsync(
                    It.IsAny<Stream>(),
                    It.IsAny<OutgoingMessageContext>(),
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(uploadedAddress);

        var serviceProvider = CreateServiceProvider(repository.Object);

        var message = new TestMessageWithAttachment
        {
            Name = "Test",
            Attachment = new MessageData(attachmentStream),
        };

        var context = CreateContext(message, serviceProvider);
        var nextCalled = false;

        var middleware = new OutgoingAttachmentMiddleware();

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
            r =>
                r.PutAsync(
                    It.IsAny<Stream>(),
                    It.IsAny<OutgoingMessageContext>(),
                    It.IsAny<CancellationToken>()
                ),
            Times.Once
        );

        // Verify the MessageData was replaced with one that has the address
        var updatedMessage = (TestMessageWithAttachment)context.Message;
        await Assert.That(updatedMessage.Attachment).IsNotNull();
        await Assert.That(updatedMessage.Attachment!.Address).IsEqualTo(uploadedAddress);
        await Assert.That(updatedMessage.Attachment.GetSendStream()).IsNull();
    }

    [Test]
    public async Task InvokeAsync_SkipsUpload_WhenMessageDataHasNoStream()
    {
        // Arrange
        var repository = new Mock<IMessageAttachmentRepository>();
        var serviceProvider = CreateServiceProvider(repository.Object);

        var message = new TestMessageWithAttachment
        {
            Name = "Test",
            Attachment = new MessageData(), // No stream
        };

        var context = CreateContext(message, serviceProvider);
        var nextCalled = false;

        var middleware = new OutgoingAttachmentMiddleware();

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
            r =>
                r.PutAsync(
                    It.IsAny<Stream>(),
                    It.IsAny<OutgoingMessageContext>(),
                    It.IsAny<CancellationToken>()
                ),
            Times.Never
        );
    }

    [Test]
    public async Task InvokeAsync_SkipsUpload_WhenMessageDataIsNull()
    {
        // Arrange
        var repository = new Mock<IMessageAttachmentRepository>();
        var serviceProvider = CreateServiceProvider(repository.Object);

        var message = new TestMessageWithAttachment { Name = "Test", Attachment = null };

        var context = CreateContext(message, serviceProvider);
        var nextCalled = false;

        var middleware = new OutgoingAttachmentMiddleware();

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
            r =>
                r.PutAsync(
                    It.IsAny<Stream>(),
                    It.IsAny<OutgoingMessageContext>(),
                    It.IsAny<CancellationToken>()
                ),
            Times.Never
        );
    }

    [Test]
    public async Task InvokeAsync_CallsNextMiddleware_WhenMessageHasNoMessageDataProperties()
    {
        // Arrange
        var repository = new Mock<IMessageAttachmentRepository>();
        var serviceProvider = CreateServiceProvider(repository.Object);

        var message = new TestMessageWithoutAttachment { Name = "Test" };

        var context = CreateContext(message, serviceProvider, typeof(TestMessageWithoutAttachment));
        var nextCalled = false;

        var middleware = new OutgoingAttachmentMiddleware();

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
            r =>
                r.PutAsync(
                    It.IsAny<Stream>(),
                    It.IsAny<OutgoingMessageContext>(),
                    It.IsAny<CancellationToken>()
                ),
            Times.Never
        );
    }

    [Test]
    public async Task InvokeAsync_HandlesMultipleMessageDataProperties()
    {
        // Arrange
        var address1 = new Uri("https://test.blob.core.windows.net/container/blob1");
        var address2 = new Uri("https://test.blob.core.windows.net/container/blob2");
        var stream1 = new MemoryStream("content1"u8.ToArray());
        var stream2 = new MemoryStream("content2"u8.ToArray());

        var callCount = 0;
        var repository = new Mock<IMessageAttachmentRepository>();
        repository
            .Setup(r =>
                r.PutAsync(
                    It.IsAny<Stream>(),
                    It.IsAny<OutgoingMessageContext>(),
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(() => callCount++ == 0 ? address1 : address2);

        var serviceProvider = CreateServiceProvider(repository.Object);

        var message = new TestMessageWithMultipleAttachments
        {
            Name = "Test",
            Attachment1 = new MessageData(stream1),
            Attachment2 = new MessageData(stream2),
        };

        var context = CreateContext(
            message,
            serviceProvider,
            typeof(TestMessageWithMultipleAttachments)
        );

        var middleware = new OutgoingAttachmentMiddleware();

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert
        repository.Verify(
            r =>
                r.PutAsync(
                    It.IsAny<Stream>(),
                    It.IsAny<OutgoingMessageContext>(),
                    It.IsAny<CancellationToken>()
                ),
            Times.Exactly(2)
        );

        var updatedMessage = (TestMessageWithMultipleAttachments)context.Message;
        await Assert.That(updatedMessage.Attachment1!.Address).IsEqualTo(address1);
        await Assert.That(updatedMessage.Attachment2!.Address).IsEqualTo(address2);
    }

    [Test]
    public async Task InvokeAsync_OnlyUploadsPropertiesWithData()
    {
        // Arrange
        var uploadedAddress = new Uri("https://test.blob.core.windows.net/container/blob");
        var stream = new MemoryStream("content"u8.ToArray());

        var repository = new Mock<IMessageAttachmentRepository>();
        repository
            .Setup(r =>
                r.PutAsync(
                    It.IsAny<Stream>(),
                    It.IsAny<OutgoingMessageContext>(),
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(uploadedAddress);

        var serviceProvider = CreateServiceProvider(repository.Object);

        var message = new TestMessageWithMultipleAttachments
        {
            Name = "Test",
            Attachment1 = new MessageData(stream), // Has data
            Attachment2 = new MessageData(), // No data
        };

        var context = CreateContext(
            message,
            serviceProvider,
            typeof(TestMessageWithMultipleAttachments)
        );

        var middleware = new OutgoingAttachmentMiddleware();

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert - Only one upload should happen
        repository.Verify(
            r =>
                r.PutAsync(
                    It.IsAny<Stream>(),
                    It.IsAny<OutgoingMessageContext>(),
                    It.IsAny<CancellationToken>()
                ),
            Times.Once
        );

        var updatedMessage = (TestMessageWithMultipleAttachments)context.Message;
        await Assert.That(updatedMessage.Attachment1!.Address).IsEqualTo(uploadedAddress);
    }

    [Test]
    public async Task InvokeAsync_SkipsReadOnlyProperties()
    {
        // Arrange
        var repository = new Mock<IMessageAttachmentRepository>();
        var serviceProvider = CreateServiceProvider(repository.Object);

        var message = new TestMessageWithReadOnlyAttachment("Test");

        var context = CreateContext(
            message,
            serviceProvider,
            typeof(TestMessageWithReadOnlyAttachment)
        );

        var middleware = new OutgoingAttachmentMiddleware();

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert - Should not try to upload since property is read-only
        repository.Verify(
            r =>
                r.PutAsync(
                    It.IsAny<Stream>(),
                    It.IsAny<OutgoingMessageContext>(),
                    It.IsAny<CancellationToken>()
                ),
            Times.Never
        );
    }

    [Test]
    public async Task InvokeAsync_InlinesSmallPayload_WhenUnderThreshold()
    {
        // Arrange
        var smallContent = "small"u8.ToArray();
        var repository = new Mock<IMessageAttachmentRepository>();
        var serviceProvider = CreateServiceProvider(repository.Object, inlineThreshold: 1024);

        var message = new TestMessageWithAttachment
        {
            Name = "Test",
            Attachment = new MessageData(new MemoryStream(smallContent)),
        };

        var context = CreateContext(message, serviceProvider);
        var middleware = new OutgoingAttachmentMiddleware();

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert - No upload, data inlined
        repository.Verify(
            r =>
                r.PutAsync(
                    It.IsAny<Stream>(),
                    It.IsAny<OutgoingMessageContext>(),
                    It.IsAny<CancellationToken>()
                ),
            Times.Never
        );

        var updatedMessage = (TestMessageWithAttachment)context.Message;
        await Assert.That(updatedMessage.Attachment).IsNotNull();
        await Assert
            .That(updatedMessage.Attachment!.InlineData!.SequenceEqual(smallContent))
            .IsTrue();
        await Assert.That(updatedMessage.Attachment.Address).IsNull();
    }

    [Test]
    public async Task InvokeAsync_UploadsLargePayload_WhenOverThreshold()
    {
        // Arrange
        var largeContent = new byte[2048];
        Array.Fill(largeContent, (byte)'x');
        var uploadedAddress = new Uri("https://test.blob.core.windows.net/container/blob");

        var repository = new Mock<IMessageAttachmentRepository>();
        repository
            .Setup(r =>
                r.PutAsync(
                    It.IsAny<Stream>(),
                    It.IsAny<OutgoingMessageContext>(),
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(uploadedAddress);

        var serviceProvider = CreateServiceProvider(repository.Object, inlineThreshold: 1024);

        var message = new TestMessageWithAttachment
        {
            Name = "Test",
            Attachment = new MessageData(new MemoryStream(largeContent)),
        };

        var context = CreateContext(message, serviceProvider);
        var middleware = new OutgoingAttachmentMiddleware();

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert - Should upload
        repository.Verify(
            r =>
                r.PutAsync(
                    It.IsAny<Stream>(),
                    It.IsAny<OutgoingMessageContext>(),
                    It.IsAny<CancellationToken>()
                ),
            Times.Once
        );

        var updatedMessage = (TestMessageWithAttachment)context.Message;
        await Assert.That(updatedMessage.Attachment!.Address).IsEqualTo(uploadedAddress);
        await Assert.That(updatedMessage.Attachment.InlineData).IsNull();
    }

    [Test]
    public async Task InvokeAsync_InlinesPayload_WhenExactlyAtThreshold()
    {
        // Arrange
        var content = new byte[100];
        Array.Fill(content, (byte)'x');
        var repository = new Mock<IMessageAttachmentRepository>();
        var serviceProvider = CreateServiceProvider(repository.Object, inlineThreshold: 100);

        var message = new TestMessageWithAttachment
        {
            Name = "Test",
            Attachment = new MessageData(new MemoryStream(content)),
        };

        var context = CreateContext(message, serviceProvider);
        var middleware = new OutgoingAttachmentMiddleware();

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert - Should inline (<=)
        repository.Verify(
            r =>
                r.PutAsync(
                    It.IsAny<Stream>(),
                    It.IsAny<OutgoingMessageContext>(),
                    It.IsAny<CancellationToken>()
                ),
            Times.Never
        );

        var updatedMessage = (TestMessageWithAttachment)context.Message;
        await Assert.That(updatedMessage.Attachment!.InlineData!.SequenceEqual(content)).IsTrue();
    }

    private static ServiceProvider CreateServiceProvider(
        IMessageAttachmentRepository repository,
        int inlineThreshold = 0
    )
    {
        var services = new ServiceCollection();
        services.AddSingleton(repository);
        services.Configure<AttachmentOptions>(o => o.InlineThreshold = inlineThreshold);
        return services.BuildServiceProvider();
    }

    private static OutgoingMessageContext CreateContext(
        object message,
        IServiceProvider serviceProvider,
        Type? messageType = null
    )
    {
        return new OutgoingMessageContext
        {
            Message = message,
            MessageType = messageType ?? typeof(TestMessageWithAttachment),
            Options = new MessageSendOptions(),
            QueueName = "test-queue",
            MessageTypes = ["test-queue"],
            ServiceProvider = serviceProvider,
            CancellationToken = CancellationToken.None,
        };
    }

    private sealed class TestMessageWithAttachment : IMessage
    {
        public string? Name { get; init; }
        public MessageData? Attachment { get; set; }
    }

    private sealed class TestMessageWithMultipleAttachments : IMessage
    {
        public string? Name { get; init; }
        public MessageData? Attachment1 { get; set; }
        public MessageData? Attachment2 { get; set; }
    }

    private sealed class TestMessageWithoutAttachment : IMessage
    {
        public string? Name { get; init; }
    }

    private sealed class TestMessageWithReadOnlyAttachment(string name) : IMessage
    {
        public string Name { get; } = name;

        // Read-only property with MessageData - should be skipped
        public MessageData Attachment { get; } = new(new MemoryStream("data"u8.ToArray()));
    }
}
