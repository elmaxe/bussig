using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using Bussig.Abstractions;
using Bussig.Abstractions.Messages;
using Bussig.Abstractions.Middleware;
using Bussig.Abstractions.Options;
using Bussig.Processing.Middleware;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Bussig.Tests.Unit.Processing.Middleware;

#pragma warning disable CA2263 // Prefer generic overload - mocking non-generic interface method

public class DeserializationMiddlewareTests
{
    [Test]
    public async Task InvokeAsync_DeserializesAllMessages()
    {
        // Arrange
        var message1 = new TestMessage { Value = "First" };
        var message2 = new TestMessage { Value = "Second" };
        var body1 = JsonSerializer.SerializeToUtf8Bytes(message1);
        var body2 = JsonSerializer.SerializeToUtf8Bytes(message2);

        var serializer = new Mock<IMessageSerializer>();
        serializer.Setup(s => s.Deserialize(body1, typeof(TestMessage))).Returns(message1);
        serializer.Setup(s => s.Deserialize(body2, typeof(TestMessage))).Returns(message2);

        var middleware = new DeserializationMiddleware(
            serializer.Object,
            NullLogger<DeserializationMiddleware>.Instance
        );

        var context = CreateContext([CreateIncomingMessage(body1), CreateIncomingMessage(body2)]);

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
        await Assert.That(context.DeserializedMessages).IsNotNull();
        await Assert.That(context.DeserializedMessages!.Count).IsEqualTo(2);
        await Assert.That(nextCalled).IsTrue();
    }

    [Test]
    public async Task InvokeAsync_SetsDeserializedMessages_OnContext()
    {
        // Arrange
        var testMessage = new TestMessage { Value = "Test" };
        var serializedBody = JsonSerializer.SerializeToUtf8Bytes(testMessage);

        var serializer = new Mock<IMessageSerializer>();
        serializer
            .Setup(s => s.Deserialize(serializedBody, typeof(TestMessage)))
            .Returns(testMessage);

        var middleware = new DeserializationMiddleware(
            serializer.Object,
            NullLogger<DeserializationMiddleware>.Instance
        );

        var context = CreateContext([CreateIncomingMessage(serializedBody)]);

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert
        await Assert.That(context.DeserializedMessages).IsNotNull();
        await Assert.That(context.DeserializedMessages!.Count).IsEqualTo(1);
        await Assert.That(((TestMessage)context.DeserializedMessages[0]).Value).IsEqualTo("Test");
    }

    [Test]
    public async Task InvokeAsync_SetsDeserializationFailed_WhenDeserializationReturnsNull()
    {
        // Arrange
        var serializer = new Mock<IMessageSerializer>();
        serializer
            .Setup(s => s.Deserialize(It.IsAny<byte[]>(), typeof(TestMessage)))
            .Returns((object?)null);

        var middleware = new DeserializationMiddleware(
            serializer.Object,
            NullLogger<DeserializationMiddleware>.Instance
        );

        var context = CreateContext([CreateIncomingMessage([1, 2, 3])]);
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
        await Assert.That(context.Items[MiddlewareConstants.DeserializationFailed]).IsEqualTo(true);
        await Assert.That(context.Items[MiddlewareConstants.ErrorCode]).IsEqualTo("NullMessage");
        await Assert.That(context.Exception).IsNotNull();
        await Assert.That(nextCalled).IsFalse();
    }

    [Test]
    public async Task InvokeAsync_SetsDeserializationFailed_WhenExceptionThrown()
    {
        // Arrange
        var serializer = new Mock<IMessageSerializer>();
        serializer
            .Setup(s => s.Deserialize(It.IsAny<byte[]>(), typeof(TestMessage)))
            .Throws(new JsonException("Invalid JSON"));

        var middleware = new DeserializationMiddleware(
            serializer.Object,
            NullLogger<DeserializationMiddleware>.Instance
        );

        var context = CreateContext([CreateIncomingMessage([1, 2, 3])]);
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
        await Assert.That(context.Items[MiddlewareConstants.DeserializationFailed]).IsEqualTo(true);
        await Assert
            .That(context.Items[MiddlewareConstants.ErrorCode])
            .IsEqualTo("DeserializationFailed");
        await Assert
            .That(context.Items[MiddlewareConstants.ErrorMessage])
            .IsEqualTo("Invalid JSON");
        await Assert.That(context.Exception).IsNotNull();
        await Assert.That(nextCalled).IsFalse();
    }

    [Test]
    public async Task InvokeAsync_SkipsProcessing_WhenIsHandledIsTrue()
    {
        // Arrange
        var serializer = new Mock<IMessageSerializer>();

        var middleware = new DeserializationMiddleware(
            serializer.Object,
            NullLogger<DeserializationMiddleware>.Instance
        );

        var context = CreateContext([CreateIncomingMessage([])]);
        context.IsHandled = true;

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert
        serializer.Verify(s => s.Deserialize(It.IsAny<byte[]>(), It.IsAny<Type>()), Times.Never);
        await Assert.That(context.DeserializedMessages).IsNull();
    }

    [Test]
    public async Task InvokeAsync_StopsAtFirstFailure_InBatch()
    {
        // Arrange
        var validMessage = new TestMessage { Value = "Valid" };
        var validBody = JsonSerializer.SerializeToUtf8Bytes(validMessage);
        var invalidBody = new byte[] { 1, 2, 3 }; // Invalid JSON

        var callCount = 0;
        var serializer = new Mock<IMessageSerializer>();
        serializer
            .Setup(s => s.Deserialize(validBody, typeof(TestMessage)))
            .Returns(() =>
            {
                callCount++;
                return validMessage;
            });
        serializer
            .Setup(s => s.Deserialize(invalidBody, typeof(TestMessage)))
            .Returns(() =>
            {
                callCount++;
                throw new JsonException("Invalid");
            });

        var middleware = new DeserializationMiddleware(
            serializer.Object,
            NullLogger<DeserializationMiddleware>.Instance
        );

        var context = CreateContext([
            CreateIncomingMessage(validBody),
            CreateIncomingMessage(invalidBody),
        ]);

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert
        await Assert.That(context.Items[MiddlewareConstants.DeserializationFailed]).IsEqualTo(true);
        await Assert.That(callCount).IsEqualTo(2);
    }

    private static MessageContext CreateContext(IReadOnlyList<IncomingMessage> messages)
    {
        return new MessageContext
        {
            Messages = messages,
            QueueName = "test-queue",
            ProcessorType = typeof(object),
            MessageType = typeof(TestMessage),
            Options = new ProcessorOptions(),
            ServiceProvider = null!,
            CancellationToken = CancellationToken.None,
            IsBatchProcessor = false,
            CompleteAllAsync = () => Task.CompletedTask,
            AbandonAllAsync = _ => Task.CompletedTask,
        };
    }

    private static IncomingMessage CreateIncomingMessage(byte[] body, Guid? messageId = null)
    {
        return new IncomingMessage
        {
            MessageId = messageId ?? Guid.NewGuid(),
            MessageDeliveryId = 1,
            LockId = Guid.NewGuid(),
            Body = body,
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

    private sealed class TestMessage : IMessage
    {
        public string? Value { get; init; }
    }
}
