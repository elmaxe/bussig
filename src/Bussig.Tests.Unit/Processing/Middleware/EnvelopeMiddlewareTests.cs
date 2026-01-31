using System.Text.Json;
using Bussig.Abstractions;
using Bussig.Abstractions.Messages;
using Bussig.Abstractions.Middleware;
using Bussig.Abstractions.Options;
using Bussig.Processing.Middleware;

namespace Bussig.Tests.Unit.Processing.Middleware;

#pragma warning disable CA1861 // Avoid constant arrays - test data is fine

public class EnvelopeMiddlewareTests
{
    [Test]
    public async Task InvokeAsync_CreatesEnvelopesForAllMessages()
    {
        // Arrange
        var middleware = new EnvelopeMiddleware();

        var message1 = CreateIncomingMessage(Guid.NewGuid());
        var message2 = CreateIncomingMessage(Guid.NewGuid());

        var context = CreateContext([message1, message2]);
        context.DeserializedMessages = [new TestMessage(), new TestMessage()];

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
        await Assert.That(context.Envelopes).IsNotNull();
        await Assert.That(context.Envelopes!.Count).IsEqualTo(2);
        await Assert.That(context.Envelopes[0].MessageId).IsEqualTo(message1.MessageId);
        await Assert.That(context.Envelopes[1].MessageId).IsEqualTo(message2.MessageId);
        await Assert.That(nextCalled).IsTrue();
    }

    [Test]
    public async Task InvokeAsync_ParsesCorrelationId_FromHeaders()
    {
        // Arrange
        var middleware = new EnvelopeMiddleware();
        var correlationId = Guid.NewGuid();
        var headers = JsonSerializer.Serialize(
            new Dictionary<string, object> { ["correlation-id"] = correlationId.ToString() }
        );

        var message = CreateIncomingMessage(Guid.NewGuid(), headers);
        var context = CreateContext([message]);
        context.DeserializedMessages = [new TestMessage()];

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert
        await Assert.That(context.Envelope).IsNotNull();
        await Assert.That(context.Envelope!.CorrelationId).IsEqualTo(correlationId);
    }

    [Test]
    public async Task InvokeAsync_ParsesMessageType_FromHeaders()
    {
        // Arrange
        var middleware = new EnvelopeMiddleware();
        var headers = JsonSerializer.Serialize(
            new Dictionary<string, object>
            {
                ["message-types"] = new[] { "urn:message:MyNamespace:MyMessage" },
            }
        );

        var message = CreateIncomingMessage(Guid.NewGuid(), headers);
        var context = CreateContext([message]);
        context.DeserializedMessages = [new TestMessage()];

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert
        await Assert.That(context.Envelope).IsNotNull();
        await Assert
            .That(context.Envelope!.MessageType)
            .IsEqualTo("urn:message:MyNamespace:MyMessage");
    }

    [Test]
    public async Task InvokeAsync_ParsesCustomHeaders()
    {
        // Arrange
        var middleware = new EnvelopeMiddleware();
        var headers = JsonSerializer.Serialize(
            new Dictionary<string, object>
            {
                ["custom-header"] = "custom-value",
                ["numeric-header"] = 123,
                ["boolean-header"] = true,
            }
        );

        var message = CreateIncomingMessage(Guid.NewGuid(), headers);
        var context = CreateContext([message]);
        context.DeserializedMessages = [new TestMessage()];

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert
        await Assert.That(context.Envelope).IsNotNull();
        await Assert.That(context.Envelope!.Headers["custom-header"]).IsEqualTo("custom-value");
        await Assert.That(context.Envelope.Headers["numeric-header"]).IsEqualTo("123");
        await Assert.That(context.Envelope.Headers["boolean-header"]).IsEqualTo("true");
    }

    [Test]
    public async Task InvokeAsync_HandlesEmptyHeaders()
    {
        // Arrange
        var middleware = new EnvelopeMiddleware();

        var message = CreateIncomingMessage(Guid.NewGuid(), null);
        var context = CreateContext([message]);
        context.DeserializedMessages = [new TestMessage()];

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert
        await Assert.That(context.Envelope).IsNotNull();
        await Assert.That(context.Envelope!.Headers.Count).IsEqualTo(0);
        await Assert.That(context.Envelope.CorrelationId).IsNull();
    }

    [Test]
    public async Task InvokeAsync_HandlesInvalidHeadersJson()
    {
        // Arrange
        var middleware = new EnvelopeMiddleware();

        var message = CreateIncomingMessage(Guid.NewGuid(), "not-valid-json");
        var context = CreateContext([message]);
        context.DeserializedMessages = [new TestMessage()];

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert - should not throw, headers should be empty
        await Assert.That(context.Envelope).IsNotNull();
        await Assert.That(context.Envelope!.Headers.Count).IsEqualTo(0);
    }

    [Test]
    public async Task InvokeAsync_SetsTimestamp_FromEnqueuedAt()
    {
        // Arrange
        var middleware = new EnvelopeMiddleware();
        var enqueuedAt = DateTimeOffset.UtcNow.AddMinutes(-5);

        var message = CreateIncomingMessage(Guid.NewGuid()) with { EnqueuedAt = enqueuedAt };
        var context = CreateContext([message]);
        context.DeserializedMessages = [new TestMessage()];

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert
        await Assert.That(context.Envelope).IsNotNull();
        await Assert.That(context.Envelope!.Timestamp).IsEqualTo(enqueuedAt);
    }

    [Test]
    public async Task InvokeAsync_SetsPayload_ToDeserializedMessage()
    {
        // Arrange
        var middleware = new EnvelopeMiddleware();
        var testMessage = new TestMessage { Value = "test-payload" };

        var message = CreateIncomingMessage(Guid.NewGuid());
        var context = CreateContext([message]);
        context.DeserializedMessages = [testMessage];

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert
        await Assert.That(context.Envelope).IsNotNull();
        await Assert.That(context.Envelope!.Payload).IsEqualTo(testMessage);
    }

    [Test]
    public async Task InvokeAsync_SkipsProcessing_WhenIsHandledIsTrue()
    {
        // Arrange
        var middleware = new EnvelopeMiddleware();

        var context = CreateContext([CreateIncomingMessage(Guid.NewGuid())]);
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
        await Assert.That(context.Envelopes).IsNull();
        await Assert.That(nextCalled).IsTrue();
    }

    [Test]
    public async Task InvokeAsync_SkipsProcessing_WhenDeserializedMessagesIsNull()
    {
        // Arrange
        var middleware = new EnvelopeMiddleware();

        var context = CreateContext([CreateIncomingMessage(Guid.NewGuid())]);
        // DeserializedMessages is null by default

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
        await Assert.That(context.Envelopes).IsNull();
        await Assert.That(nextCalled).IsTrue();
    }

    [Test]
    public async Task InvokeAsync_UsesMessageTypeName_WhenNotInHeaders()
    {
        // Arrange
        var middleware = new EnvelopeMiddleware();

        var message = CreateIncomingMessage(Guid.NewGuid(), null);
        var context = CreateContext([message]);
        context.DeserializedMessages = [new TestMessage()];

        // Act
        await middleware.InvokeAsync(context, _ => Task.CompletedTask);

        // Assert
        await Assert.That(context.Envelope).IsNotNull();
        await Assert.That(context.Envelope!.MessageType).IsEqualTo("TestMessage");
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

    private static IncomingMessage CreateIncomingMessage(Guid messageId, string? headers = null)
    {
        return new IncomingMessage
        {
            MessageId = messageId,
            MessageDeliveryId = 1,
            LockId = Guid.NewGuid(),
            Body = [],
            Headers = headers,
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
