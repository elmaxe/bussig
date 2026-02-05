using System.Reflection;
using Bussig.Abstractions;
using Bussig.Abstractions.Messages;
using Bussig.Processing;

namespace Bussig.Tests.Unit.Processing;

public class MessageProcessorContextTests
{
    [Test]
    public async Task Constructor_SetsMessageProperty()
    {
        // Arrange
        var body = new TestMessage { Value = "hello world" };
        var envelope = CreateEnvelope();
        var delivery = CreateDeliveryInfo();

        // Act
        var context = CreateContext(body, envelope, delivery);

        // Assert
        await Assert.That(context.Message).IsEqualTo(body);
        await Assert.That(context.Message.Value).IsEqualTo("hello world");
    }

    [Test]
    public async Task Constructor_SetsEnvelopeProperty()
    {
        // Arrange
        var body = new TestMessage { Value = "test" };
        var envelope = CreateEnvelope();
        var delivery = CreateDeliveryInfo();

        // Act
        var context = CreateContext(body, envelope, delivery);

        // Assert
        await Assert.That(context.Envelope).IsEqualTo(envelope);
    }

    [Test]
    public async Task Constructor_SetsDeliveryProperty()
    {
        // Arrange
        var body = new TestMessage { Value = "test" };
        var envelope = CreateEnvelope();
        var delivery = CreateDeliveryInfo(deliveryCount: 3, maxDeliveryCount: 10);

        // Act
        var context = CreateContext(body, envelope, delivery);

        // Assert
        await Assert.That(context.Delivery).IsEqualTo(delivery);
    }

    [Test]
    public async Task MessageId_DelegatesToEnvelope()
    {
        // Arrange
        var messageId = Guid.NewGuid();
        var body = new TestMessage { Value = "test" };
        var envelope = CreateEnvelope(messageId: messageId);
        var delivery = CreateDeliveryInfo();

        // Act
        var context = CreateContext(body, envelope, delivery);

        // Assert
        await Assert.That(context.MessageId).IsEqualTo(messageId);
        await Assert.That(context.MessageId).IsEqualTo(context.Envelope.MessageId);
    }

    [Test]
    public async Task CorrelationId_DelegatesToEnvelope()
    {
        // Arrange
        var correlationId = Guid.NewGuid();
        var body = new TestMessage { Value = "test" };
        var envelope = CreateEnvelope(correlationId: correlationId);
        var delivery = CreateDeliveryInfo();

        // Act
        var context = CreateContext(body, envelope, delivery);

        // Assert
        await Assert.That(context.CorrelationId).IsEqualTo(correlationId);
        await Assert.That(context.CorrelationId).IsEqualTo(context.Envelope.CorrelationId);
    }

    [Test]
    public async Task CorrelationId_ReturnsNull_WhenNotSet()
    {
        // Arrange
        var body = new TestMessage { Value = "test" };
        var envelope = CreateEnvelope(correlationId: null);
        var delivery = CreateDeliveryInfo();

        // Act
        var context = CreateContext(body, envelope, delivery);

        // Assert
        await Assert.That(context.CorrelationId).IsNull();
    }

    [Test]
    public async Task DeliveryCount_DelegatesToDelivery()
    {
        // Arrange
        var body = new TestMessage { Value = "test" };
        var envelope = CreateEnvelope();
        var delivery = CreateDeliveryInfo(deliveryCount: 5);

        // Act
        var context = CreateContext(body, envelope, delivery);

        // Assert
        await Assert.That(context.DeliveryCount).IsEqualTo(5);
        await Assert.That(context.DeliveryCount).IsEqualTo(context.Delivery.DeliveryCount);
    }

    [Test]
    public async Task MaxDeliveryCount_DelegatesToDelivery()
    {
        // Arrange
        var body = new TestMessage { Value = "test" };
        var envelope = CreateEnvelope();
        var delivery = CreateDeliveryInfo(maxDeliveryCount: 15);

        // Act
        var context = CreateContext(body, envelope, delivery);

        // Assert
        await Assert.That(context.MaxDeliveryCount).IsEqualTo(15);
        await Assert.That(context.MaxDeliveryCount).IsEqualTo(context.Delivery.MaxDeliveryCount);
    }

    [Test]
    public async Task EnqueuedAt_DelegatesToDelivery()
    {
        // Arrange
        var enqueuedAt = DateTimeOffset.UtcNow.AddMinutes(-5);
        var body = new TestMessage { Value = "test" };
        var envelope = CreateEnvelope();
        var delivery = CreateDeliveryInfo(enqueuedAt: enqueuedAt);

        // Act
        var context = CreateContext(body, envelope, delivery);

        // Assert
        await Assert.That(context.EnqueuedAt).IsEqualTo(enqueuedAt);
        await Assert.That(context.EnqueuedAt).IsEqualTo(context.Delivery.EnqueuedAt);
    }

    [Test]
    public async Task Envelope_ExposesMessageTypes()
    {
        // Arrange
        var body = new TestMessage { Value = "test" };
        var messageTypes = new[] { "urn:message:TestMessage", "urn:message:IMessage" };
        var envelope = CreateEnvelope(messageTypes: messageTypes);
        var delivery = CreateDeliveryInfo();

        // Act
        var context = CreateContext(body, envelope, delivery);

        // Assert
        await Assert.That(context.Envelope.MessageTypes).IsEquivalentTo(messageTypes);
    }

    [Test]
    public async Task Envelope_ExposesHeaders()
    {
        // Arrange
        var body = new TestMessage { Value = "test" };
        var headers = new Dictionary<string, string>
        {
            ["custom-header"] = "custom-value",
            ["another-header"] = "another-value",
        };
        var envelope = CreateEnvelope(headers: headers);
        var delivery = CreateDeliveryInfo();

        // Act
        var context = CreateContext(body, envelope, delivery);

        // Assert
        await Assert.That(context.Envelope.Headers["custom-header"]).IsEqualTo("custom-value");
        await Assert.That(context.Envelope.Headers["another-header"]).IsEqualTo("another-value");
    }

    [Test]
    public async Task Envelope_ExposesSentAt()
    {
        // Arrange
        var sentAt = DateTimeOffset.UtcNow.AddMinutes(-10);
        var body = new TestMessage { Value = "test" };
        var envelope = CreateEnvelope(sentAt: sentAt);
        var delivery = CreateDeliveryInfo();

        // Act
        var context = CreateContext(body, envelope, delivery);

        // Assert
        await Assert.That(context.Envelope.SentAt).IsEqualTo(sentAt);
    }

    [Test]
    public async Task Delivery_ExposesLastDeliveredAt()
    {
        // Arrange
        var lastDeliveredAt = DateTimeOffset.UtcNow.AddSeconds(-30);
        var body = new TestMessage { Value = "test" };
        var envelope = CreateEnvelope();
        var delivery = CreateDeliveryInfo(lastDeliveredAt: lastDeliveredAt);

        // Act
        var context = CreateContext(body, envelope, delivery);

        // Assert
        await Assert.That(context.Delivery.LastDeliveredAt).IsEqualTo(lastDeliveredAt);
    }

    [Test]
    public async Task Delivery_ExposesDeliveryHeaders()
    {
        // Arrange
        var body = new TestMessage { Value = "test" };
        var envelope = CreateEnvelope();
        var deliveryHeaders = new Dictionary<string, string> { ["error"] = "previous error" };
        var delivery = CreateDeliveryInfo(deliveryHeaders: deliveryHeaders);

        // Act
        var context = CreateContext(body, envelope, delivery);

        // Assert
        var contextDeliveryHeaders = context.Delivery.DeliveryHeaders;
        await Assert.That(contextDeliveryHeaders).IsNotNull();
        await Assert.That(contextDeliveryHeaders!["error"]).IsEqualTo("previous error");
    }

    [Test]
    public async Task ImplementsProcessorContextInterface()
    {
        // Arrange
        var body = new TestMessage { Value = "test" };
        var envelope = CreateEnvelope();
        var delivery = CreateDeliveryInfo();

        // Act
        var context = CreateContext(body, envelope, delivery);

        // Assert
        await Assert.That(context).IsAssignableTo<ProcessorContext<TestMessage>>();
    }

    [Test]
    public async Task WorksWithDifferentMessageTypes()
    {
        // Arrange
        var body = new AnotherTestMessage { Id = 42, Name = "test" };
        var envelope = CreateEnvelope();
        var delivery = CreateDeliveryInfo();

        // Act
        var context = CreateContextViaReflection(
            body,
            envelope,
            delivery,
            typeof(AnotherTestMessage)
        );

        // Assert
        await Assert.That(context).IsTypeOf<MessageProcessorContext<AnotherTestMessage>>();
        var typedContext = (MessageProcessorContext<AnotherTestMessage>)context;
        await Assert.That(typedContext.Message.Id).IsEqualTo(42);
        await Assert.That(typedContext.Message.Name).IsEqualTo("test");
    }

    private static MessageProcessorContext<TestMessage> CreateContext(
        TestMessage message,
        MessageEnvelope envelope,
        DeliveryInfo delivery,
        long messageDeliveryId = 1,
        Guid? lockId = null
    )
    {
        // Use reflection to call internal constructor
        var contextType = typeof(MessageProcessorContext<TestMessage>);
        var context = Activator.CreateInstance(
            contextType,
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            args: [message, envelope, delivery, messageDeliveryId, lockId ?? Guid.NewGuid()],
            culture: null
        );

        return (MessageProcessorContext<TestMessage>)context!;
    }

    private static object CreateContextViaReflection(
        object message,
        MessageEnvelope envelope,
        DeliveryInfo delivery,
        Type messageType,
        long messageDeliveryId = 1,
        Guid? lockId = null
    )
    {
        var contextType = typeof(MessageProcessorContext<>).MakeGenericType(messageType);
        var context = Activator.CreateInstance(
            contextType,
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            args: [message, envelope, delivery, messageDeliveryId, lockId ?? Guid.NewGuid()],
            culture: null
        );

        return context!;
    }

    private static MessageEnvelope CreateEnvelope(
        Guid? messageId = null,
        Guid? correlationId = null,
        DateTimeOffset? sentAt = null,
        IReadOnlyList<string>? messageTypes = null,
        IReadOnlyDictionary<string, string>? headers = null
    )
    {
        return new MessageEnvelope
        {
            MessageId = messageId ?? Guid.NewGuid(),
            SentAt = sentAt ?? DateTimeOffset.UtcNow,
            MessageTypes = messageTypes ?? ["urn:message:TestMessage"],
            CorrelationId = correlationId,
            Headers = headers ?? new Dictionary<string, string>(),
        };
    }

    private static DeliveryInfo CreateDeliveryInfo(
        int deliveryCount = 1,
        int maxDeliveryCount = 5,
        DateTimeOffset? enqueuedAt = null,
        DateTimeOffset? lastDeliveredAt = null,
        IReadOnlyDictionary<string, string>? deliveryHeaders = null
    )
    {
        return new DeliveryInfo
        {
            DeliveryCount = deliveryCount,
            MaxDeliveryCount = maxDeliveryCount,
            EnqueuedAt = enqueuedAt ?? DateTimeOffset.UtcNow,
            LastDeliveredAt = lastDeliveredAt,
            DeliveryHeaders = deliveryHeaders,
        };
    }

    // Test message types
    private sealed record TestMessage : IMessage
    {
        public string Value { get; init; } = "";
    }

    private sealed record AnotherTestMessage : IMessage
    {
        public int Id { get; init; }
        public string Name { get; init; } = "";
    }
}
