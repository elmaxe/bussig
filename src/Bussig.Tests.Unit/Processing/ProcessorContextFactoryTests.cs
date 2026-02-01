using Bussig.Abstractions;
using Bussig.Abstractions.Messages;
using Bussig.Processing;
using Bussig.Processing.Internal;

namespace Bussig.Tests.Unit.Processing;

public class ProcessorContextFactoryTests
{
    [Test]
    public async Task CreateContext_ReturnsCorrectContextType()
    {
        // Arrange
        var message = CreateIncomingMessage();
        var body = new TestMessage { Value = "test" };

        // Act
        var context = ProcessorContextFactory.CreateContext(message, body, typeof(TestMessage));

        // Assert
        await Assert.That(context).IsTypeOf<MessageProcessorContext<TestMessage>>();
    }

    [Test]
    public async Task CreateContext_SetsMessageProperty()
    {
        // Arrange
        var message = CreateIncomingMessage();
        var body = new TestMessage { Value = "hello world" };

        // Act
        var context =
            (MessageProcessorContext<TestMessage>)
                ProcessorContextFactory.CreateContext(message, body, typeof(TestMessage));

        // Assert
        await Assert.That(context.Message).IsEqualTo(body);
        await Assert.That(context.Message.Value).IsEqualTo("hello world");
    }

    [Test]
    public async Task CreateContext_SetsMessageId()
    {
        // Arrange
        var messageId = Guid.NewGuid();
        var message = CreateIncomingMessage(messageId: messageId);
        var body = new TestMessage { Value = "test" };

        // Act
        var context =
            (MessageProcessorContext<TestMessage>)
                ProcessorContextFactory.CreateContext(message, body, typeof(TestMessage));

        // Assert
        await Assert.That(context.MessageId).IsEqualTo(messageId);
    }

    [Test]
    public async Task CreateContext_SetsDeliveryCount()
    {
        // Arrange
        var message = CreateIncomingMessage(deliveryCount: 3);
        var body = new TestMessage { Value = "test" };

        // Act
        var context =
            (MessageProcessorContext<TestMessage>)
                ProcessorContextFactory.CreateContext(message, body, typeof(TestMessage));

        // Assert
        await Assert.That(context.DeliveryCount).IsEqualTo(3);
    }

    [Test]
    public async Task CreateContext_SetsMaxDeliveryCount()
    {
        // Arrange
        var message = CreateIncomingMessage(maxDeliveryCount: 10);
        var body = new TestMessage { Value = "test" };

        // Act
        var context =
            (MessageProcessorContext<TestMessage>)
                ProcessorContextFactory.CreateContext(message, body, typeof(TestMessage));

        // Assert
        await Assert.That(context.MaxDeliveryCount).IsEqualTo(10);
    }

    [Test]
    public async Task CreateContext_SetsEnqueuedAt()
    {
        // Arrange
        var enqueuedAt = DateTimeOffset.UtcNow.AddMinutes(-5);
        var message = CreateIncomingMessage(enqueuedAt: enqueuedAt);
        var body = new TestMessage { Value = "test" };

        // Act
        var context =
            (MessageProcessorContext<TestMessage>)
                ProcessorContextFactory.CreateContext(message, body, typeof(TestMessage));

        // Assert
        await Assert.That(context.EnqueuedAt).IsEqualTo(enqueuedAt);
    }

    [Test]
    public async Task CreateContext_WorksWithDifferentMessageTypes()
    {
        // Arrange
        var message = CreateIncomingMessage();
        var body = new AnotherTestMessage { Id = 42, Name = "test" };

        // Act
        var context = ProcessorContextFactory.CreateContext(
            message,
            body,
            typeof(AnotherTestMessage)
        );

        // Assert
        await Assert.That(context).IsTypeOf<MessageProcessorContext<AnotherTestMessage>>();
        var typedContext = (MessageProcessorContext<AnotherTestMessage>)context;
        await Assert.That(typedContext.Message.Id).IsEqualTo(42);
        await Assert.That(typedContext.Message.Name).IsEqualTo("test");
    }

    [Test]
    public async Task CreateContext_WithNullBody_SetsMessageToNull()
    {
        // Arrange
        var message = CreateIncomingMessage();

        // Act - CreateContext uses reflection and will set the message to null
        var context =
            (MessageProcessorContext<TestMessage>)
                ProcessorContextFactory.CreateContext(message, null!, typeof(TestMessage));

        // Assert - The context is created but Message property will be null
        await Assert.That(context).IsNotNull();
        await Assert.That(context.Message).IsNull();
    }

    private static IncomingMessage CreateIncomingMessage(
        Guid? messageId = null,
        int deliveryCount = 1,
        int maxDeliveryCount = 5,
        DateTimeOffset? enqueuedAt = null
    )
    {
        return new IncomingMessage
        {
            MessageId = messageId ?? Guid.NewGuid(),
            MessageDeliveryId = 1,
            LockId = Guid.NewGuid(),
            Body = [],
            Headers = null,
            MessageDeliveryHeaders = null,
            DeliveryCount = deliveryCount,
            MaxDeliveryCount = maxDeliveryCount,
            MessageVersion = 1,
            EnqueuedAt = enqueuedAt ?? DateTimeOffset.UtcNow,
            LastDeliveredAt = null,
            VisibleAt = DateTimeOffset.UtcNow,
            ExpirationTime = null,
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
