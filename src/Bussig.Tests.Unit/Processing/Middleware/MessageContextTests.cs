using Bussig.Abstractions;
using Bussig.Abstractions.Messages;
using Bussig.Abstractions.Middleware;
using Bussig.Abstractions.Options;

namespace Bussig.Tests.Unit.Processing.Middleware;

public class MessageContextTests
{
    [Test]
    public async Task Message_ReturnsSingleMessage_WhenBatchOfOne()
    {
        // Arrange
        var incomingMessage = CreateIncomingMessage();
        var context = CreateContext([incomingMessage]);

        // Act
        var message = context.Message;

        // Assert
        await Assert.That(message).IsEqualTo(incomingMessage);
    }

    [Test]
    public async Task Message_ReturnsFirstMessage_WhenMultipleMessages()
    {
        // Arrange
        var message1 = CreateIncomingMessage(Guid.NewGuid());
        var message2 = CreateIncomingMessage(Guid.NewGuid());
        var context = CreateContext([message1, message2]);

        // Act
        var message = context.Message;

        // Assert
        await Assert.That(message).IsEqualTo(message1);
    }

    [Test]
    public async Task DeserializedMessage_ReturnsFirst_WhenSet()
    {
        // Arrange
        var context = CreateContext([CreateIncomingMessage()]);
        var deserialized1 = new TestMessage { Value = "First" };
        var deserialized2 = new TestMessage { Value = "Second" };
        context.DeserializedMessages = [deserialized1, deserialized2];

        // Act
        var result = context.DeserializedMessage;

        // Assert
        await Assert.That(result).IsEqualTo(deserialized1);
    }

    [Test]
    public async Task DeserializedMessage_ReturnsNull_WhenNotSet()
    {
        // Arrange
        var context = CreateContext([CreateIncomingMessage()]);

        // Act
        var result = context.DeserializedMessage;

        // Assert
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task Envelope_ReturnsFirst_WhenSet()
    {
        // Arrange
        var context = CreateContext([CreateIncomingMessage()]);
        var envelope1 = CreateEnvelope(Guid.NewGuid());
        var envelope2 = CreateEnvelope(Guid.NewGuid());
        context.Envelopes = [envelope1, envelope2];

        // Act
        var result = context.Envelope;

        // Assert
        await Assert.That(result).IsEqualTo(envelope1);
    }

    [Test]
    public async Task Envelope_ReturnsNull_WhenNotSet()
    {
        // Arrange
        var context = CreateContext([CreateIncomingMessage()]);

        // Act
        var result = context.Envelope;

        // Assert
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task ProcessorContext_ReturnsFirst_WhenSet()
    {
        // Arrange
        var context = CreateContext([CreateIncomingMessage()]);
        var processorContext1 = new object();
        var processorContext2 = new object();
        context.ProcessorContexts = [processorContext1, processorContext2];

        // Act
        var result = context.ProcessorContext;

        // Assert
        await Assert.That(result).IsEqualTo(processorContext1);
    }

    [Test]
    public async Task Items_PersistsDataBetweenAccesses()
    {
        // Arrange
        var context = CreateContext([CreateIncomingMessage()]);
        context.Items["key1"] = "value1";
        context.Items["key2"] = 123;

        // Act & Assert
        await Assert.That(context.Items["key1"]).IsEqualTo("value1");
        await Assert.That(context.Items["key2"]).IsEqualTo(123);
    }

    [Test]
    public async Task GetItem_ReturnsTypedItem_WhenExists()
    {
        // Arrange
        var context = CreateContext([CreateIncomingMessage()]);
        var testObject = new TestMessage { Value = "test" };
        context.SetItem("myKey", testObject);

        // Act
        var result = context.GetItem<TestMessage>("myKey");

        // Assert
        await Assert.That(result).IsEqualTo(testObject);
    }

    [Test]
    public async Task GetItem_ReturnsNull_WhenKeyDoesNotExist()
    {
        // Arrange
        var context = CreateContext([CreateIncomingMessage()]);

        // Act
        var result = context.GetItem<TestMessage>("nonexistent");

        // Assert
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task GetItem_ReturnsNull_WhenTypeDoesNotMatch()
    {
        // Arrange
        var context = CreateContext([CreateIncomingMessage()]);
        context.Items["key"] = "a string";

        // Act
        var result = context.GetItem<TestMessage>("key");

        // Assert
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task SetItem_StoresNullValue()
    {
        // Arrange
        var context = CreateContext([CreateIncomingMessage()]);

        // Act
        context.SetItem<TestMessage>("key", null);

        // Assert
        await Assert.That(context.Items.ContainsKey("key")).IsTrue();
        await Assert.That(context.Items["key"]).IsNull();
    }

    [Test]
    public async Task Messages_ReturnsAllMessages()
    {
        // Arrange
        var message1 = CreateIncomingMessage(Guid.NewGuid());
        var message2 = CreateIncomingMessage(Guid.NewGuid());
        var message3 = CreateIncomingMessage(Guid.NewGuid());
        var context = CreateContext([message1, message2, message3]);

        // Act & Assert
        await Assert.That(context.Messages.Count).IsEqualTo(3);
        await Assert.That(context.Messages[0]).IsEqualTo(message1);
        await Assert.That(context.Messages[1]).IsEqualTo(message2);
        await Assert.That(context.Messages[2]).IsEqualTo(message3);
    }

    [Test]
    public async Task IsBatchProcessor_ReflectsInitializedValue()
    {
        // Arrange
        var context = CreateContext([CreateIncomingMessage()], isBatchProcessor: true);

        // Assert
        await Assert.That(context.IsBatchProcessor).IsTrue();
    }

    [Test]
    public async Task IsHandled_DefaultsToFalse()
    {
        // Arrange
        var context = CreateContext([CreateIncomingMessage()]);

        // Assert
        await Assert.That(context.IsHandled).IsFalse();
    }

    [Test]
    public async Task IsHandled_CanBeSet()
    {
        // Arrange
        var context = CreateContext([CreateIncomingMessage()]);

        // Act
        context.IsHandled = true;

        // Assert
        await Assert.That(context.IsHandled).IsTrue();
    }

    [Test]
    public async Task Exception_DefaultsToNull()
    {
        // Arrange
        var context = CreateContext([CreateIncomingMessage()]);

        // Assert
        await Assert.That(context.Exception).IsNull();
    }

    [Test]
    public async Task Exception_CanBeSet()
    {
        // Arrange
        var context = CreateContext([CreateIncomingMessage()]);
        var ex = new InvalidOperationException("Test");

        // Act
        context.Exception = ex;

        // Assert
        await Assert.That(context.Exception).IsEqualTo(ex);
    }

    private static MessageContext CreateContext(
        IReadOnlyList<IncomingMessage> messages,
        bool isBatchProcessor = false
    )
    {
        return new MessageContext
        {
            Messages = messages,
            QueueName = "test-queue",
            ProcessorType = typeof(TestProcessor),
            MessageType = typeof(TestMessage),
            Options = new ProcessorOptions(),
            ServiceProvider = null!,
            CancellationToken = CancellationToken.None,
            IsBatchProcessor = isBatchProcessor,
            CompleteAllAsync = () => Task.CompletedTask,
            AbandonAllAsync = _ => Task.CompletedTask,
        };
    }

    private static IncomingMessage CreateIncomingMessage(Guid? messageId = null)
    {
        return new IncomingMessage
        {
            MessageId = messageId ?? Guid.NewGuid(),
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

    private static MessageEnvelope CreateEnvelope(Guid messageId)
    {
        return new MessageEnvelope
        {
            MessageId = messageId,
            MessageType = "TestMessage",
            Timestamp = DateTimeOffset.UtcNow,
            Payload = new TestMessage(),
        };
    }

    private sealed class TestMessage : IMessage
    {
        public string? Value { get; init; }
    }

    private sealed class TestProcessor : IProcessor<TestMessage>
    {
        public Task ProcessAsync(
            ProcessorContext<TestMessage> context,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
        }
    }
}
