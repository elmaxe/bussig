using Bussig.Abstractions.Options;
using Bussig.Processing.Internal;

namespace Bussig.Tests.Unit.Processing;

public class ProcessorConfigurationTests
{
    [Test]
    public async Task ProcessorConfiguration_StoresQueueName()
    {
        // Arrange & Act
        var context = CreateContext(queueName: "test-queue");

        // Assert
        await Assert.That(context.QueueName).IsEqualTo("test-queue");
    }

    [Test]
    public async Task ProcessorConfiguration_StoresMessageType()
    {
        // Arrange & Act
        var context = CreateContext(messageType: typeof(string));

        // Assert
        await Assert.That(context.MessageType).IsEqualTo(typeof(string));
    }

    [Test]
    public async Task ProcessorConfiguration_StoresProcessorType()
    {
        // Arrange & Act
        var context = CreateContext(processorType: typeof(TestProcessor));

        // Assert
        await Assert.That(context.ProcessorType).IsEqualTo(typeof(TestProcessor));
    }

    [Test]
    public async Task ProcessorConfiguration_StoresResponseMessageType()
    {
        // Arrange & Act
        var context = CreateContext(responseMessageType: typeof(int));

        // Assert
        await Assert.That(context.ResponseMessageType).IsEqualTo(typeof(int));
    }

    [Test]
    public async Task ProcessorConfiguration_ResponseMessageType_CanBeNull()
    {
        // Arrange & Act
        var context = CreateContext(responseMessageType: null);

        // Assert
        await Assert.That(context.ResponseMessageType).IsNull();
    }

    [Test]
    public async Task ProcessorConfiguration_StoresBatchMessageType()
    {
        // Arrange & Act
        var context = CreateContext(batchMessageType: typeof(double));

        // Assert
        await Assert.That(context.BatchMessageType).IsEqualTo(typeof(double));
    }

    [Test]
    public async Task ProcessorConfiguration_BatchMessageType_CanBeNull()
    {
        // Arrange & Act
        var context = CreateContext(batchMessageType: null);

        // Assert
        await Assert.That(context.BatchMessageType).IsNull();
    }

    [Test]
    public async Task ProcessorConfiguration_StoresOptions()
    {
        // Arrange
        var options = new ProcessorOptions();
        options.Polling.MaxConcurrency = 10;

        // Act
        var context = CreateContext(options: options);

        // Assert
        await Assert.That(context.Options).IsEqualTo(options);
        await Assert.That(context.Options.Polling.MaxConcurrency).IsEqualTo(10);
    }

    [Test]
    public async Task ProcessorConfiguration_IsRecord_SupportsEquality()
    {
        // Arrange
        var options = new ProcessorOptions();
        var context1 = new ProcessorConfiguration
        {
            QueueName = "queue",
            MessageType = typeof(string),
            ProcessorType = typeof(TestProcessor),
            ResponseMessageType = null,
            BatchMessageType = null,
            Options = options,
        };
        var context2 = new ProcessorConfiguration
        {
            QueueName = "queue",
            MessageType = typeof(string),
            ProcessorType = typeof(TestProcessor),
            ResponseMessageType = null,
            BatchMessageType = null,
            Options = options,
        };

        // Assert
        await Assert.That(context1).IsEqualTo(context2);
    }

    [Test]
    public async Task ProcessorConfiguration_WithDifferentQueueName_AreNotEqual()
    {
        // Arrange
        var options = new ProcessorOptions();
        var context1 = CreateContext(queueName: "queue-1", options: options);
        var context2 = CreateContext(queueName: "queue-2", options: options);

        // Assert
        await Assert.That(context1).IsNotEqualTo(context2);
    }

    [Test]
    public async Task ProcessorConfiguration_WithDifferentMessageType_AreNotEqual()
    {
        // Arrange
        var options = new ProcessorOptions();
        var context1 = CreateContext(messageType: typeof(string), options: options);
        var context2 = CreateContext(messageType: typeof(int), options: options);

        // Assert
        await Assert.That(context1).IsNotEqualTo(context2);
    }

    private static ProcessorConfiguration CreateContext(
        string queueName = "default-queue",
        Type? messageType = null,
        Type? processorType = null,
        Type? responseMessageType = null,
        Type? batchMessageType = null,
        ProcessorOptions? options = null
    )
    {
        return new ProcessorConfiguration
        {
            QueueName = queueName,
            MessageType = messageType ?? typeof(object),
            ProcessorType = processorType ?? typeof(TestProcessor),
            ResponseMessageType = responseMessageType,
            BatchMessageType = batchMessageType,
            Options = options ?? new ProcessorOptions(),
        };
    }

    // Test types for processor
    private sealed class TestProcessor { }
}
