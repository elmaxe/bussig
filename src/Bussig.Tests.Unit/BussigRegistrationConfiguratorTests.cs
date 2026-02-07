using Bussig.Abstractions;
using Bussig.Abstractions.Messages;

namespace Bussig.Tests.Unit;

public class BussigRegistrationConfiguratorTests
{
    [Test]
    public async Task AddProcessor_WithTypeInference_RegistersCorrectMessageType()
    {
        // Arrange
        var configurator = new BussigRegistrationConfigurator();

        // Act
        configurator.AddProcessor<TestProcessor>();

        // Assert
        await Assert.That(configurator.ProcessorRegistrations.Count).IsEqualTo(1);
        var registration = configurator.ProcessorRegistrations[0];
        await Assert.That(registration.MessageType).IsEqualTo(typeof(TestMessage));
        await Assert.That(registration.ProcessorType).IsEqualTo(typeof(TestProcessor));
        await Assert.That(registration.ResponseMessageType).IsNull();
        await Assert.That(registration.IsBatchProcessor).IsFalse();
    }

    [Test]
    public async Task AddProcessor_WithTypeInference_ForRequestReply_RegistersResponseType()
    {
        // Arrange
        var configurator = new BussigRegistrationConfigurator();

        // Act
        configurator.AddProcessor<RequestReplyProcessor>();

        // Assert
        await Assert.That(configurator.ProcessorRegistrations.Count).IsEqualTo(1);
        var registration = configurator.ProcessorRegistrations[0];
        await Assert.That(registration.MessageType).IsEqualTo(typeof(TestMessage));
        await Assert.That(registration.ProcessorType).IsEqualTo(typeof(RequestReplyProcessor));
        await Assert.That(registration.ResponseMessageType).IsEqualTo(typeof(TestResponse));
        await Assert.That(registration.IsBatchProcessor).IsFalse();
    }

    [Test]
    public async Task AddProcessor_WithTypeInference_ForBatchProcessor_SetsBatchFlags()
    {
        // Arrange
        var configurator = new BussigRegistrationConfigurator();

        // Act
        configurator.AddProcessor<BatchProcessor>();

        // Assert
        await Assert.That(configurator.ProcessorRegistrations.Count).IsEqualTo(1);
        var registration = configurator.ProcessorRegistrations[0];
        await Assert.That(registration.IsBatchProcessor).IsTrue();
        await Assert.That(registration.BatchMessageType).IsEqualTo(typeof(TestMessage));
        // Queue name should be based on inner message type URN, not Batch<T>
        await Assert.That(registration.QueueName).Contains("test-message");
    }

    [Test]
    public async Task AddProcessor_NonGenericOverload_InfersMessageType()
    {
        // Arrange
        var configurator = new BussigRegistrationConfigurator();

        // Act - explicitly testing non-generic overload
#pragma warning disable CA2263
        configurator.AddProcessor(typeof(TestProcessor));
#pragma warning restore CA2263

        // Assert
        await Assert.That(configurator.ProcessorRegistrations.Count).IsEqualTo(1);
        var registration = configurator.ProcessorRegistrations[0];
        await Assert.That(registration.MessageType).IsEqualTo(typeof(TestMessage));
    }

    [Test]
    public async Task AddProcessor_NonProcessorType_ThrowsArgumentException()
    {
        // Arrange
        var configurator = new BussigRegistrationConfigurator();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() =>
            Task.Run(() => configurator.AddProcessor(typeof(NotAProcessor)))
        );
    }

    [Test]
    public async Task AddProcessorsFromAssembly_RegistersAllProcessors()
    {
        // Arrange
        var configurator = new BussigRegistrationConfigurator();

        // Act
        configurator.AddProcessorsFromAssembly(typeof(TestProcessor).Assembly);

        // Assert - should find TestProcessor, RequestReplyProcessor, BatchProcessor
        await Assert.That(configurator.ProcessorRegistrations.Count).IsGreaterThanOrEqualTo(3);

        var processorTypes = configurator.ProcessorRegistrations.Select(r => r.ProcessorType);
        await Assert.That(processorTypes).Contains(typeof(TestProcessor));
        await Assert.That(processorTypes).Contains(typeof(RequestReplyProcessor));
        await Assert.That(processorTypes).Contains(typeof(BatchProcessor));
    }

    [Test]
    public async Task AddProcessorsFromAssembly_WithConfigure_AppliesOptionsToAll()
    {
        // Arrange
        var configurator = new BussigRegistrationConfigurator();

        // Act
        configurator.AddProcessorsFromAssembly(
            typeof(TestProcessor).Assembly,
            options => options.Polling.MaxConcurrency = 10
        );

        // Assert
        foreach (var registration in configurator.ProcessorRegistrations)
        {
            await Assert.That(registration.Options.Polling.MaxConcurrency).IsEqualTo(10);
        }
    }

    [Test]
    public async Task AddProcessors_WhenSingletonProcessorInterface_SetsSingletonEnabled()
    {
        // Arrange
        var configurator = new BussigRegistrationConfigurator();

        // Act
        configurator.AddProcessor<SingletonProcessor>();

        // Assert
        var processor = configurator.ProcessorRegistrations[0];
        await Assert
            .That(processor.Options.Polling.SingletonProcessing.EnableSingletonProcessing)
            .IsTrue();
    }

    [Test]
    public async Task AddProcessors_WhenSingletonProcessorOptions_SetsSingletonEnabled()
    {
        // Arrange
        var configurator = new BussigRegistrationConfigurator();

        // Act
        configurator.AddProcessor<SingletonProcessor>(options =>
            options.Polling.SingletonProcessing.EnableSingletonProcessing = true
        );

        // Assert
        var processor = configurator.ProcessorRegistrations[0];
        await Assert
            .That(processor.Options.Polling.SingletonProcessing.EnableSingletonProcessing)
            .IsTrue();
    }

    [Test]
    public async Task AddProcessors_WhenNoSingletonProcessorOptions_SetsSingletonDisabled()
    {
        // Arrange
        var configurator = new BussigRegistrationConfigurator();

        // Act
        configurator.AddProcessor<SingletonProcessor>();

        // Assert
        var processor = configurator.ProcessorRegistrations[0];
        await Assert
            .That(processor.Options.Polling.SingletonProcessing.EnableSingletonProcessing)
            .IsTrue();
    }

    // Test message types
    [MessageMapping("test-message")]
    public record TestMessage : IMessage;

    [MessageMapping("test-response")]
    public record TestResponse : IMessage;

    // Test processors
    public class TestProcessor : IProcessor<TestMessage>
    {
        public Task ProcessAsync(
            ProcessorContext<TestMessage> context,
            CancellationToken cancellationToken = default
        ) => Task.CompletedTask;
    }

    public class RequestReplyProcessor : IProcessor<TestMessage, TestResponse>
    {
        public Task<TestResponse> ProcessAsync(
            ProcessorContext<TestMessage> context,
            CancellationToken cancellationToken = default
        ) => Task.FromResult(new TestResponse());
    }

    public class BatchProcessor : IProcessor<Batch<TestMessage>>
    {
        public Task ProcessAsync(
            ProcessorContext<Batch<TestMessage>> context,
            CancellationToken cancellationToken = default
        ) => Task.CompletedTask;
    }

    public class SingletonProcessor : IProcessor<TestMessage>, ISingletonProcessor
    {
        public Task ProcessAsync(
            ProcessorContext<TestMessage> context,
            CancellationToken cancellationToken = default
        ) => Task.CompletedTask;
    }

    public class NotAProcessor { }
}
