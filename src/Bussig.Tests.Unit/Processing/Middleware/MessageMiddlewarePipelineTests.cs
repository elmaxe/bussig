using Bussig.Abstractions;
using Bussig.Abstractions.Middleware;
using Bussig.Abstractions.Options;
using Bussig.Processing.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace Bussig.Tests.Unit.Processing.Middleware;

public class MessageMiddlewarePipelineTests
{
    [Test]
    public async Task Execute_CallsMiddlewareInOrder()
    {
        // Arrange
        var callOrder = new List<string>();
        var middleware1 = CreateOrderTrackingMiddleware(callOrder, "First");
        var middleware2 = CreateOrderTrackingMiddleware(callOrder, "Second");
        var middleware3 = CreateOrderTrackingMiddleware(callOrder, "Third");

        var serviceProviderMock = new Mock<IServiceProvider>();
        serviceProviderMock.Setup(p => p.GetService(typeof(FirstMiddleware))).Returns(middleware1);
        serviceProviderMock.Setup(p => p.GetService(typeof(SecondMiddleware))).Returns(middleware2);
        serviceProviderMock.Setup(p => p.GetService(typeof(ThirdMiddleware))).Returns(middleware3);

        var pipeline = new MessageMiddlewarePipeline(
            [typeof(FirstMiddleware), typeof(SecondMiddleware), typeof(ThirdMiddleware)],
            serviceProviderMock.Object
        );

        var context = CreateContext(serviceProviderMock.Object);

        // Act
        await pipeline.ExecuteAsync(context);

        // Assert
        await Assert.That(callOrder).Count().IsEqualTo(6);
        await Assert.That(callOrder[0]).IsEqualTo("First-Before");
        await Assert.That(callOrder[1]).IsEqualTo("Second-Before");
        await Assert.That(callOrder[2]).IsEqualTo("Third-Before");
        await Assert.That(callOrder[3]).IsEqualTo("Third-After");
        await Assert.That(callOrder[4]).IsEqualTo("Second-After");
        await Assert.That(callOrder[5]).IsEqualTo("First-After");
    }

    [Test]
    public async Task Execute_PropagatesContextThroughPipeline()
    {
        // Arrange
        var modifyingMiddleware = new ContextModifyingMiddleware();
        var readingMiddleware = new ContextReadingMiddleware();

        var serviceProviderMock = new Mock<IServiceProvider>();
        serviceProviderMock
            .Setup(p => p.GetService(typeof(ContextModifyingMiddleware)))
            .Returns(modifyingMiddleware);
        serviceProviderMock
            .Setup(p => p.GetService(typeof(ContextReadingMiddleware)))
            .Returns(readingMiddleware);

        var pipeline = new MessageMiddlewarePipeline(
            [typeof(ContextModifyingMiddleware), typeof(ContextReadingMiddleware)],
            serviceProviderMock.Object
        );

        var context = CreateContext(serviceProviderMock.Object);

        // Act
        await pipeline.ExecuteAsync(context);

        // Assert
        await Assert.That(context.Items["modified"]).IsEqualTo(true);
        await Assert.That(context.Items["read"]).IsEqualTo(true);
    }

    [Test]
    public async Task Execute_SetsIsHandled_WhenMiddlewareSetsIt()
    {
        // Arrange
        var shortCircuitMiddleware = new ShortCircuitMiddleware();

        var serviceProviderMock = new Mock<IServiceProvider>();
        serviceProviderMock
            .Setup(p => p.GetService(typeof(ShortCircuitMiddleware)))
            .Returns(shortCircuitMiddleware);

        var pipeline = new MessageMiddlewarePipeline(
            [typeof(ShortCircuitMiddleware)],
            serviceProviderMock.Object
        );

        var context = CreateContext(serviceProviderMock.Object);

        // Act
        await pipeline.ExecuteAsync(context);

        // Assert
        await Assert.That(context.IsHandled).IsTrue();
    }

    [Test]
    public async Task Execute_HandlesEmptyMiddlewareList()
    {
        // Arrange
        var serviceProviderMock = new Mock<IServiceProvider>();
        var pipeline = new MessageMiddlewarePipeline([], serviceProviderMock.Object);
        var context = CreateContext(serviceProviderMock.Object);

        // Act - should not throw
        await pipeline.ExecuteAsync(context);

        // Assert
        await Assert.That(context.IsHandled).IsFalse();
    }

    [Test]
    public async Task Execute_ResolvesMiddlewareFromServiceProvider()
    {
        // Arrange
        var middleware = new DependencyMiddleware("injected-value");

        var serviceProviderMock = new Mock<IServiceProvider>();
        serviceProviderMock
            .Setup(p => p.GetService(typeof(DependencyMiddleware)))
            .Returns(middleware);

        var pipeline = new MessageMiddlewarePipeline(
            [typeof(DependencyMiddleware)],
            serviceProviderMock.Object
        );

        var context = CreateContext(serviceProviderMock.Object);

        // Act
        await pipeline.ExecuteAsync(context);

        // Assert
        await Assert.That(context.Items["dependency-value"]).IsEqualTo("injected-value");
    }

    private static IMessageMiddleware CreateOrderTrackingMiddleware(
        List<string> callOrder,
        string name
    )
    {
        var mock = new Mock<IMessageMiddleware>();
        mock.Setup(m =>
                m.InvokeAsync(It.IsAny<MessageContext>(), It.IsAny<MessageMiddlewareDelegate>())
            )
            .Returns<MessageContext, MessageMiddlewareDelegate>(
                async (ctx, next) =>
                {
                    callOrder.Add($"{name}-Before");
                    await next(ctx);
                    callOrder.Add($"{name}-After");
                }
            );
        return mock.Object;
    }

    private static MessageContext CreateContext(IServiceProvider serviceProvider)
    {
        return new MessageContext
        {
            Messages = [CreateIncomingMessage()],
            QueueName = "test-queue",
            ProcessorType = typeof(object),
            MessageType = typeof(object),
            Options = new ProcessorOptions(),
            ServiceProvider = serviceProvider,
            CancellationToken = CancellationToken.None,
            IsBatchProcessor = false,
            CompleteAllAsync = () => Task.CompletedTask,
            AbandonAllAsync = (_, _, _, _) => Task.CompletedTask,
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

    // Test middleware type markers (for type resolution)
    private sealed class FirstMiddleware : IMessageMiddleware
    {
        public Task InvokeAsync(MessageContext context, MessageMiddlewareDelegate next) =>
            throw new NotImplementedException();
    }

    private sealed class SecondMiddleware : IMessageMiddleware
    {
        public Task InvokeAsync(MessageContext context, MessageMiddlewareDelegate next) =>
            throw new NotImplementedException();
    }

    private sealed class ThirdMiddleware : IMessageMiddleware
    {
        public Task InvokeAsync(MessageContext context, MessageMiddlewareDelegate next) =>
            throw new NotImplementedException();
    }

    private sealed class ShortCircuitMiddleware : IMessageMiddleware
    {
        public async Task InvokeAsync(MessageContext context, MessageMiddlewareDelegate next)
        {
            context.IsHandled = true;
            await next(context);
        }
    }

    private sealed class ContextModifyingMiddleware : IMessageMiddleware
    {
        public async Task InvokeAsync(MessageContext context, MessageMiddlewareDelegate next)
        {
            context.Items["modified"] = true;
            await next(context);
        }
    }

    private sealed class ContextReadingMiddleware : IMessageMiddleware
    {
        public async Task InvokeAsync(MessageContext context, MessageMiddlewareDelegate next)
        {
            if (context.Items.TryGetValue("modified", out var value) && value is true)
            {
                context.Items["read"] = true;
            }
            await next(context);
        }
    }

    private sealed class DependencyMiddleware : IMessageMiddleware
    {
        private readonly string _value;

        public DependencyMiddleware(string value) => _value = value;

        public async Task InvokeAsync(MessageContext context, MessageMiddlewareDelegate next)
        {
            context.Items["dependency-value"] = _value;
            await next(context);
        }
    }
}
