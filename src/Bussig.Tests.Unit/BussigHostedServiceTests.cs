using Bussig.Abstractions;
using Bussig.Hosting;
using Microsoft.Extensions.Logging;
using Moq;

namespace Bussig.Tests.Unit;

public class BussigHostedServiceTests
{
    private readonly Mock<IBusObserver> _observerMock;
    private readonly Mock<ILogger<BussigHostedService>> _loggerMock;
    private readonly BussigRegistrationConfigurator _configurator;

    public BussigHostedServiceTests()
    {
        _observerMock = new Mock<IBusObserver>();
        _observerMock.Setup(o => o.PreStartAsync()).Returns(Task.CompletedTask);
        _observerMock.Setup(o => o.PostStartAsync()).Returns(Task.CompletedTask);
        _observerMock.Setup(o => o.PreStopAsync()).Returns(Task.CompletedTask);
        _observerMock.Setup(o => o.PostStopAsync()).Returns(Task.CompletedTask);
        _loggerMock = new Mock<ILogger<BussigHostedService>>();
        _configurator = new BussigRegistrationConfigurator();
    }

    private BussigHostedService CreateSut(IEnumerable<IBusObserver>? observers = null) =>
        new(
            _configurator,
            null!, // PostgresQueueCreator — not called with empty registrations
            null!, // QueueConsumerFactory — not called with empty registrations
            observers ?? [_observerMock.Object],
            _loggerMock.Object
        );

    [Test]
    public async Task StartAsync_WithObserver_CallsPreStartAsync()
    {
        // Arrange
        var sut = CreateSut();

        // Act
        await sut.StartAsync(CancellationToken.None);

        // Assert
        _observerMock.Verify(o => o.PreStartAsync(), Times.Once);
    }

    [Test]
    public async Task StartAsync_WithObserver_CallsPostStartAsync()
    {
        // Arrange
        var sut = CreateSut();

        // Act
        await sut.StartAsync(CancellationToken.None);

        // Assert
        _observerMock.Verify(o => o.PostStartAsync(), Times.Once);
    }

    [Test]
    public async Task StartAsync_WithObserver_CallsPreStartBeforePostStart()
    {
        // Arrange
        var callOrder = new List<string>();
        _observerMock.Setup(o => o.PreStartAsync()).Callback(() => callOrder.Add("PreStart")).Returns(Task.CompletedTask);
        _observerMock.Setup(o => o.PostStartAsync()).Callback(() => callOrder.Add("PostStart")).Returns(Task.CompletedTask);
        var sut = CreateSut();

        // Act
        await sut.StartAsync(CancellationToken.None);

        // Assert
        await Assert.That(callOrder.Count).IsEqualTo(2);
        await Assert.That(callOrder[0]).IsEqualTo("PreStart");
        await Assert.That(callOrder[1]).IsEqualTo("PostStart");
    }

    [Test]
    public async Task StopAsync_WithObserver_CallsPreStopAsync()
    {
        // Arrange
        var sut = CreateSut();
        await sut.StartAsync(CancellationToken.None);

        // Act
        await sut.StopAsync(CancellationToken.None);

        // Assert
        _observerMock.Verify(o => o.PreStopAsync(), Times.Once);
    }

    [Test]
    public async Task StopAsync_WithObserver_CallsPostStopAsync()
    {
        // Arrange
        var sut = CreateSut();
        await sut.StartAsync(CancellationToken.None);

        // Act
        await sut.StopAsync(CancellationToken.None);

        // Assert
        _observerMock.Verify(o => o.PostStopAsync(), Times.Once);
    }

    [Test]
    public async Task StopAsync_WithObserver_CallsPreStopBeforePostStop()
    {
        // Arrange
        var callOrder = new List<string>();
        _observerMock.Setup(o => o.PreStopAsync()).Callback(() => callOrder.Add("PreStop")).Returns(Task.CompletedTask);
        _observerMock.Setup(o => o.PostStopAsync()).Callback(() => callOrder.Add("PostStop")).Returns(Task.CompletedTask);
        var sut = CreateSut();
        await sut.StartAsync(CancellationToken.None);

        // Act
        await sut.StopAsync(CancellationToken.None);

        // Assert
        await Assert.That(callOrder.Count).IsEqualTo(2);
        await Assert.That(callOrder[0]).IsEqualTo("PreStop");
        await Assert.That(callOrder[1]).IsEqualTo("PostStop");
    }

    [Test]
    public async Task StartAsync_WithMultipleObservers_CallsAllObservers()
    {
        // Arrange
        var observer1 = new Mock<IBusObserver>();
        observer1.Setup(o => o.PreStartAsync()).Returns(Task.CompletedTask);
        observer1.Setup(o => o.PostStartAsync()).Returns(Task.CompletedTask);
        var observer2 = new Mock<IBusObserver>();
        observer2.Setup(o => o.PreStartAsync()).Returns(Task.CompletedTask);
        observer2.Setup(o => o.PostStartAsync()).Returns(Task.CompletedTask);
        var sut = CreateSut([observer1.Object, observer2.Object]);

        // Act
        await sut.StartAsync(CancellationToken.None);

        // Assert
        observer1.Verify(o => o.PreStartAsync(), Times.Once);
        observer1.Verify(o => o.PostStartAsync(), Times.Once);
        observer2.Verify(o => o.PreStartAsync(), Times.Once);
        observer2.Verify(o => o.PostStartAsync(), Times.Once);
    }

    [Test]
    public async Task StartAsync_WithNoObservers_DoesNotThrow()
    {
        // Arrange
        var sut = CreateSut([]);

        // Act & Assert
        await Assert.That(async () => await sut.StartAsync(CancellationToken.None)).ThrowsNothing();
    }
}
