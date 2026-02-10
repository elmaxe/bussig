using Bussig.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Moq;

namespace Bussig.EntityFrameworkCore.Tests.Extensions;

public class BussigOutboxExtensionsTests
{
    [Test]
    public async Task AddBussigOutbox_ThrowsWhenNoSenderRegistered()
    {
        var services = new ServiceCollection();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
        {
            services.AddBussigOutbox<TestDbContext>();
            return Task.CompletedTask;
        });
    }

    [Test]
    public async Task AddBussigOutbox_RegistersOutboxSenderAsDecorator()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IOutgoingMessageSender>(new Mock<IOutgoingMessageSender>().Object);

        services.AddBussigOutbox<TestDbContext>();

        var primarySender = services.FirstOrDefault(d =>
            d.ServiceType == typeof(IOutgoingMessageSender) && !d.IsKeyedService
        );
        await Assert.That(primarySender).IsNotNull();
        await Assert.That(primarySender!.ImplementationType).IsEqualTo(typeof(OutboxSender));
    }

    [Test]
    public async Task AddBussigOutbox_MovesOriginalToKeyedService()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IOutgoingMessageSender>(new Mock<IOutgoingMessageSender>().Object);

        services.AddBussigOutbox<TestDbContext>();

        var keyed = services.FirstOrDefault(d =>
            d.ServiceType == typeof(IOutgoingMessageSender)
            && d.IsKeyedService
            && (string?)d.ServiceKey == OutboxServiceKeys.InnerSender
        );
        await Assert.That(keyed).IsNotNull();
    }

    [Test]
    public async Task AddBussigOutbox_RegistersTransactionContext()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IOutgoingMessageSender>(new Mock<IOutgoingMessageSender>().Object);

        services.AddBussigOutbox<TestDbContext>();

        var descriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(OutboxTransactionContext)
        );
        await Assert.That(descriptor).IsNotNull();
        await Assert.That(descriptor!.Lifetime).IsEqualTo(ServiceLifetime.Singleton);
    }

    [Test]
    public async Task AddBussigOutbox_RegistersHostedService()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IOutgoingMessageSender>(new Mock<IOutgoingMessageSender>().Object);

        services.AddBussigOutbox<TestDbContext>();

        var hostedService = services.FirstOrDefault(d =>
            d.ServiceType == typeof(IHostedService)
            && d.ImplementationType == typeof(OutboxForwarder<TestDbContext>)
        );
        await Assert.That(hostedService).IsNotNull();
    }

    [Test]
    public async Task AddBussigOutbox_ConfigureCallback_AppliesOptions()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IOutgoingMessageSender>(new Mock<IOutgoingMessageSender>().Object);

        services.AddBussigOutbox<TestDbContext>(o =>
        {
            o.BatchSize = 42;
            o.PollingInterval = TimeSpan.FromSeconds(5);
        });

        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<IOptions<OutboxOptions>>().Value;

        await Assert.That(options.BatchSize).IsEqualTo(42);
        await Assert.That(options.PollingInterval).IsEqualTo(TimeSpan.FromSeconds(5));
    }
}
