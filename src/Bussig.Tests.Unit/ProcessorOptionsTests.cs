using Bussig.Abstractions.Options;

namespace Bussig.Tests.Unit;

public class ProcessorOptionsTests
{
    [Test]
    public async Task Default_Lock_EnableRenewal_IsTrue()
    {
        // Arrange & Act
        var options = new ProcessorOptions();

        // Assert
        await Assert.That(options.Lock.EnableRenewal).IsTrue();
    }

    [Test]
    public async Task Default_Lock_MaxRenewalCount_IsNull()
    {
        // Arrange & Act
        var options = new ProcessorOptions();

        // Assert
        await Assert.That(options.Lock.MaxRenewalCount).IsNull();
    }

    [Test]
    public async Task Default_Batch_TimeLimit_IsFiveSeconds()
    {
        // Arrange & Act
        var options = new ProcessorOptions();

        // Assert
        await Assert.That(options.Batch.TimeLimit).IsEqualTo(TimeSpan.FromSeconds(5));
    }

    [Test]
    public async Task Default_Batch_MessageLimit_Is100()
    {
        // Arrange & Act
        var options = new ProcessorOptions();

        // Assert
        await Assert.That(options.Batch.MessageLimit).IsEqualTo(100u);
    }

    [Test]
    public async Task LockOptions_CanBeConfigured()
    {
        // Arrange & Act
        var options = new ProcessorOptions();
        options.Lock.EnableRenewal = false;
        options.Lock.MaxRenewalCount = 5;

        // Assert
        await Assert.That(options.Lock.EnableRenewal).IsFalse();
        await Assert.That(options.Lock.MaxRenewalCount).IsEqualTo(5);
    }

    [Test]
    public async Task BatchOptions_CanBeConfigured()
    {
        // Arrange & Act
        var options = new ProcessorOptions();
        options.Batch.TimeLimit = TimeSpan.FromSeconds(10);
        options.Batch.MessageLimit = 50;

        // Assert
        await Assert.That(options.Batch.TimeLimit).IsEqualTo(TimeSpan.FromSeconds(10));
        await Assert.That(options.Batch.MessageLimit).IsEqualTo(50u);
    }

    [Test]
    public async Task Default_Lock_Duration_IsFiveMinutes()
    {
        // Arrange & Act
        var options = new ProcessorOptions();

        // Assert
        await Assert.That(options.Lock.Duration).IsEqualTo(TimeSpan.FromMinutes(5));
    }

    [Test]
    public async Task Default_Lock_RenewalInterval_IsTwoMinutes()
    {
        // Arrange & Act
        var options = new ProcessorOptions();

        // Assert
        await Assert.That(options.Lock.RenewalInterval).IsEqualTo(TimeSpan.FromMinutes(2));
    }

    [Test]
    public async Task Default_Polling_Interval_Is500Milliseconds()
    {
        // Arrange & Act
        var options = new ProcessorOptions();

        // Assert
        await Assert.That(options.Polling.Interval).IsEqualTo(TimeSpan.FromMilliseconds(500));
    }

    [Test]
    public async Task Default_Polling_PrefetchCount_Is10()
    {
        // Arrange & Act
        var options = new ProcessorOptions();

        // Assert
        await Assert.That(options.Polling.PrefetchCount).IsEqualTo(10);
    }

    [Test]
    public async Task Default_Polling_MaxConcurrency_Is5()
    {
        // Arrange & Act
        var options = new ProcessorOptions();

        // Assert
        await Assert.That(options.Polling.MaxConcurrency).IsEqualTo(5);
    }

    [Test]
    public async Task PollingOptions_CanBeConfigured()
    {
        // Arrange & Act
        var options = new ProcessorOptions();
        options.Polling.Interval = TimeSpan.FromSeconds(1);
        options.Polling.PrefetchCount = 20;
        options.Polling.MaxConcurrency = 10;

        // Assert
        await Assert.That(options.Polling.Interval).IsEqualTo(TimeSpan.FromSeconds(1));
        await Assert.That(options.Polling.PrefetchCount).IsEqualTo(20);
        await Assert.That(options.Polling.MaxConcurrency).IsEqualTo(10);
    }
}
