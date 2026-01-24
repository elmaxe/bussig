using Bussig.Abstractions;

namespace Bussig.Tests.Unit;

public class ConsumerOptionsTests
{
    [Test]
    public async Task Default_EnableLockRenewal_IsTrue()
    {
        // Arrange & Act
        var options = new ConsumerOptions();

        // Assert
        await Assert.That(options.EnableLockRenewal).IsTrue();
    }

    [Test]
    public async Task Default_MaxLockRenewalCount_IsNull()
    {
        // Arrange & Act
        var options = new ConsumerOptions();

        // Assert
        await Assert.That(options.MaxLockRenewalCount).IsNull();
    }

    [Test]
    public async Task Default_BatchTimeLimit_IsFiveSeconds()
    {
        // Arrange & Act
        var options = new ConsumerOptions();

        // Assert
        await Assert.That(options.BatchTimeLimit).IsEqualTo(TimeSpan.FromSeconds(5));
    }

    [Test]
    public async Task Default_BatchMessageLimit_Is100()
    {
        // Arrange & Act
        var options = new ConsumerOptions();

        // Assert
        await Assert.That(options.BatchMessageLimit).IsEqualTo(100u);
    }

    [Test]
    public async Task LockRenewalOptions_CanBeConfigured()
    {
        // Arrange & Act
        var options = new ConsumerOptions { EnableLockRenewal = false, MaxLockRenewalCount = 5 };

        // Assert
        await Assert.That(options.EnableLockRenewal).IsFalse();
        await Assert.That(options.MaxLockRenewalCount).IsEqualTo(5);
    }

    [Test]
    public async Task BatchOptions_CanBeConfigured()
    {
        // Arrange & Act
        var options = new ConsumerOptions
        {
            BatchTimeLimit = TimeSpan.FromSeconds(10),
            BatchMessageLimit = 50,
        };

        // Assert
        await Assert.That(options.BatchTimeLimit).IsEqualTo(TimeSpan.FromSeconds(10));
        await Assert.That(options.BatchMessageLimit).IsEqualTo(50u);
    }

    [Test]
    public async Task Default_LockDuration_IsFiveMinutes()
    {
        // Arrange & Act
        var options = new ConsumerOptions();

        // Assert
        await Assert.That(options.LockDuration).IsEqualTo(TimeSpan.FromMinutes(5));
    }

    [Test]
    public async Task Default_LockRenewalInterval_IsTwoMinutes()
    {
        // Arrange & Act
        var options = new ConsumerOptions();

        // Assert
        await Assert.That(options.LockRenewalInterval).IsEqualTo(TimeSpan.FromMinutes(2));
    }
}
