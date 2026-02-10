namespace Bussig.EntityFrameworkCore.Tests;

public class OutboxOptionsTests
{
    [Test]
    public async Task Default_PollingInterval_IsOneSecond()
    {
        var options = new OutboxOptions();

        await Assert.That(options.PollingInterval).IsEqualTo(TimeSpan.FromSeconds(1));
    }

    [Test]
    public async Task Default_BatchSize_Is100()
    {
        var options = new OutboxOptions();

        await Assert.That(options.BatchSize).IsEqualTo(100);
    }

    [Test]
    public async Task Default_PublishedRetention_Is24Hours()
    {
        var options = new OutboxOptions();

        await Assert.That(options.PublishedRetention).IsEqualTo(TimeSpan.FromHours(24));
    }
}
