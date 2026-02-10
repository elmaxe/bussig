using Azure.Identity;

namespace Bussig.Azure.Postgres.Tests;

public class AzurePostgresOptionsTests
{
    [Test]
    public async Task Default_TokenCredential_IsDefaultAzureCredential()
    {
        var options = new AzurePostgresOptions();

        await Assert.That(options.TokenCredential).IsTypeOf<DefaultAzureCredential>();
    }

    [Test]
    public async Task Default_SuccessRefreshInterval_Is55Minutes()
    {
        var options = new AzurePostgresOptions();

        await Assert.That(options.SuccessRefreshInterval).IsEqualTo(TimeSpan.FromMinutes(55));
    }

    [Test]
    public async Task Default_FailureRefreshInterval_Is2Seconds()
    {
        var options = new AzurePostgresOptions();

        await Assert.That(options.FailureRefreshInterval).IsEqualTo(TimeSpan.FromSeconds(2));
    }
}
