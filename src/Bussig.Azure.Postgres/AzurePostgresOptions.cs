using Azure.Core;
using Azure.Identity;

namespace Bussig.Azure.Postgres;

public class AzurePostgresOptions
{
    public TokenCredential TokenCredential { get; set; } = new DefaultAzureCredential();
    public TimeSpan SuccessRefreshInterval { get; set; } = TimeSpan.FromMinutes(55);
    public TimeSpan FailureRefreshInterval { get; set; } = TimeSpan.FromSeconds(2);

    public TimeSpan RefreshTimeoutInterval { get; set; } = TimeSpan.FromSeconds(10);
}
