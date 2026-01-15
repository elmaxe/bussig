using Microsoft.Extensions.Hosting;

namespace Bussig;

public sealed class BussigHostedService(BussigStartup startup) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await startup.EnsureInitializedAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
