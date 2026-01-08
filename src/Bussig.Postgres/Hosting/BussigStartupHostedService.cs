using Microsoft.Extensions.Hosting;

namespace Bussig;

public sealed class BussigStartupHostedService(BussigStartup startup) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) =>
        startup.EnsureInitializedAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
