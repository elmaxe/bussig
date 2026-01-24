using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Bussig.Hosting;

public sealed class BussigHostedService(
    // MessageProcessorsLocator locator,
    ILogger<BussigHostedService> logger
) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Bussig starting...");
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

public class MessageProcessorsLocator
{
    // public IEnumerable<string> NameMe() { }
}
