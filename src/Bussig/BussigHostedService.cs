using Microsoft.Extensions.Hosting;

namespace Bussig;

public class BussigHostedService() : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            // TODO: Create queues for registered message types
            //
            await Task.Delay(100, cancellationToken);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}
