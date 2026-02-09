using Microsoft.Extensions.Hosting;

namespace Bussig.Outbox.Npgsql;

internal sealed class NpgsqlOutboxHostedService(NpgsqlOutboxMigrator migrator) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await migrator.MigrateAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
