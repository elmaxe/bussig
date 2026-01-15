using Bussig.Abstractions;
using Bussig.Postgres;
using Microsoft.Extensions.Logging;

namespace Bussig;

public sealed class BussigStartup(
    PostgresMigrator migrator,
    IPostgresSettings settings,
    PostgresQueueCreator queueCreator,
    IBussigRegistrationConfigurator configurator,
    ILogger<BussigStartup> logger
)
{
    public bool IsInitialized { get; private set; }

    private static readonly SemaphoreSlim InitLock = new(1, 1);

    public async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (IsInitialized)
        {
            return;
        }

        await InitLock.WaitAsync(cancellationToken);
        try
        {
            if (IsInitialized)
            {
                return;
            }

            var options = new TransportOptions { SchemaName = settings.Schema };
            await migrator.CreateSchema(options, cancellationToken);
            await migrator.CreateInfrastructure(options, cancellationToken);

            if (configurator.CreateQueuesOnStartup)
            {
                await CreateQueues(cancellationToken);
            }

            IsInitialized = true;
        }
        finally
        {
            InitLock.Release();
        }
    }

    private async Task CreateQueues(CancellationToken cancellationToken)
    {
        logger.LogInformation("CreateQueuesOnStartup = true, making sure queues are created...");

        foreach (var messageType in configurator.Messages)
        {
            var queueName = MessageUrn.ForType(messageType).ToString();
            var maxDeliveryCount = configurator.TryGetQueueOptions(
                messageType,
                out var queueOptions
            )
                ? queueOptions.MaxDeliveryCount
                : null;
            await queueCreator.CreateQueue(
                new QueueDefinition(queueName, maxDeliveryCount),
                cancellationToken
            );
            logger.LogInformation("Bussig queue initialized: {QueueName}", queueName);
        }

        logger.LogInformation("Done creating queues!");
    }

    private sealed class QueueDefinition(string name, int? maxDeliveryCount) : IQueue
    {
        public string Name { get; } = name;
        public int? MaxDeliveryCount { get; } = maxDeliveryCount;
    }
}
