using Bussig.Abstractions;
using Bussig.Postgres;
using Microsoft.Extensions.Logging;

namespace Bussig;

public sealed class BussigStartup(
    PostgresMigrator migrator,
    IPostgresSettings settings,
    PostgresQueueSender queueSender,
    IBussigRegistrationConfigurator configurator,
    ILogger<BussigStartup> logger
)
{
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _initialized;

    public async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized)
        {
            return;
        }

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_initialized)
            {
                return;
            }

            var options = new TransportOptions { SchemaName = settings.Schema };
            await migrator.CreateSchema(options, cancellationToken);
            await migrator.CreateInfrastructure(options, cancellationToken);

            if (configurator.CreateQueuesOnStartup)
            {
                logger.LogInformation(
                    "CreateQueuesOnStartup = true, making sure queues are created..."
                );

                foreach (var messageType in configurator.Messages)
                {
                    var queueName = MessageUrn.ForType(messageType).ToString();
                    var maxDeliveryCount = configurator.TryGetQueueOptions(
                        messageType,
                        out var queueOptions
                    )
                        ? queueOptions.MaxDeliveryCount
                        : null;
                    await queueSender.CreateQueue(
                        new QueueDefinition(queueName, maxDeliveryCount),
                        cancellationToken
                    );
                    logger.LogInformation("Bussig queue initialized: {QueueName}", queueName);
                }

                logger.LogInformation("Done creating queues!");
            }

            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    private sealed class QueueDefinition(string name, int? maxDeliveryCount) : IQueue
    {
        public string Name { get; } = name;
        public int? MaxDeliveryCount { get; } = maxDeliveryCount;
    }
}
