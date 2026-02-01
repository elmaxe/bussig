using Bussig.Processing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Bussig.Hosting;

public sealed class BussigHostedService : IHostedService, IAsyncDisposable
{
    private readonly BussigRegistrationConfigurator _configurator;
    private readonly IReadOnlyList<ProcessorRegistration> _registrations;
    private readonly PostgresQueueCreator _queueCreator;
    private readonly QueueConsumerFactory _consumerFactory;
    private readonly ILogger<BussigHostedService> _logger;
    private readonly List<QueueConsumer> _consumers = [];

    public BussigHostedService(
        BussigRegistrationConfigurator configurator,
        PostgresQueueCreator queueCreator,
        QueueConsumerFactory consumerFactory,
        ILogger<BussigHostedService> logger
    )
    {
        _configurator = configurator;
        _registrations = configurator.ProcessorRegistrations;
        _queueCreator = queueCreator;
        _consumerFactory = consumerFactory;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Bussig starting with {ProcessorCount} processor(s)...",
            _registrations.Count
        );

        // Create queues before starting consumers
        var createdQueues = new HashSet<string>();
        foreach (var registration in _registrations)
        {
            if (createdQueues.Add(registration.QueueName))
            {
                _configurator.TryGetQueueOptions(registration.MessageType, out var queueOptions);
                var queue = new Queue(registration.QueueName, queueOptions?.MaxDeliveryCount);

                await _queueCreator.CreateQueue(queue, cancellationToken);
                _logger.LogInformation(
                    "Created/verified queue {QueueName}",
                    registration.QueueName
                );
            }
        }

        foreach (var registration in _registrations)
        {
            _logger.LogInformation(
                "Starting consumer for queue {QueueName} with processor {ProcessorType}",
                registration.QueueName,
                registration.ProcessorType.Name
            );

            var consumer = _consumerFactory.Create(registration);
            consumer.Start();
            _consumers.Add(consumer);
        }

        _logger.LogInformation("Bussig started successfully");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Bussig stopping...");

        var stopTasks = _consumers.Select(c => c.StopAsync(cancellationToken));
        await Task.WhenAll(stopTasks);

        _logger.LogInformation("Bussig stopped");
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var consumer in _consumers)
        {
            await consumer.DisposeAsync();
        }

        _consumers.Clear();
    }
}
