using Bussig.Abstractions;
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
    private readonly IEnumerable<IBusObserver> _observers;
    private readonly List<QueueConsumer> _consumers = [];

    public BussigHostedService(
        BussigRegistrationConfigurator configurator,
        PostgresQueueCreator queueCreator,
        QueueConsumerFactory consumerFactory,
        IEnumerable<IBusObserver> observers,
        ILogger<BussigHostedService> logger
    )
    {
        _configurator = configurator;
        _registrations = configurator.ProcessorRegistrations;
        _queueCreator = queueCreator;
        _consumerFactory = consumerFactory;
        _observers = observers;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Bussig starting with {ProcessorCount} processor(s)...",
            _registrations.Count
        );

        foreach (var observer in _observers)
            await observer.PreStartAsync();

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

        foreach (var observer in _observers)
            await observer.PostStartAsync();

        _logger.LogInformation("Bussig started successfully");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Bussig stopping...");

        foreach (var observer in _observers)
            await observer.PreStopAsync();

        var stopTasks = _consumers.Select(c => c.StopAsync(cancellationToken));
        await Task.WhenAll(stopTasks);

        foreach (var observer in _observers)
            await observer.PostStopAsync();

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
