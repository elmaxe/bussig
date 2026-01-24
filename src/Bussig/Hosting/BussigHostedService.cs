using Bussig.Abstractions;
using Bussig.Constants;
using Bussig.Processing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Bussig.Hosting;

public sealed class BussigHostedService : IHostedService, IAsyncDisposable
{
    private readonly BussigRegistrationConfigurator _configurator;
    private readonly IReadOnlyList<ProcessorRegistration> _registrations;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly PostgresMessageReceiver _receiver;
    private readonly PostgresQueueCreator _queueCreator;
    private readonly IMessageSerializer _serializer;
    private readonly NpgsqlDataSource _npgsqlDataSource;
    private readonly IPostgresTransactionAccessor _transactionAccessor;
    private readonly IBus _bus;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<BussigHostedService> _logger;
    private readonly List<QueueConsumer> _consumers = [];

    public BussigHostedService(
        BussigRegistrationConfigurator configurator,
        IServiceScopeFactory scopeFactory,
        PostgresMessageReceiver receiver,
        PostgresQueueCreator queueCreator,
        IMessageSerializer serializer,
        [FromKeyedServices(ServiceKeys.BussigNpgsql)] NpgsqlDataSource npgsqlDataSource,
        IPostgresTransactionAccessor transactionAccessor,
        IBus bus,
        ILoggerFactory loggerFactory,
        ILogger<BussigHostedService> logger
    )
    {
        _configurator = configurator;
        _registrations = configurator.ProcessorRegistrations;
        _scopeFactory = scopeFactory;
        _receiver = receiver;
        _queueCreator = queueCreator;
        _serializer = serializer;
        _npgsqlDataSource = npgsqlDataSource;
        _transactionAccessor = transactionAccessor;
        _bus = bus;
        _loggerFactory = loggerFactory;
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

            var consumer = new QueueConsumer(
                registration.QueueName,
                registration.MessageType,
                registration.ProcessorType,
                registration.ResponseMessageType,
                registration.IsBatchProcessor,
                registration.BatchMessageType,
                registration.Options,
                _scopeFactory,
                _receiver,
                _serializer,
                _npgsqlDataSource,
                _transactionAccessor,
                _bus,
                _loggerFactory.CreateLogger<QueueConsumer>()
            );

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
