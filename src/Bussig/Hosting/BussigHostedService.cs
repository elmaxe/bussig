using Bussig.Abstractions;
using Bussig.Processing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Bussig.Hosting;

public sealed class BussigHostedService : IHostedService, IAsyncDisposable
{
    private readonly IReadOnlyList<ProcessorRegistration> _registrations;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly PostgresMessageReceiver _receiver;
    private readonly IMessageSerializer _serializer;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<BussigHostedService> _logger;
    private readonly List<QueueConsumer> _consumers = [];

    public BussigHostedService(
        BussigRegistrationConfigurator configurator,
        IServiceScopeFactory scopeFactory,
        PostgresMessageReceiver receiver,
        IMessageSerializer serializer,
        ILoggerFactory loggerFactory,
        ILogger<BussigHostedService> logger
    )
    {
        _registrations = configurator.ProcessorRegistrations;
        _scopeFactory = scopeFactory;
        _receiver = receiver;
        _serializer = serializer;
        _loggerFactory = loggerFactory;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Bussig starting with {ProcessorCount} processor(s)...",
            _registrations.Count
        );

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
                registration.Options,
                _scopeFactory,
                _receiver,
                _serializer,
                _loggerFactory.CreateLogger<QueueConsumer>()
            );

            consumer.Start();
            _consumers.Add(consumer);
        }

        _logger.LogInformation("Bussig started successfully");
        return Task.CompletedTask;
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
