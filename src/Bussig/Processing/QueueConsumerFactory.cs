using Bussig.Processing.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Bussig.Processing;

/// <summary>
/// Factory for creating QueueConsumer instances with the appropriate processing strategy.
/// </summary>
public sealed class QueueConsumerFactory
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly PostgresMessageReceiver _receiver;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IReadOnlyList<Type> _globalMiddleware;

    public QueueConsumerFactory(
        IServiceScopeFactory scopeFactory,
        PostgresMessageReceiver receiver,
        ILoggerFactory loggerFactory,
        IReadOnlyList<Type> globalMiddleware
    )
    {
        _scopeFactory = scopeFactory;
        _receiver = receiver;
        _loggerFactory = loggerFactory;
        _globalMiddleware = globalMiddleware;
    }

    public QueueConsumer Create(ProcessorRegistration registration)
    {
        var logger = _loggerFactory.CreateLogger<QueueConsumer>();

        var config = new ProcessorConfiguration
        {
            QueueName = registration.QueueName,
            MessageType = registration.MessageType,
            ProcessorType = registration.ProcessorType,
            ResponseMessageType = registration.ResponseMessageType,
            BatchMessageType = registration.BatchMessageType,
            Options = registration.Options,
            GlobalMiddleware = _globalMiddleware,
        };

        var concurrencySemaphore = new SemaphoreSlim(
            registration.Options.Polling.MaxConcurrency,
            registration.Options.Polling.MaxConcurrency
        );

        IMessageProcessingStrategy strategy;
        if (registration.IsBatchProcessor)
        {
            strategy = new BatchMessageStrategy(
                config,
                _scopeFactory,
                _receiver,
                logger,
                concurrencySemaphore
            );
        }
        else
        {
            strategy = new SingleMessageStrategy(
                config,
                _scopeFactory,
                _receiver,
                logger,
                concurrencySemaphore
            );
        }

        return new QueueConsumer(strategy, registration.QueueName, logger, concurrencySemaphore);
    }
}
