using Bussig.Abstractions;
using Bussig.Constants;
using Bussig.Processing.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Bussig.Processing;

/// <summary>
/// Factory for creating QueueConsumer instances with the appropriate processing strategy.
/// </summary>
public sealed class QueueConsumerFactory
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly PostgresMessageReceiver _receiver;
    private readonly IMessageSerializer _serializer;
    private readonly NpgsqlDataSource _npgsqlDataSource;
    private readonly IPostgresTransactionAccessor _transactionAccessor;
    private readonly IBus _bus;
    private readonly ILoggerFactory _loggerFactory;

    public QueueConsumerFactory(
        IServiceScopeFactory scopeFactory,
        PostgresMessageReceiver receiver,
        IMessageSerializer serializer,
        [FromKeyedServices(ServiceKeys.BussigNpgsql)] NpgsqlDataSource npgsqlDataSource,
        IPostgresTransactionAccessor transactionAccessor,
        IBus bus,
        ILoggerFactory loggerFactory
    )
    {
        _scopeFactory = scopeFactory;
        _receiver = receiver;
        _serializer = serializer;
        _npgsqlDataSource = npgsqlDataSource;
        _transactionAccessor = transactionAccessor;
        _bus = bus;
        _loggerFactory = loggerFactory;
    }

    public QueueConsumer Create(ProcessorRegistration registration)
    {
        var logger = _loggerFactory.CreateLogger<QueueConsumer>();

        var consumerContext = new ProcessorConfiguration
        {
            QueueName = registration.QueueName,
            MessageType = registration.MessageType,
            ProcessorType = registration.ProcessorType,
            ResponseMessageType = registration.ResponseMessageType,
            BatchMessageType = registration.BatchMessageType,
            Options = registration.Options,
        };

        var concurrencySemaphore = new SemaphoreSlim(
            registration.Options.Polling.MaxConcurrency,
            registration.Options.Polling.MaxConcurrency
        );

        var retryDelayCalculator = new RetryDelayCalculator(registration.Options.Retry);
        var lockManager = new MessageLockManager(_receiver, registration.Options.Lock, logger);
        var errorHandler = new MessageErrorHandler(_receiver, retryDelayCalculator, logger);
        var contextFactory = new ProcessorContextFactory(_serializer, logger);

        IMessageProcessingStrategy strategy;
        if (registration.IsBatchProcessor)
        {
            strategy = new BatchMessageStrategy(
                consumerContext,
                _scopeFactory,
                _receiver,
                contextFactory,
                lockManager,
                errorHandler,
                retryDelayCalculator,
                logger,
                concurrencySemaphore
            );
        }
        else
        {
            strategy = new SingleMessageStrategy(
                consumerContext,
                _scopeFactory,
                _receiver,
                contextFactory,
                lockManager,
                errorHandler,
                _npgsqlDataSource,
                _transactionAccessor,
                _bus,
                logger,
                concurrencySemaphore
            );
        }

        return new QueueConsumer(strategy, registration.QueueName, logger, concurrencySemaphore);
    }
}
