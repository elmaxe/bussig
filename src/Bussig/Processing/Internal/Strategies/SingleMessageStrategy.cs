using Bussig.Abstractions;
using Bussig.Abstractions.Middleware;
using Bussig.Processing.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SecurityDriven;

namespace Bussig.Processing.Internal.Strategies;

/// <summary>
/// Processing strategy for single message (non-batch) processing.
/// Uses a unified middleware pipeline with batch of 1 semantics.
/// </summary>
internal sealed class SingleMessageStrategy : IMessageProcessingStrategy
{
    private readonly ProcessorConfiguration _config;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly PostgresMessageReceiver _receiver;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _concurrencySemaphore;

    public SingleMessageStrategy(
        ProcessorConfiguration config,
        IServiceScopeFactory scopeFactory,
        PostgresMessageReceiver receiver,
        ILogger logger,
        SemaphoreSlim concurrencySemaphore
    )
    {
        _config = config;
        _scopeFactory = scopeFactory;
        _receiver = receiver;
        _logger = logger;
        _concurrencySemaphore = concurrencySemaphore;
    }

    public async Task PollAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Starting consumer for queue {QueueName} with processor {ProcessorType}",
            _config.QueueName,
            _config.ProcessorType.Name
        );

        var processingTasks = new List<Task>();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Clean up completed tasks
                processingTasks.RemoveAll(t => t.IsCompleted);

                // Calculate how many messages we can fetch
                var availableSlots = _config.Options.Polling.MaxConcurrency - processingTasks.Count;
                var fetchCount = Math.Min(availableSlots, _config.Options.Polling.PrefetchCount);

                if (fetchCount <= 0)
                {
                    // Wait for a slot to become available
                    await _concurrencySemaphore.WaitAsync(stoppingToken);
                    _concurrencySemaphore.Release();
                    continue;
                }

                var lockId = FastGuid.NewPostgreSqlGuid();
                var messages = await _receiver.ReceiveAsync(
                    _config.QueueName,
                    lockId,
                    _config.Options.Lock.Duration,
                    fetchCount,
                    stoppingToken
                );

                if (messages.Count == 0)
                {
                    await Task.Delay(_config.Options.Polling.Interval, stoppingToken);
                    continue;
                }

                foreach (var message in messages)
                {
                    await _concurrencySemaphore.WaitAsync(stoppingToken);

                    var processingTask = ProcessMessageAsync(message, stoppingToken);
                    processingTasks.Add(processingTask);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Error polling queue {QueueName}, will retry after interval",
                    _config.QueueName
                );
                await Task.Delay(_config.Options.Polling.Interval, stoppingToken);
            }
        }

        // Wait for all in-flight messages to complete
        if (processingTasks.Count > 0)
        {
            _logger.LogInformation(
                "Waiting for {Count} in-flight messages to complete for queue {QueueName}",
                processingTasks.Count,
                _config.QueueName
            );

            await Task.WhenAll(processingTasks);
        }

        _logger.LogInformation("Consumer stopped for queue {QueueName}", _config.QueueName);
    }

    private async Task ProcessMessageAsync(
        IncomingMessage incomingMessage,
        CancellationToken stoppingToken
    )
    {
        try
        {
            await using var scope = _scopeFactory.CreateAsyncScope();

            // Build the unified message context with batch of 1
            var context = new MessageContext
            {
                Messages = [incomingMessage],
                QueueName = _config.QueueName,
                ProcessorType = _config.ProcessorType,
                MessageType = _config.MessageType,
                ResponseMessageType = _config.ResponseMessageType,
                Options = _config.Options,
                ServiceProvider = scope.ServiceProvider,
                CancellationToken = stoppingToken,
                IsBatchProcessor = false,
                CompleteAllAsync = () => CompleteMessageAsync(incomingMessage),
                AbandonAllAsync = (delay, exception, _, _) =>
                    AbandonMessageAsync(incomingMessage, exception, delay),
            };

            // Create and execute the middleware pipeline
            var pipeline = MessageMiddlewarePipeline.CreateDefault(
                _config.GlobalMiddleware,
                _config.Options.Middleware.MiddlewareTypes,
                _config.AttachmentsEnabled,
                scope.ServiceProvider
            );

            await pipeline.ExecuteAsync(context);
        }
        finally
        {
            _concurrencySemaphore.Release();
        }
    }

    private async Task CompleteMessageAsync(IncomingMessage message)
    {
        await _receiver.CompleteAsync(
            [message.MessageDeliveryId],
            [message.LockId],
            CancellationToken.None
        );
    }

    private async Task AbandonMessageAsync(
        IncomingMessage message,
        Exception? exception,
        TimeSpan delay
    )
    {
        var errorHandler = new MessageErrorHandler(_receiver);

        await errorHandler.AbandonAsync(
            message,
            exception,
            "Processing failed",
            "ProcessingFailed",
            delay,
            CancellationToken.None
        );
    }
}
