using Bussig.Abstractions;
using Bussig.Abstractions.Middleware;
using Bussig.Processing.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SecurityDriven;

namespace Bussig.Processing.Internal;

/// <summary>
/// Processing strategy for batch message processing.
/// Uses a unified middleware pipeline with batch semantics.
/// </summary>
internal sealed class BatchMessageStrategy : IMessageProcessingStrategy
{
    private readonly ProcessorConfiguration _config;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly PostgresMessageReceiver _receiver;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _concurrencySemaphore;

    public BatchMessageStrategy(
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
            "Starting batch consumer for queue {QueueName} with processor {ProcessorType}",
            _config.QueueName,
            _config.ProcessorType.Name
        );

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _concurrencySemaphore.WaitAsync(stoppingToken);

                try
                {
                    var batch = await CollectBatchAsync(stoppingToken);
                    if (batch.Count > 0)
                    {
                        await ProcessBatchAsync(batch, stoppingToken);
                    }
                }
                finally
                {
                    _concurrencySemaphore.Release();
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
                    "Error in batch polling for queue {QueueName}, will retry after interval",
                    _config.QueueName
                );
                await Task.Delay(_config.Options.Polling.Interval, stoppingToken);
            }
        }

        _logger.LogInformation("Batch consumer stopped for queue {QueueName}", _config.QueueName);
    }

    private async Task<List<IncomingMessage>> CollectBatchAsync(CancellationToken stoppingToken)
    {
        var messages = new List<IncomingMessage>();
        var batchStartTime = DateTime.UtcNow;
        var lockId = FastGuid.NewPostgreSqlGuid();

        while (
            messages.Count < _config.Options.Batch.MessageLimit
            && !stoppingToken.IsCancellationRequested
        )
        {
            var remaining = (int)(_config.Options.Batch.MessageLimit - messages.Count);
            var fetchCount = Math.Min(remaining, _config.Options.Polling.PrefetchCount);

            var fetched = await _receiver.ReceiveAsync(
                _config.QueueName,
                lockId,
                _config.Options.Lock.Duration,
                fetchCount,
                stoppingToken
            );

            messages.AddRange(fetched);

            // Check if time limit exceeded and we have at least one message
            if (
                DateTime.UtcNow - batchStartTime >= _config.Options.Batch.TimeLimit
                && messages.Count > 0
            )
            {
                break;
            }

            // If no messages fetched and we have some, process what we have
            if (fetched.Count == 0)
            {
                if (messages.Count > 0)
                {
                    break;
                }

                // No messages at all, wait before next poll
                await Task.Delay(_config.Options.Polling.Interval, stoppingToken);
            }
        }

        return messages;
    }

    private async Task ProcessBatchAsync(
        List<IncomingMessage> incomingMessages,
        CancellationToken stoppingToken
    )
    {
        _logger.LogDebug(
            "Processing batch of {Count} messages from queue {QueueName}",
            incomingMessages.Count,
            _config.QueueName
        );

        await using var scope = _scopeFactory.CreateAsyncScope();

        // Build the unified message context with the batch
        var context = new MessageContext
        {
            Messages = incomingMessages,
            QueueName = _config.QueueName,
            ProcessorType = _config.ProcessorType,
            MessageType = _config.BatchMessageType!, // The inner message type
            ResponseMessageType = _config.ResponseMessageType,
            Options = _config.Options,
            ServiceProvider = scope.ServiceProvider,
            CancellationToken = stoppingToken,
            IsBatchProcessor = true,
            CompleteAllAsync = () => CompleteAllMessagesAsync(incomingMessages),
            AbandonAllAsync = delay => AbandonAllMessagesAsync(incomingMessages, delay),
        };

        // Create and execute the unified middleware pipeline
        var pipeline = MessageMiddlewarePipeline.CreateDefault(
            _config.GlobalMiddleware,
            _config.Options.Middleware.MiddlewareTypes,
            scope.ServiceProvider
        );

        await pipeline.ExecuteAsync(context);
    }

    private async Task CompleteAllMessagesAsync(IReadOnlyList<IncomingMessage> messages)
    {
        var deliveryIds = messages.Select(m => m.MessageDeliveryId).ToArray();
        var lockIds = messages.Select(m => m.LockId).ToArray();

        await _receiver.CompleteAsync(deliveryIds, lockIds, CancellationToken.None);
    }

    private async Task AbandonAllMessagesAsync(
        IReadOnlyList<IncomingMessage> messages,
        TimeSpan delay
    )
    {
        var retryCalculator = new RetryDelayCalculator(_config.Options.Retry);
        var errorHandler = new MessageErrorHandler(_receiver, retryCalculator, _logger);

        await errorHandler.AbandonAsync(
            messages,
            "Batch processing failed",
            "BatchProcessingFailed",
            delay,
            CancellationToken.None
        );
    }
}
