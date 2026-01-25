using Bussig.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SecurityDriven;

namespace Bussig.Processing.Internal;

/// <summary>
/// Processing strategy for batch message processing.
/// </summary>
internal sealed class BatchMessageStrategy : IMessageProcessingStrategy
{
    private readonly ProcessorConfiguration _context;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly PostgresMessageReceiver _receiver;
    private readonly ProcessorContextFactory _contextFactory;
    private readonly MessageLockManager _lockManager;
    private readonly MessageErrorHandler _errorHandler;
    private readonly RetryDelayCalculator _retryDelayCalculator;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _concurrencySemaphore;

    public BatchMessageStrategy(
        ProcessorConfiguration context,
        IServiceScopeFactory scopeFactory,
        PostgresMessageReceiver receiver,
        ProcessorContextFactory contextFactory,
        MessageLockManager lockManager,
        MessageErrorHandler errorHandler,
        RetryDelayCalculator retryDelayCalculator,
        ILogger logger,
        SemaphoreSlim concurrencySemaphore
    )
    {
        _context = context;
        _scopeFactory = scopeFactory;
        _receiver = receiver;
        _contextFactory = contextFactory;
        _lockManager = lockManager;
        _errorHandler = errorHandler;
        _retryDelayCalculator = retryDelayCalculator;
        _logger = logger;
        _concurrencySemaphore = concurrencySemaphore;
    }

    public async Task PollAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Starting batch consumer for queue {QueueName} with processor {ProcessorType}",
            _context.QueueName,
            _context.ProcessorType.Name
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
                    _context.QueueName
                );
                await Task.Delay(_context.Options.Polling.Interval, stoppingToken);
            }
        }

        _logger.LogInformation("Batch consumer stopped for queue {QueueName}", _context.QueueName);
    }

    private async Task<List<IncomingMessage>> CollectBatchAsync(CancellationToken stoppingToken)
    {
        var messages = new List<IncomingMessage>();
        var batchStartTime = DateTime.UtcNow;
        var lockId = FastGuid.NewPostgreSqlGuid();

        while (
            messages.Count < _context.Options.Batch.MessageLimit
            && !stoppingToken.IsCancellationRequested
        )
        {
            var remaining = (int)(_context.Options.Batch.MessageLimit - messages.Count);
            var fetchCount = Math.Min(remaining, _context.Options.Polling.PrefetchCount);

            var fetched = await _receiver.ReceiveAsync(
                _context.QueueName,
                lockId,
                _context.Options.Lock.Duration,
                fetchCount,
                stoppingToken
            );

            messages.AddRange(fetched);

            // Check if time limit exceeded and we have at least one message
            if (
                DateTime.UtcNow - batchStartTime >= _context.Options.Batch.TimeLimit
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
                await Task.Delay(_context.Options.Polling.Interval, stoppingToken);
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
            _context.QueueName
        );

        // Start lock renewal for all messages in the batch
        using var lockRenewalCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var lockRenewalTasks = incomingMessages
            .Select(m =>
                _lockManager.RunLockRenewalAsync(
                    m.MessageDeliveryId,
                    m.LockId,
                    lockRenewalCts.Token
                )
            )
            .ToList();

        try
        {
            await using var scope = _scopeFactory.CreateAsyncScope();

            // Deserialize all messages and create contexts
            var contexts = new List<object>();

            foreach (var incomingMessage in incomingMessages)
            {
                var messageBody = _contextFactory.DeserializeMessage(
                    incomingMessage,
                    _context.BatchMessageType!,
                    _context.QueueName,
                    out var errorMessage
                );

                if (messageBody is null)
                {
                    _logger.LogError(
                        "Failed to deserialize message {MessageId} in batch, sending entire batch to dead letter",
                        incomingMessage.MessageId
                    );

                    // Deadletter all messages in the batch
                    await DeadletterBatchAsync(
                        incomingMessages,
                        errorMessage ?? "DeserializationFailed",
                        messageBody is null && errorMessage?.Contains("null") == true
                            ? "NullMessage"
                            : "DeserializationFailed"
                    );
                    return;
                }

                var context = ProcessorContextFactory.CreateContext(
                    incomingMessage,
                    messageBody,
                    _context.BatchMessageType!
                );
                contexts.Add(context);
            }

            // Create the batch using factory
            var batch = _contextFactory.CreateBatch(contexts, _context.BatchMessageType!);

            // Resolve the processor
            var processor = scope.ServiceProvider.GetRequiredService(_context.ProcessorType);

            // Invoke ProcessAsync
            var processMethod = _context.ProcessorType.GetMethod(nameof(IProcessor<>.ProcessAsync));
            if (processMethod is null)
            {
                throw new InvalidOperationException(
                    $"ProcessAsync method not found on processor {_context.ProcessorType.Name}"
                );
            }

            var task = (Task)processMethod.Invoke(processor, [batch, stoppingToken])!;
            await task;

            // Complete all messages
            foreach (var message in incomingMessages)
            {
                await _receiver.CompleteAsync(
                    message.MessageDeliveryId,
                    message.LockId,
                    CancellationToken.None
                );
            }

            _logger.LogDebug(
                "Successfully processed batch of {Count} messages from queue {QueueName}",
                incomingMessages.Count,
                _context.QueueName
            );
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogWarning(
                "Batch processing cancelled for {Count} messages, abandoning",
                incomingMessages.Count
            );

            await AbandonBatchAsync(incomingMessages, TimeSpan.Zero);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Error processing batch of {Count} messages from queue {QueueName}",
                incomingMessages.Count,
                _context.QueueName
            );

            // Check if any message has exceeded max delivery count
            var exceededMax = incomingMessages.Any(m => m.DeliveryCount >= m.MaxDeliveryCount);
            if (exceededMax)
            {
                _logger.LogWarning(
                    "Batch contains message(s) exceeding max delivery count, sending to dead letter"
                );
                await DeadletterBatchAsync(incomingMessages, ex.Message, "MaxRetriesExceeded");
            }
            else
            {
                // Use the message with the highest delivery count as representative for retry calculation
                var representativeMessage = incomingMessages.MaxBy(m => m.DeliveryCount)!;
                await AbandonBatchAsync(
                    incomingMessages,
                    _retryDelayCalculator.CalculateDelay(representativeMessage)
                );
            }
        }
        finally
        {
            await lockRenewalCts.CancelAsync();
            await Task.WhenAll(lockRenewalTasks);
        }
    }

    private async Task DeadletterBatchAsync(
        List<IncomingMessage> messages,
        string errorMessage,
        string errorCode
    )
    {
        foreach (var message in messages)
        {
            await _errorHandler.DeadletterAsync(
                message,
                _context.QueueName,
                errorMessage,
                errorCode,
                CancellationToken.None
            );
        }
    }

    private async Task AbandonBatchAsync(List<IncomingMessage> messages, TimeSpan delay)
    {
        foreach (var message in messages)
        {
            await _errorHandler.AbandonAsync(
                message,
                "Batch processing failed",
                "BatchProcessingFailed",
                delay,
                CancellationToken.None
            );
        }
    }
}
