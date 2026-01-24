using System.Reflection;
using System.Text.Json;
using Bussig.Abstractions;
using Bussig.Abstractions.Messages;
using Bussig.Abstractions.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using SecurityDriven;

namespace Bussig.Processing;

public sealed class QueueConsumer : IAsyncDisposable
{
    private readonly string _queueName;
    private readonly Type _messageType;
    private readonly Type _processorType;
    private readonly Type? _responseMessageType;
    private readonly bool _isBatchProcessor;
    private readonly Type? _batchMessageType;
    private readonly ProcessorOptions _options;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly PostgresMessageReceiver _receiver;
    private readonly IMessageSerializer _serializer;
    private readonly NpgsqlDataSource _npgsqlDataSource;
    private readonly IPostgresTransactionAccessor _transactionAccessor;
    private readonly IBus _bus;
    private readonly ILogger<QueueConsumer> _logger;
    private readonly SemaphoreSlim _concurrencySemaphore;
    private readonly CancellationTokenSource _stoppingCts = new();
    private Task? _pollingTask;

    public QueueConsumer(
        string queueName,
        Type messageType,
        Type processorType,
        Type? responseMessageType,
        bool isBatchProcessor,
        Type? batchMessageType,
        ProcessorOptions options,
        IServiceScopeFactory scopeFactory,
        PostgresMessageReceiver receiver,
        IMessageSerializer serializer,
        NpgsqlDataSource npgsqlDataSource,
        IPostgresTransactionAccessor transactionAccessor,
        IBus bus,
        ILogger<QueueConsumer> logger
    )
    {
        _queueName = queueName;
        _messageType = messageType;
        _processorType = processorType;
        _responseMessageType = responseMessageType;
        _isBatchProcessor = isBatchProcessor;
        _batchMessageType = batchMessageType;
        _options = options;
        _scopeFactory = scopeFactory;
        _receiver = receiver;
        _serializer = serializer;
        _npgsqlDataSource = npgsqlDataSource;
        _transactionAccessor = transactionAccessor;
        _bus = bus;
        _logger = logger;
        _concurrencySemaphore = new SemaphoreSlim(
            options.Polling.MaxConcurrency,
            options.Polling.MaxConcurrency
        );
    }

    public void Start()
    {
        _pollingTask = _isBatchProcessor
            ? PollBatchAsync(_stoppingCts.Token)
            : PollAsync(_stoppingCts.Token);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stoppingCts.CancelAsync();

        if (_pollingTask is not null)
        {
            try
            {
                await _pollingTask.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Expected when stopping
            }
        }
    }

    private async Task PollAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Starting consumer for queue {QueueName} with processor {ProcessorType}",
            _queueName,
            _processorType.Name
        );

        var processingTasks = new List<Task>();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Clean up completed tasks
                processingTasks.RemoveAll(t => t.IsCompleted);

                // Calculate how many messages we can fetch
                var availableSlots = _options.Polling.MaxConcurrency - processingTasks.Count;
                var fetchCount = Math.Min(availableSlots, _options.Polling.PrefetchCount);

                if (fetchCount <= 0)
                {
                    // Wait for a slot to become available
                    await _concurrencySemaphore.WaitAsync(stoppingToken);
                    _concurrencySemaphore.Release();
                    continue;
                }

                var lockId = FastGuid.NewPostgreSqlGuid();
                var messages = await _receiver.ReceiveAsync(
                    _queueName,
                    lockId,
                    _options.Lock.Duration,
                    fetchCount,
                    stoppingToken
                );

                if (messages.Count == 0)
                {
                    await Task.Delay(_options.Polling.Interval, stoppingToken);
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
                    _queueName
                );
                await Task.Delay(_options.Polling.Interval, stoppingToken);
            }
        }

        // Wait for all in-flight messages to complete
        if (processingTasks.Count > 0)
        {
            _logger.LogInformation(
                "Waiting for {Count} in-flight messages to complete for queue {QueueName}",
                processingTasks.Count,
                _queueName
            );

            await Task.WhenAll(processingTasks);
        }

        _logger.LogInformation("Consumer stopped for queue {QueueName}", _queueName);
    }

    private async Task PollBatchAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Starting batch consumer for queue {QueueName} with processor {ProcessorType}",
            _queueName,
            _processorType.Name
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
                    _queueName
                );
                await Task.Delay(_options.Polling.Interval, stoppingToken);
            }
        }

        _logger.LogInformation("Batch consumer stopped for queue {QueueName}", _queueName);
    }

    private async Task<List<IncomingMessage>> CollectBatchAsync(CancellationToken stoppingToken)
    {
        var messages = new List<IncomingMessage>();
        var batchStartTime = DateTime.UtcNow;
        var lockId = FastGuid.NewPostgreSqlGuid();

        while (
            messages.Count < _options.Batch.MessageLimit && !stoppingToken.IsCancellationRequested
        )
        {
            var remaining = (int)(_options.Batch.MessageLimit - messages.Count);
            var fetchCount = Math.Min(remaining, _options.Polling.PrefetchCount);

            var fetched = await _receiver.ReceiveAsync(
                _queueName,
                lockId,
                _options.Lock.Duration,
                fetchCount,
                stoppingToken
            );

            messages.AddRange(fetched);

            // Check if time limit exceeded and we have at least one message
            if (DateTime.UtcNow - batchStartTime >= _options.Batch.TimeLimit && messages.Count > 0)
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
                await Task.Delay(_options.Polling.Interval, stoppingToken);
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
            _queueName
        );

        // Start lock renewal for all messages in the batch
        using var lockRenewalCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var lockRenewalTasks = incomingMessages
            .Select(m => RunLockRenewalAsync(m.MessageDeliveryId, m.LockId, lockRenewalCts.Token))
            .ToList();

        try
        {
            await using var scope = _scopeFactory.CreateAsyncScope();

            // Deserialize all messages and create contexts
            var contexts = new List<object>();
            var contextType = typeof(MessageProcessorContext<>).MakeGenericType(_batchMessageType!);

            foreach (var incomingMessage in incomingMessages)
            {
                object? messageBody;
                try
                {
                    messageBody = _serializer.Deserialize(incomingMessage.Body, _batchMessageType!);
                }
                catch (JsonException ex)
                {
                    _logger.LogError(
                        ex,
                        "Failed to deserialize message {MessageId} in batch, sending entire batch to dead letter",
                        incomingMessage.MessageId
                    );

                    // Deadletter all messages in the batch
                    await DeadletterBatchAsync(
                        incomingMessages,
                        ex.Message,
                        "DeserializationFailed"
                    );
                    return;
                }

                if (messageBody is null)
                {
                    _logger.LogError(
                        "Deserialized message {MessageId} is null in batch, sending entire batch to dead letter",
                        incomingMessage.MessageId
                    );

                    await DeadletterBatchAsync(
                        incomingMessages,
                        "Message body deserialized to null",
                        "NullMessage"
                    );
                    return;
                }

                var context = Activator.CreateInstance(
                    contextType,
                    BindingFlags.Instance | BindingFlags.NonPublic,
                    binder: null,
                    args:
                    [
                        messageBody,
                        incomingMessage.MessageId,
                        incomingMessage.DeliveryCount,
                        incomingMessage.MaxDeliveryCount,
                        incomingMessage.EnqueuedAt,
                        incomingMessage.MessageDeliveryId,
                        incomingMessage.LockId,
                    ],
                    culture: null
                );

                if (context is null)
                {
                    throw new InvalidOperationException(
                        $"Failed to create context for message type {_batchMessageType!.Name}"
                    );
                }

                contexts.Add(context);
            }

            // Create the batch using reflection
            var batchType = typeof(MessageBatch<>).MakeGenericType(_batchMessageType!);
            var batch = Activator.CreateInstance(
                batchType,
                BindingFlags.Instance | BindingFlags.Public,
                binder: null,
                args: [contexts.Cast<object>()],
                culture: null
            );

            // Actually, we need to pass the correctly typed contexts
            // Let me create it properly using a generic method
            var createBatchMethod = GetType()
                .GetMethod(nameof(CreateBatch), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(_batchMessageType!);
            batch = createBatchMethod.Invoke(null, [contexts]);

            // Resolve the processor
            var processor = scope.ServiceProvider.GetRequiredService(_processorType);

            // Invoke ProcessAsync
            var processMethod = _processorType.GetMethod(nameof(IProcessor<>.ProcessAsync));
            if (processMethod is null)
            {
                throw new InvalidOperationException(
                    $"ProcessAsync method not found on processor {_processorType.Name}"
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
                _queueName
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
                _queueName
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
                    CalculateRetryDelay(representativeMessage)
                );
            }
        }
        finally
        {
            await lockRenewalCts.CancelAsync();
            await Task.WhenAll(lockRenewalTasks);
        }
    }

    private static MessageBatch<TMessage> CreateBatch<TMessage>(List<object> contexts)
        where TMessage : class, IMessage
    {
        var typedContexts = contexts.Cast<MessageProcessorContext<TMessage>>();
        return new MessageBatch<TMessage>(typedContexts);
    }

    private async Task DeadletterBatchAsync(
        List<IncomingMessage> messages,
        string errorMessage,
        string errorCode
    )
    {
        foreach (var message in messages)
        {
            var headers = BuildErrorHeaders(
                message.MessageDeliveryHeaders,
                errorMessage,
                errorCode
            );
            await _receiver.DeadletterAsync(
                message.MessageDeliveryId,
                message.LockId,
                _queueName,
                headers,
                CancellationToken.None
            );
        }
    }

    private async Task AbandonBatchAsync(List<IncomingMessage> messages, TimeSpan delay)
    {
        foreach (var message in messages)
        {
            var headers = BuildErrorHeaders(
                message.MessageDeliveryHeaders,
                "Batch processing failed",
                "BatchProcessingFailed"
            );
            await _receiver.AbandonAsync(
                message.MessageDeliveryId,
                message.LockId,
                headers,
                delay,
                CancellationToken.None
            );
        }
    }

    private async Task ProcessMessageAsync(
        IncomingMessage incomingMessage,
        CancellationToken stoppingToken
    )
    {
        using var lockRenewalCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var lockRenewalTask = RunLockRenewalAsync(
            incomingMessage.MessageDeliveryId,
            incomingMessage.LockId,
            lockRenewalCts.Token
        );

        try
        {
            await using var scope = _scopeFactory.CreateAsyncScope();

            // Deserialize the message
            object? messageBody;
            try
            {
                messageBody = _serializer.Deserialize(incomingMessage.Body, _messageType);
            }
            catch (JsonException ex)
            {
                _logger.LogError(
                    ex,
                    "Failed to deserialize message {MessageId} for queue {QueueName}, sending to dead letter",
                    incomingMessage.MessageId,
                    _queueName
                );

                var deadletterHeaders = BuildErrorHeaders(
                    incomingMessage.MessageDeliveryHeaders,
                    ex.Message,
                    "DeserializationFailed"
                );
                await _receiver.DeadletterAsync(
                    incomingMessage.MessageDeliveryId,
                    incomingMessage.LockId,
                    _queueName,
                    deadletterHeaders,
                    CancellationToken.None
                );
                return;
            }

            if (messageBody is null)
            {
                _logger.LogError(
                    "Deserialized message {MessageId} is null for queue {QueueName}, sending to dead letter",
                    incomingMessage.MessageId,
                    _queueName
                );

                var deadletterHeaders = BuildErrorHeaders(
                    incomingMessage.MessageDeliveryHeaders,
                    "Message body deserialized to null",
                    "NullMessage"
                );
                await _receiver.DeadletterAsync(
                    incomingMessage.MessageDeliveryId,
                    incomingMessage.LockId,
                    _queueName,
                    deadletterHeaders,
                    CancellationToken.None
                );
                return;
            }

            // Resolve the processor
            var processor = scope.ServiceProvider.GetRequiredService(_processorType);

            // Build context using constructor via reflection
            var contextType = typeof(MessageProcessorContext<>).MakeGenericType(_messageType);
            var context = Activator.CreateInstance(
                contextType,
                BindingFlags.Instance | BindingFlags.NonPublic,
                binder: null,
                args:
                [
                    messageBody,
                    incomingMessage.MessageId,
                    incomingMessage.DeliveryCount,
                    incomingMessage.MaxDeliveryCount,
                    incomingMessage.EnqueuedAt,
                    incomingMessage.MessageDeliveryId,
                    incomingMessage.LockId,
                ],
                culture: null
            );

            if (context is null)
            {
                throw new InvalidOperationException(
                    $"Failed to create context for message type {_messageType.Name}"
                );
            }

            // Invoke ProcessAsync using reflection
            var processMethod = _processorType.GetMethod(nameof(IProcessor<>.ProcessAsync));
            if (processMethod is null)
            {
                throw new InvalidOperationException(
                    $"ProcessAsync method not found on processor {_processorType.Name}"
                );
            }

            if (_responseMessageType is not null)
            {
                // Request-reply processor with transactional outbox
                await ProcessWithOutboxAsync(
                    processor,
                    processMethod,
                    context,
                    incomingMessage,
                    stoppingToken
                );
            }
            else
            {
                // Fire-and-forget processor
                var task = (Task)processMethod.Invoke(processor, [context, stoppingToken])!;
                await task;

                // Complete the message
                await _receiver.CompleteAsync(
                    incomingMessage.MessageDeliveryId,
                    incomingMessage.LockId,
                    CancellationToken.None
                );
            }

            _logger.LogDebug(
                "Successfully processed message {MessageId} from queue {QueueName}",
                incomingMessage.MessageId,
                _queueName
            );
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Abandon the message when shutting down so it can be reprocessed
            _logger.LogWarning(
                "Processing cancelled for message {MessageId}, abandoning",
                incomingMessage.MessageId
            );

            await _receiver.AbandonAsync(
                incomingMessage.MessageDeliveryId,
                incomingMessage.LockId,
                incomingMessage.MessageDeliveryHeaders,
                TimeSpan.Zero,
                CancellationToken.None
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Error processing message {MessageId} from queue {QueueName} (delivery {DeliveryCount}/{MaxDeliveryCount})",
                incomingMessage.MessageId,
                _queueName,
                incomingMessage.DeliveryCount,
                incomingMessage.MaxDeliveryCount
            );

            // Check if we've exceeded max delivery count
            if (incomingMessage.DeliveryCount >= incomingMessage.MaxDeliveryCount)
            {
                _logger.LogWarning(
                    "Message {MessageId} exceeded max delivery count, sending to dead letter queue",
                    incomingMessage.MessageId
                );

                var deadletterHeaders = BuildErrorHeaders(
                    incomingMessage.MessageDeliveryHeaders,
                    ex.Message,
                    "MaxRetriesExceeded"
                );
                await _receiver.DeadletterAsync(
                    incomingMessage.MessageDeliveryId,
                    incomingMessage.LockId,
                    _queueName,
                    deadletterHeaders,
                    CancellationToken.None
                );
            }
            else
            {
                // Abandon with delay for retry
                var errorHeaders = BuildErrorHeaders(
                    incomingMessage.MessageDeliveryHeaders,
                    ex.Message,
                    "ProcessingFailed"
                );
                await _receiver.AbandonAsync(
                    incomingMessage.MessageDeliveryId,
                    incomingMessage.LockId,
                    errorHeaders,
                    CalculateRetryDelay(incomingMessage, ex),
                    CancellationToken.None
                );
            }
        }
        finally
        {
            await lockRenewalCts.CancelAsync();

            try
            {
                await lockRenewalTask;
            }
            catch (OperationCanceledException)
            {
                // Expected
            }

            _concurrencySemaphore.Release();
        }
    }

    private async Task ProcessWithOutboxAsync(
        object processor,
        MethodInfo processMethod,
        object context,
        IncomingMessage incomingMessage,
        CancellationToken stoppingToken
    )
    {
        await using var conn = await _npgsqlDataSource.OpenConnectionAsync(stoppingToken);
        await using var transaction = await conn.BeginTransactionAsync(stoppingToken);

        using var _ = _transactionAccessor.Use(transaction);

        try
        {
            // Invoke ProcessAsync - returns Task<TSend>
            var task = (Task)processMethod.Invoke(processor, [context, stoppingToken])!;
            await task;

            // Get the result using reflection (task is Task<TSend>)
            var resultProperty = task.GetType().GetProperty("Result");
            var responseMessage = resultProperty?.GetValue(task);

            // Send response message within transaction if not null
            if (responseMessage is not null && responseMessage is IMessage)
            {
                // Use reflection to call IBus.SendAsync<TMessage>
                var sendMethod = typeof(IBus)
                    .GetMethods()
                    .First(m =>
                        m.Name == nameof(IBus.SendAsync)
                        && m.GetParameters().Length == 2
                        && m.GetParameters()[1].ParameterType == typeof(CancellationToken)
                    );
                var genericSendMethod = sendMethod.MakeGenericMethod(_responseMessageType!);

                await (Task)genericSendMethod.Invoke(_bus, [responseMessage, stoppingToken])!;

                _logger.LogDebug(
                    "Sent response message of type {ResponseType} for message {MessageId}",
                    _responseMessageType!.Name,
                    incomingMessage.MessageId
                );
            }

            // Complete message within transaction
            await _receiver.CompleteWithinTransactionAsync(
                conn,
                transaction,
                incomingMessage.MessageDeliveryId,
                incomingMessage.LockId,
                stoppingToken
            );

            await transaction.CommitAsync(stoppingToken);
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    private async Task RunLockRenewalAsync(
        long messageDeliveryId,
        Guid lockId,
        CancellationToken cancellationToken
    )
    {
        if (!_options.Lock.EnableRenewal)
        {
            return;
        }

        var renewalCount = 0;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(_options.Lock.RenewalInterval, cancellationToken);

                // Check max renewal count
                if (
                    _options.Lock.MaxRenewalCount.HasValue
                    && renewalCount >= _options.Lock.MaxRenewalCount.Value
                )
                {
                    _logger.LogWarning(
                        "Lock renewal limit ({MaxCount}) reached for message delivery {MessageDeliveryId}",
                        _options.Lock.MaxRenewalCount.Value,
                        messageDeliveryId
                    );
                    break;
                }

                var renewed = await _receiver.RenewLockAsync(
                    messageDeliveryId,
                    lockId,
                    _options.Lock.Duration,
                    cancellationToken
                );

                if (!renewed)
                {
                    _logger.LogWarning(
                        "Failed to renew lock for message delivery {MessageDeliveryId}",
                        messageDeliveryId
                    );
                    break;
                }

                renewalCount++;

                _logger.LogDebug(
                    "Renewed lock for message delivery {MessageDeliveryId} (renewal #{Count})",
                    messageDeliveryId,
                    renewalCount
                );
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Expected when processing completes
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Error renewing lock for message delivery {MessageDeliveryId}",
                messageDeliveryId
            );
        }
    }

    private static string BuildErrorHeaders(
        string? existingHeaders,
        string errorMessage,
        string errorCode
    )
    {
        var headers = string.IsNullOrEmpty(existingHeaders)
            ? new Dictionary<string, object>()
            : JsonSerializer.Deserialize<Dictionary<string, object>>(existingHeaders) ?? [];

        headers["error-message"] = errorMessage;
        headers["error-code"] = errorCode;
        headers["error-timestamp"] = DateTimeOffset.UtcNow.ToString("O");

        return JsonSerializer.Serialize(headers);
    }

    private TimeSpan CalculateRetryDelay(IncomingMessage message, Exception? exception = null)
    {
        return _options.Retry.Strategy switch
        {
            RetryStrategy.Immediate => TimeSpan.Zero,
            RetryStrategy.Fixed => _options.Retry.Delay,
            RetryStrategy.Exponential => CalculateExponentialDelay(message.DeliveryCount),
            RetryStrategy.Custom => _options.Retry.CustomDelayCalculator?.Invoke(
                new RetryContext
                {
                    DeliveryCount = message.DeliveryCount,
                    MaxDeliveryCount = message.MaxDeliveryCount,
                    EnqueuedAt = message.EnqueuedAt,
                    LastDeliveredAt = message.LastDeliveredAt,
                    ExpirationTime = message.ExpirationTime,
                    Exception = exception,
                    BaseDelay = _options.Retry.Delay,
                }
            ) ?? _options.Retry.Delay,
            _ => _options.Retry.Delay,
        };
    }

    private TimeSpan CalculateExponentialDelay(int deliveryCount)
    {
        // 2^(deliveryCount-1) * baseDelay, capped at MaxRetryDelay
        var multiplier = Math.Pow(2, deliveryCount - 1);
        var delay = TimeSpan.FromTicks((long)(_options.Retry.Delay.Ticks * multiplier));
        return delay > _options.Retry.MaxDelay ? _options.Retry.MaxDelay : delay;
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None);
        _stoppingCts.Dispose();
        _concurrencySemaphore.Dispose();
    }
}
