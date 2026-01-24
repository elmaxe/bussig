using System.Reflection;
using System.Text.Json;
using Bussig.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SecurityDriven;

namespace Bussig.Processing;

public sealed class QueueConsumer : IAsyncDisposable
{
    private readonly string _queueName;
    private readonly Type _messageType;
    private readonly Type _processorType;
    private readonly ConsumerOptions _options;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly PostgresMessageReceiver _receiver;
    private readonly IMessageSerializer _serializer;
    private readonly ILogger<QueueConsumer> _logger;
    private readonly SemaphoreSlim _concurrencySemaphore;
    private readonly CancellationTokenSource _stoppingCts = new();
    private Task? _pollingTask;

    public QueueConsumer(
        string queueName,
        Type messageType,
        Type processorType,
        ConsumerOptions options,
        IServiceScopeFactory scopeFactory,
        PostgresMessageReceiver receiver,
        IMessageSerializer serializer,
        ILogger<QueueConsumer> logger
    )
    {
        _queueName = queueName;
        _messageType = messageType;
        _processorType = processorType;
        _options = options;
        _scopeFactory = scopeFactory;
        _receiver = receiver;
        _serializer = serializer;
        _logger = logger;
        _concurrencySemaphore = new SemaphoreSlim(options.MaxConcurrency, options.MaxConcurrency);
    }

    public void Start()
    {
        _pollingTask = PollAsync(_stoppingCts.Token);
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
                var availableSlots = _options.MaxConcurrency - processingTasks.Count;
                var fetchCount = Math.Min(availableSlots, _options.PrefetchCount);

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
                    _options.LockDuration,
                    fetchCount,
                    stoppingToken
                );

                if (messages.Count == 0)
                {
                    await Task.Delay(_options.PollInterval, stoppingToken);
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
                await Task.Delay(_options.PollInterval, stoppingToken);
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

            var task = (Task)processMethod.Invoke(processor, [context, stoppingToken])!;
            await task;

            // Complete the message
            await _receiver.CompleteAsync(
                incomingMessage.MessageDeliveryId,
                incomingMessage.LockId,
                CancellationToken.None
            );

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
                    _options.RetryDelay,
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

    private async Task RunLockRenewalAsync(
        long messageDeliveryId,
        Guid lockId,
        CancellationToken cancellationToken
    )
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(_options.LockRenewalInterval, cancellationToken);

                var renewed = await _receiver.RenewLockAsync(
                    messageDeliveryId,
                    lockId,
                    _options.LockDuration,
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

    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None);
        _stoppingCts.Dispose();
        _concurrencySemaphore.Dispose();
    }
}
