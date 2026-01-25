using System.Reflection;
using Bussig.Abstractions;
using Bussig.Abstractions.Messages;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using SecurityDriven;

namespace Bussig.Processing.Internal;

/// <summary>
/// Processing strategy for single message (non-batch) processing.
/// </summary>
internal sealed class SingleMessageStrategy : IMessageProcessingStrategy
{
    private readonly ProcessorConfiguration _context;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly PostgresMessageReceiver _receiver;
    private readonly ProcessorContextFactory _contextFactory;
    private readonly MessageLockManager _lockManager;
    private readonly MessageErrorHandler _errorHandler;
    private readonly NpgsqlDataSource _npgsqlDataSource;
    private readonly IPostgresTransactionAccessor _transactionAccessor;
    private readonly IBus _bus;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _concurrencySemaphore;

    public SingleMessageStrategy(
        ProcessorConfiguration context,
        IServiceScopeFactory scopeFactory,
        PostgresMessageReceiver receiver,
        ProcessorContextFactory contextFactory,
        MessageLockManager lockManager,
        MessageErrorHandler errorHandler,
        NpgsqlDataSource npgsqlDataSource,
        IPostgresTransactionAccessor transactionAccessor,
        IBus bus,
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
        _npgsqlDataSource = npgsqlDataSource;
        _transactionAccessor = transactionAccessor;
        _bus = bus;
        _logger = logger;
        _concurrencySemaphore = concurrencySemaphore;
    }

    public async Task PollAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Starting consumer for queue {QueueName} with processor {ProcessorType}",
            _context.QueueName,
            _context.ProcessorType.Name
        );

        var processingTasks = new List<Task>();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Clean up completed tasks
                processingTasks.RemoveAll(t => t.IsCompleted);

                // Calculate how many messages we can fetch
                var availableSlots =
                    _context.Options.Polling.MaxConcurrency - processingTasks.Count;
                var fetchCount = Math.Min(availableSlots, _context.Options.Polling.PrefetchCount);

                if (fetchCount <= 0)
                {
                    // Wait for a slot to become available
                    await _concurrencySemaphore.WaitAsync(stoppingToken);
                    _concurrencySemaphore.Release();
                    continue;
                }

                var lockId = FastGuid.NewPostgreSqlGuid();
                var messages = await _receiver.ReceiveAsync(
                    _context.QueueName,
                    lockId,
                    _context.Options.Lock.Duration,
                    fetchCount,
                    stoppingToken
                );

                if (messages.Count == 0)
                {
                    await Task.Delay(_context.Options.Polling.Interval, stoppingToken);
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
                    _context.QueueName
                );
                await Task.Delay(_context.Options.Polling.Interval, stoppingToken);
            }
        }

        // Wait for all in-flight messages to complete
        if (processingTasks.Count > 0)
        {
            _logger.LogInformation(
                "Waiting for {Count} in-flight messages to complete for queue {QueueName}",
                processingTasks.Count,
                _context.QueueName
            );

            await Task.WhenAll(processingTasks);
        }

        _logger.LogInformation("Consumer stopped for queue {QueueName}", _context.QueueName);
    }

    private async Task ProcessMessageAsync(
        IncomingMessage incomingMessage,
        CancellationToken stoppingToken
    )
    {
        using var lockRenewalCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var lockRenewalTask = _lockManager.RunLockRenewalAsync(
            incomingMessage.MessageDeliveryId,
            incomingMessage.LockId,
            lockRenewalCts.Token
        );

        try
        {
            await using var scope = _scopeFactory.CreateAsyncScope();

            // Deserialize the message
            var messageBody = _contextFactory.DeserializeMessage(
                incomingMessage,
                _context.MessageType,
                _context.QueueName,
                out var errorMessage
            );

            if (messageBody is null)
            {
                await _errorHandler.DeadletterAsync(
                    incomingMessage,
                    _context.QueueName,
                    errorMessage ?? "Unknown deserialization error",
                    messageBody is null && errorMessage?.Contains("null") == true
                        ? "NullMessage"
                        : "DeserializationFailed",
                    CancellationToken.None
                );
                return;
            }

            // Resolve the processor
            var processor = scope.ServiceProvider.GetRequiredService(_context.ProcessorType);

            // Build context using factory
            var context = ProcessorContextFactory.CreateContext(
                incomingMessage,
                messageBody,
                _context.MessageType
            );

            // Invoke ProcessAsync using reflection
            var processMethod = _context.ProcessorType.GetMethod(
                nameof(IProcessor<object>.ProcessAsync)
            );
            if (processMethod is null)
            {
                throw new InvalidOperationException(
                    $"ProcessAsync method not found on processor {_context.ProcessorType.Name}"
                );
            }

            if (_context.ResponseMessageType is not null)
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
                _context.QueueName
            );
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Abandon the message when shutting down so it can be reprocessed
            _logger.LogWarning(
                "Processing cancelled for message {MessageId}, abandoning",
                incomingMessage.MessageId
            );

            await _errorHandler.AbandonWithoutErrorAsync(incomingMessage, CancellationToken.None);
        }
        catch (Exception ex)
        {
            await _errorHandler.HandleErrorAsync(
                incomingMessage,
                _context.QueueName,
                ex,
                CancellationToken.None
            );
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
                var genericSendMethod = sendMethod.MakeGenericMethod(_context.ResponseMessageType!);

                await (Task)genericSendMethod.Invoke(_bus, [responseMessage, stoppingToken])!;

                _logger.LogDebug(
                    "Sent response message of type {ResponseType} for message {MessageId}",
                    _context.ResponseMessageType!.Name,
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
}
