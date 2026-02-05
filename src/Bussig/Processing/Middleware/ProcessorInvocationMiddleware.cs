using System.Reflection;
using Bussig.Abstractions;
using Bussig.Abstractions.Messages;
using Bussig.Abstractions.Middleware;
using Bussig.Constants;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Bussig.Processing.Middleware;

/// <summary>
/// Terminal middleware that invokes the processor and handles message completion.
/// Handles both single-message and batch processors with atomic completion semantics.
/// </summary>
internal sealed class ProcessorInvocationMiddleware : IMessageMiddleware
{
    private readonly PostgresMessageReceiver _receiver;
    private readonly NpgsqlDataSource _npgsqlDataSource;
    private readonly IPostgresTransactionAccessor _transactionAccessor;
    private readonly IBus _bus;
    private readonly ILogger<ProcessorInvocationMiddleware> _logger;

    public ProcessorInvocationMiddleware(
        PostgresMessageReceiver receiver,
        [FromKeyedServices(ServiceKeys.BussigNpgsql)] NpgsqlDataSource npgsqlDataSource,
        IPostgresTransactionAccessor transactionAccessor,
        IBus bus,
        ILogger<ProcessorInvocationMiddleware> logger
    )
    {
        _receiver = receiver;
        _npgsqlDataSource = npgsqlDataSource;
        _transactionAccessor = transactionAccessor;
        _bus = bus;
        _logger = logger;
    }

    public async Task InvokeAsync(MessageContext context, MessageMiddlewareDelegate nextMiddleware)
    {
        if (
            context.IsHandled
            || context.DeserializedMessages is null
            || context.Envelopes is null
            || context.DeliveryInfos is null
        )
        {
            await nextMiddleware(context);
            return;
        }

        // Build processor contexts for all messages
        var contexts = new List<object>(context.Messages.Count);
        for (var i = 0; i < context.Messages.Count; i++)
        {
            var processorContext = CreateContext(
                context.Messages[i],
                context.DeserializedMessages[i],
                context.Envelopes[i].Envelope,
                context.DeliveryInfos[i],
                context.MessageType
            );
            contexts.Add(processorContext);
        }
        context.ProcessorContexts = contexts;

        // Resolve the processor
        var processor = context.ServiceProvider.GetRequiredService(context.ProcessorType);

        // Get the ProcessAsync method
        var processMethod = context.ProcessorType.GetMethod(
            nameof(IProcessor<object>.ProcessAsync)
        );
        if (processMethod is null)
        {
            throw new InvalidOperationException(
                $"ProcessAsync method not found on processor {context.ProcessorType.Name}"
            );
        }

        if (context.IsBatchProcessor)
        {
            // Batch processor - create batch and call processor once
            await ProcessBatchAsync(processor, processMethod, contexts, context);
        }
        else if (context.ResponseMessageType is not null)
        {
            // Request-reply processor with transactional outbox
            // Note: Request-reply only supported for single messages (batch of 1)
            await ProcessWithOutboxAsync(processor, processMethod, contexts[0], context);
        }
        else
        {
            // Fire-and-forget processor - call for each message
            await ProcessSingleMessagesAsync(processor, processMethod, contexts[0], context);
        }

        // All succeeded - complete atomically
        await context.CompleteAllAsync();

        _logger.LogDebug(
            "Successfully processed {Count} message(s) from queue {QueueName}",
            context.Messages.Count,
            context.QueueName
        );

        context.IsHandled = true;

        // Continue pipeline in case there are post-processing middleware
        await nextMiddleware(context);
    }

    private static async Task ProcessBatchAsync(
        object processor,
        MethodInfo processMethod,
        List<object> contexts,
        MessageContext context
    )
    {
        // Create the batch using the factory method
        var batch = CreateBatch(contexts, context.MessageType);

        // Invoke ProcessAsync with the batch
        var task = (Task)processMethod.Invoke(processor, [batch, context.CancellationToken])!;
        await task;
    }

    private static async Task ProcessSingleMessagesAsync(
        object processor,
        MethodInfo processMethod,
        object processorContext,
        MessageContext context
    )
    {
        var task = (Task)
            processMethod.Invoke(processor, [processorContext, context.CancellationToken])!;
        await task;
    }

    private async Task ProcessWithOutboxAsync(
        object processor,
        MethodInfo processMethod,
        object processorContext,
        MessageContext context
    )
    {
        await using var conn = await _npgsqlDataSource.OpenConnectionAsync(
            context.CancellationToken
        );
        await using var transaction = await conn.BeginTransactionAsync(context.CancellationToken);

        using var _ = _transactionAccessor.Use(transaction);

        try
        {
            // Invoke ProcessAsync - returns Task<TSend>
            var task = (Task)
                processMethod.Invoke(processor, [processorContext, context.CancellationToken])!;
            await task;

            // Get the result using reflection (task is Task<TSend>)
            var resultProperty = task.GetType().GetProperty("Result");
            var responseMessage = resultProperty?.GetValue(task);
            context.ResponseMessage = responseMessage;

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
                var genericSendMethod = sendMethod.MakeGenericMethod(context.ResponseMessageType!);

                await (Task)
                    genericSendMethod.Invoke(_bus, [responseMessage, context.CancellationToken])!;

                _logger.LogDebug(
                    "Sent response message of type {ResponseType} for message {MessageId}",
                    context.ResponseMessageType!.Name,
                    context.Message.MessageId
                );
            }

            // Complete message within transaction
            await _receiver.CompleteWithinTransactionAsync(
                conn,
                transaction,
                context.Message.MessageDeliveryId,
                context.Message.LockId,
                context.CancellationToken
            );

            await transaction.CommitAsync(context.CancellationToken);
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    private static object CreateBatch(List<object> contexts, Type batchMessageType)
    {
        var createBatchMethod = typeof(ProcessorInvocationMiddleware)
            .GetMethod(nameof(CreateBatchGeneric), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(batchMessageType);
        return createBatchMethod.Invoke(null, [contexts])!;
    }

    private static MessageBatch<TMessage> CreateBatchGeneric<TMessage>(List<object> contexts)
        where TMessage : class, IMessage
    {
        var typedContexts = contexts.Cast<MessageProcessorContext<TMessage>>();
        return new MessageBatch<TMessage>(typedContexts);
    }

    private static object CreateContext(
        IncomingMessage incomingMessage,
        object messageBody,
        MessageEnvelope envelope,
        DeliveryInfo delivery,
        Type messageType
    )
    {
        var contextType = typeof(MessageProcessorContext<>).MakeGenericType(messageType);
        var context = Activator.CreateInstance(
            contextType,
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            args:
            [
                messageBody,
                envelope,
                delivery,
                incomingMessage.MessageDeliveryId,
                incomingMessage.LockId,
            ],
            culture: null
        );

        if (context is null)
        {
            throw new InvalidOperationException(
                $"Failed to create context for message type {messageType.Name}"
            );
        }

        return context;
    }
}
