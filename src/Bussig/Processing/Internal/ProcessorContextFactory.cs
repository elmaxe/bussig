using System.Reflection;
using Bussig.Abstractions;
using Bussig.Abstractions.Messages;
using Microsoft.Extensions.Logging;

namespace Bussig.Processing.Internal;

/// <summary>
/// Factory for creating message processor contexts and batches using reflection.
/// </summary>
internal sealed class ProcessorContextFactory
{
    private readonly IMessageSerializer _serializer;
    private readonly ILogger _logger;

    public ProcessorContextFactory(IMessageSerializer serializer, ILogger logger)
    {
        _serializer = serializer;
        _logger = logger;
    }

    public object? DeserializeMessage(
        IncomingMessage incomingMessage,
        Type messageType,
        string queueName,
        out string? errorMessage
    )
    {
        errorMessage = null;
        try
        {
            var body = _serializer.Deserialize(incomingMessage.Body, messageType);
            if (body is null)
            {
                errorMessage = "Message body deserialized to null";
                _logger.LogError(
                    "Deserialized message {MessageId} is null for queue {QueueName}",
                    incomingMessage.MessageId,
                    queueName
                );
            }
            return body;
        }
        catch (Exception ex)
        {
            errorMessage = ex.Message;
            _logger.LogError(
                ex,
                "Failed to deserialize message {MessageId} for queue {QueueName}",
                incomingMessage.MessageId,
                queueName
            );
            return null;
        }
    }

    public static object CreateContext(
        IncomingMessage incomingMessage,
        object messageBody,
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
                $"Failed to create context for message type {messageType.Name}"
            );
        }

        return context;
    }

    public object CreateBatch(List<object> contexts, Type batchMessageType)
    {
        var createBatchMethod = GetType()
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
}
