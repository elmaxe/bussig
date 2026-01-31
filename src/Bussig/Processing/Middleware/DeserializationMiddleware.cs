using Bussig.Abstractions;
using Bussig.Abstractions.Middleware;
using Microsoft.Extensions.Logging;

namespace Bussig.Processing.Middleware;

/// <summary>
/// Middleware that deserializes all message bodies.
/// Sets context.DeserializedMessages on success.
/// Sets deserialization failure flags in context.Items on failure.
/// Handles both single-message and batch processing.
/// </summary>
internal sealed class DeserializationMiddleware : IMessageMiddleware
{
    private readonly IMessageSerializer _serializer;
    private readonly ILogger<DeserializationMiddleware> _logger;

    public DeserializationMiddleware(
        IMessageSerializer serializer,
        ILogger<DeserializationMiddleware> logger
    )
    {
        _serializer = serializer;
        _logger = logger;
    }

    public async Task InvokeAsync(MessageContext context, MessageMiddlewareDelegate nextMiddleware)
    {
        if (context.IsHandled)
        {
            return;
        }

        var deserializedMessages = new List<object>(context.Messages.Count);

        foreach (var message in context.Messages)
        {
            try
            {
                var body = _serializer.Deserialize(message.Body, context.MessageType);

                if (body is null)
                {
                    _logger.LogError(
                        "Deserialized message {MessageId} is null for queue {QueueName}",
                        message.MessageId,
                        context.QueueName
                    );

                    context.Exception = new InvalidOperationException(
                        "Message body deserialized to null"
                    );
                    context.Items[MiddlewareConstants.DeserializationFailed] = true;
                    context.Items[MiddlewareConstants.ErrorMessage] =
                        "Message body deserialized to null";
                    context.Items[MiddlewareConstants.ErrorCode] = "NullMessage";
                    return;
                }

                deserializedMessages.Add(body);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Failed to deserialize message {MessageId} for queue {QueueName}",
                    message.MessageId,
                    context.QueueName
                );

                context.Exception = ex;
                context.Items[MiddlewareConstants.DeserializationFailed] = true;
                context.Items[MiddlewareConstants.ErrorMessage] = ex.Message;
                context.Items[MiddlewareConstants.ErrorCode] = "DeserializationFailed";
                return;
            }
        }

        context.DeserializedMessages = deserializedMessages;
        await nextMiddleware(context);
    }
}
