using Bussig.Abstractions;
using Bussig.Abstractions.Middleware;
using Bussig.Exceptions;
using Microsoft.Extensions.Logging;

namespace Bussig.Processing.Middleware;

/// <summary>
/// Middleware that deserializes all message bodies.
/// Sets context.DeserializedMessages on success.
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
                    throw new DeserializationException("Message body deserialized to null");
                }

                deserializedMessages.Add(body);
            }
            catch (Exception ex)
            {
                throw new DeserializationException(ex.Message, ex);
            }
        }

        context.DeserializedMessages = deserializedMessages;
        await nextMiddleware(context);
    }
}
