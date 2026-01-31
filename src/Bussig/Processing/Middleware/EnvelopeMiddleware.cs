using System.Text.Json;
using Bussig.Abstractions.Middleware;

namespace Bussig.Processing.Middleware;

/// <summary>
/// Middleware that constructs MessageEnvelopes from incoming message metadata and deserialized payloads.
/// Must run after DeserializationMiddleware.
/// Handles both single-message and batch processing.
/// </summary>
internal sealed class EnvelopeMiddleware : IMessageMiddleware
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public async Task InvokeAsync(MessageContext context, MessageMiddlewareDelegate nextMiddleware)
    {
        if (context.IsHandled || context.DeserializedMessages is null)
        {
            await nextMiddleware(context);
            return;
        }

        var envelopes = new List<MessageEnvelope>(context.Messages.Count);

        for (var i = 0; i < context.Messages.Count; i++)
        {
            var message = context.Messages[i];
            var deserializedMessage = context.DeserializedMessages[i];

            var (headers, correlationId, messageType) = ParseHeaders(message.Headers);

            envelopes.Add(
                new MessageEnvelope
                {
                    MessageId = message.MessageId,
                    MessageType = messageType ?? context.MessageType.Name,
                    Timestamp = message.EnqueuedAt,
                    CorrelationId = correlationId,
                    Headers = headers,
                    Payload = deserializedMessage,
                }
            );
        }

        context.Envelopes = envelopes;
        await nextMiddleware(context);
    }

    private static (
        IReadOnlyDictionary<string, string> Headers,
        Guid? CorrelationId,
        string? MessageType
    ) ParseHeaders(string? headersJson)
    {
        if (string.IsNullOrEmpty(headersJson))
        {
            return (new Dictionary<string, string>(), null, null);
        }

        try
        {
            var rawHeaders = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(
                headersJson,
                JsonOptions
            );

            if (rawHeaders is null)
            {
                return (new Dictionary<string, string>(), null, null);
            }

            var headers = new Dictionary<string, string>();
            Guid? correlationId = null;
            string? messageType = null;

            foreach (var (key, value) in rawHeaders)
            {
                switch (key)
                {
                    case "correlation-id":
                        if (Guid.TryParse(value.GetString(), out var parsed))
                        {
                            correlationId = parsed;
                        }
                        break;

                    case "message-types":
                        if (value.ValueKind == JsonValueKind.Array && value.GetArrayLength() > 0)
                        {
                            messageType = value[0].GetString();
                        }
                        break;

                    default:
                        // Convert all values to strings for the headers dictionary
                        headers[key] = value.ValueKind switch
                        {
                            JsonValueKind.String => value.GetString() ?? string.Empty,
                            JsonValueKind.Number => value.GetRawText(),
                            JsonValueKind.True => "true",
                            JsonValueKind.False => "false",
                            JsonValueKind.Null => string.Empty,
                            _ => value.GetRawText(),
                        };
                        break;
                }
            }

            return (headers, correlationId, messageType);
        }
        catch
        {
            return (new Dictionary<string, string>(), null, null);
        }
    }
}
