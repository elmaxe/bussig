using System.Text.Json;
using Bussig.Abstractions;
using Bussig.Abstractions.Middleware;

namespace Bussig.Processing.Middleware;

/// <summary>
/// Middleware that constructs MessageEnvelopes and DeliveryInfos from incoming message metadata.
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

        var envelopes = new List<MessageEnvelope<object>>(context.Messages.Count);
        var deliveryInfos = new List<DeliveryInfo>(context.Messages.Count);

        for (var i = 0; i < context.Messages.Count; i++)
        {
            var message = context.Messages[i];
            var deserializedMessage = context.DeserializedMessages[i];

            var (headers, correlationId, messageTypes, sentAt) = ParseHeaders(message.Headers);
            var deliveryHeaders = ParseDeliveryHeaders(message.MessageDeliveryHeaders);

            var envelope = new MessageEnvelope
            {
                MessageId = message.MessageId,
                SentAt = sentAt ?? message.EnqueuedAt,
                MessageTypes = messageTypes ?? [context.MessageType.Name],
                CorrelationId = correlationId,
                Headers = headers,
            };

            envelopes.Add(
                new MessageEnvelope<object> { Envelope = envelope, Payload = deserializedMessage }
            );

            deliveryInfos.Add(
                new DeliveryInfo
                {
                    DeliveryCount = message.DeliveryCount,
                    MaxDeliveryCount = message.MaxDeliveryCount,
                    EnqueuedAt = message.EnqueuedAt,
                    LastDeliveredAt = message.LastDeliveredAt,
                    DeliveryHeaders = deliveryHeaders,
                }
            );
        }

        context.Envelopes = envelopes;
        context.DeliveryInfos = deliveryInfos;
        await nextMiddleware(context);
    }

    private static (
        IReadOnlyDictionary<string, string> Headers,
        Guid? CorrelationId,
        IReadOnlyList<string>? MessageTypes,
        DateTimeOffset? SentAt
    ) ParseHeaders(string? headersJson)
    {
        if (string.IsNullOrEmpty(headersJson))
        {
            return (new Dictionary<string, string>(), null, null, null);
        }

        try
        {
            var rawHeaders = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(
                headersJson,
                JsonOptions
            );

            if (rawHeaders is null)
            {
                return (new Dictionary<string, string>(), null, null, null);
            }

            var headers = new Dictionary<string, string>();
            Guid? correlationId = null;
            List<string>? messageTypes = null;
            DateTimeOffset? sentAt = null;

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
                        if (value.ValueKind == JsonValueKind.Array)
                        {
                            messageTypes = [];
                            foreach (var item in value.EnumerateArray())
                            {
                                var str = item.GetString();
                                if (str is not null)
                                {
                                    messageTypes.Add(str);
                                }
                            }
                        }
                        break;

                    case "sent-at":
                        if (DateTimeOffset.TryParse(value.GetString(), out var parsedDate))
                        {
                            sentAt = parsedDate;
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

            return (headers, correlationId, messageTypes, sentAt);
        }
        catch
        {
            return (new Dictionary<string, string>(), null, null, null);
        }
    }

    private static Dictionary<string, string>? ParseDeliveryHeaders(string? deliveryHeadersJson)
    {
        if (string.IsNullOrEmpty(deliveryHeadersJson))
        {
            return null;
        }

        try
        {
            var rawHeaders = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(
                deliveryHeadersJson,
                JsonOptions
            );

            if (rawHeaders is null || rawHeaders.Count == 0)
            {
                return null;
            }

            var headers = new Dictionary<string, string>();
            foreach (var (key, value) in rawHeaders)
            {
                headers[key] = value.ValueKind switch
                {
                    JsonValueKind.String => value.GetString() ?? string.Empty,
                    JsonValueKind.Number => value.GetRawText(),
                    JsonValueKind.True => "true",
                    JsonValueKind.False => "false",
                    JsonValueKind.Null => string.Empty,
                    _ => value.GetRawText(),
                };
            }

            return headers;
        }
        catch
        {
            return null;
        }
    }
}
