using System.Text.Json;
using Bussig.Abstractions;
using Bussig.Abstractions.Middleware;
using Microsoft.Extensions.DependencyInjection;
using SecurityDriven;

namespace Bussig.Sending;

/// <summary>
/// Middleware that serializes the message body and constructs the envelope.
/// </summary>
internal sealed class OutgoingSerializationMiddleware : IOutgoingMessageMiddleware
{
    private static readonly JsonSerializerOptions HeaderJsonOptions = new(
        JsonSerializerDefaults.Web
    );

    public Task InvokeAsync(
        OutgoingMessageContext context,
        OutgoingMessageMiddlewareDelegate nextMiddleware
    )
    {
        var serializer = context.ServiceProvider.GetRequiredService<IMessageSerializer>();

        // Serialize the message body
        context.SerializedBody = serializer.SerializeToUtf8Bytes(context.Message);

        // Build the envelope
        var envelope = new MessageEnvelope
        {
            MessageId = context.Options.MessageId ?? FastGuid.NewPostgreSqlGuid(),
            SentAt = DateTimeOffset.UtcNow,
            MessageTypes = context.MessageTypes,
            CorrelationId = context.Options.CorrelationId,
            Headers = context.Options.Headers,
        };

        context.Envelope = envelope;

        // Serialize envelope to JSON for storage
        context.EnvelopeJson = SerializeEnvelope(envelope);

        return nextMiddleware(context);
    }

    private static string SerializeEnvelope(MessageEnvelope envelope)
    {
        var headers = new Dictionary<string, object>
        {
            ["message-types"] = envelope.MessageTypes,
            ["sent-at"] = envelope.SentAt.ToString("O"),
        };

        if (envelope.CorrelationId is not null)
        {
            headers["correlation-id"] = envelope.CorrelationId.Value.ToString();
        }

        if (envelope.Headers.Count > 0)
        {
            foreach (var (key, value) in envelope.Headers)
            {
                headers[key] = value;
            }
        }

        return JsonSerializer.Serialize(headers, HeaderJsonOptions);
    }
}
