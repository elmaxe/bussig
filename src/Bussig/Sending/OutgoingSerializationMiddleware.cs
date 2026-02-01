using System.Text.Json;
using Bussig.Abstractions;
using Bussig.Abstractions.Middleware;
using Microsoft.Extensions.DependencyInjection;

namespace Bussig.Sending;

/// <summary>
/// Middleware that serializes the message body and merges headers.
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

        // Merge headers
        context.FinalHeadersJson = MergeHeaders(
            context.BaseHeadersJson,
            context.Options.CorrelationId,
            context.Options.Headers
        );

        return nextMiddleware(context);
    }

    private static string MergeHeaders(
        string baseHeadersJson,
        Guid? correlationId,
        Dictionary<string, object>? customHeaders
    )
    {
        var hasCustomHeaders = customHeaders is { Count: > 0 };
        if (correlationId is null && !hasCustomHeaders)
        {
            return baseHeadersJson;
        }

        var headers =
            JsonSerializer.Deserialize<Dictionary<string, object>>(
                baseHeadersJson,
                HeaderJsonOptions
            ) ?? new Dictionary<string, object>();

        if (correlationId is not null)
        {
            headers["correlation-id"] = correlationId.Value.ToString();
        }

        if (hasCustomHeaders)
        {
            foreach (var (key, value) in customHeaders!)
            {
                headers[key] = value;
            }
        }

        return JsonSerializer.Serialize(headers, HeaderJsonOptions);
    }
}
