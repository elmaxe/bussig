using System.Text.Json;
using Bussig.Abstractions;
using Bussig.Abstractions.Messages;
using SecurityDriven;

namespace Bussig;

public sealed class Bus : IBus
{
    private static readonly JsonSerializerOptions HeaderJsonOptions = new(
        JsonSerializerDefaults.Web
    );
    private readonly IOutgoingMessageSender _messageSender;
    private readonly IMessageSerializer _serializer;

    public Bus(IOutgoingMessageSender messageSender, IMessageSerializer serializer)
    {
        _messageSender = messageSender;
        _serializer = serializer;
    }

    public Task SendAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default)
        where TMessage : IMessage
    {
        return SendAsync(message, new MessageSendOptions(), cancellationToken);
    }

    public Task SendAsync<TMessage>(
        TMessage message,
        MessageSendOptions options,
        CancellationToken cancellationToken = default
    )
        where TMessage : IMessage
    {
        return SendAsyncInternal(message, options, cancellationToken);
    }

    public Task ScheduleAsync<TMessage>(
        TMessage message,
        TimeSpan delay,
        Guid? schedulingToken,
        CancellationToken cancellationToken = default
    )
        where TMessage : IMessage
    {
        return ScheduleAsync(
            message,
            new MessageSendOptions { Delay = delay, SchedulingToken = schedulingToken },
            cancellationToken
        );
    }

    public Task ScheduleAsync<TMessage>(
        TMessage message,
        MessageSendOptions options,
        CancellationToken cancellationToken = default
    )
        where TMessage : IMessage
    {
        return SendAsyncInternal(message, options, cancellationToken);
    }

    public Task ScheduleAsync<TMessage>(
        TMessage message,
        DateTimeOffset visibleAt,
        Guid? schedulingToken,
        CancellationToken cancellationToken = default
    )
        where TMessage : IMessage
    {
        if (visibleAt < DateTimeOffset.UtcNow)
        {
            throw new ArgumentException($"{nameof(visibleAt)} should be in the future");
        }

        return SendAsyncInternal(
            message,
            new MessageSendOptions
            {
                Delay = visibleAt - DateTimeOffset.UtcNow,
                SchedulingToken = schedulingToken,
            },
            cancellationToken
        );
    }

    public async Task<bool> CancelScheduledAsync(
        Guid schedulingToken,
        CancellationToken cancellationToken = default
    )
    {
        return await _messageSender.CancelAsync(schedulingToken, cancellationToken);
    }

    private async Task SendAsyncInternal<TMessage>(
        TMessage message,
        MessageSendOptions options,
        CancellationToken cancellationToken
    )
        where TMessage : IMessage
    {
        var queueName = MessageMetadata<TMessage>.QueueName;
        var headersJson = MergeHeaders(
            MessageMetadata<TMessage>.HeadersJson,
            options.CorrelationId,
            options.Headers
        );

        var body = _serializer.SerializeToUtf8Bytes(message);
        var outgoing = new OutgoingMessage(
            options.MessageId ?? FastGuid.NewPostgreSqlGuid(),
            queueName,
            body,
            headersJson
        )
        {
            Delay = options.Delay,
            SchedulingTokenId = options.SchedulingToken,
            MessageVersion = options.MessageVersion,
            Priority = options.Priority.HasValue ? (short)options.Priority : null,
        };
        await _messageSender.SendAsync(outgoing, cancellationToken);
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

    private static string CreateHeadersJson(string messageType)
    {
        var headers = new Dictionary<string, object> { ["message-types"] = new[] { messageType } };

        return JsonSerializer.Serialize(headers, HeaderJsonOptions);
    }

    private static class MessageMetadata<TMessage>
        where TMessage : IMessage
    {
        public static readonly string QueueName = MessageUrn.ForType<TMessage>().ToString();
        public static readonly string HeadersJson = CreateHeadersJson(QueueName);
    }
}
