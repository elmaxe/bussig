using System.Text.Json;
using Bussig.Abstractions;
using Bussig.Abstractions.Messages;
using SecurityDriven;

namespace Bussig.Postgres;

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

    // TODO: Message send options for priority, version, expiration, headers, etc
    public Task SendAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default)
        where TMessage : ICommand
    {
        return SendAsyncInternal(message, null, null, cancellationToken);
    }

    public Task ScheduleAsync<TMessage>(
        TMessage message,
        TimeSpan delay,
        Guid? schedulingToken,
        CancellationToken cancellationToken = default
    )
        where TMessage : ICommand
    {
        return SendAsyncInternal(message, delay, schedulingToken, cancellationToken);
    }

    public Task ScheduleAsync<TMessage>(
        TMessage message,
        DateTimeOffset visibleAt,
        Guid? schedulingToken,
        CancellationToken cancellationToken = default
    )
        where TMessage : ICommand
    {
        if (visibleAt < DateTimeOffset.UtcNow)
        {
            throw new ArgumentException($"{nameof(visibleAt)} should be in the future");
        }
        return SendAsyncInternal(
            message,
            visibleAt - DateTimeOffset.UtcNow,
            schedulingToken,
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
        TimeSpan? delay,
        Guid? schedulingToken,
        CancellationToken cancellationToken
    )
        where TMessage : ICommand
    {
        var queueName = MessageMetadata<TMessage>.QueueName;
        var headersJson = MessageMetadata<TMessage>.HeadersJson;

        var body = _serializer.SerializeToUtf8Bytes(message);
        var outgoing = new OutgoingMessage(
            FastGuid.NewPostgreSqlGuid(),
            queueName,
            body,
            headersJson
        )
        {
            Delay = delay,
            SchedulingTokenId = schedulingToken,
        };
        await _messageSender.SendAsync(outgoing, cancellationToken);
    }

    private static string CreateHeadersJson(string messageType)
    {
        var headers = new Dictionary<string, object> { ["message-types"] = new[] { messageType } };

        return JsonSerializer.Serialize(headers, HeaderJsonOptions);
    }

    private static class MessageMetadata<TMessage>
        where TMessage : ICommand
    {
        public static readonly string QueueName = MessageUrn.ForType<TMessage>().ToString();
        public static readonly string HeadersJson = CreateHeadersJson(QueueName);
    }
}
