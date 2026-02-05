using Bussig.Abstractions;
using Bussig.Abstractions.Messages;
using Bussig.Abstractions.Middleware;
using Bussig.Sending;
using Microsoft.Extensions.DependencyInjection;

namespace Bussig;

public sealed class Bus : IBus
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly OutgoingMessageMiddlewarePipeline _sendPipeline;

    public Bus(IServiceScopeFactory scopeFactory, OutgoingMessageMiddlewarePipeline sendPipeline)
    {
        _scopeFactory = scopeFactory;
        _sendPipeline = sendPipeline;
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
        await using var scope = _scopeFactory.CreateAsyncScope();
        var messageSender = scope.ServiceProvider.GetRequiredService<IOutgoingMessageSender>();
        return await messageSender.CancelAsync(schedulingToken, cancellationToken);
    }

    private async Task SendAsyncInternal<TMessage>(
        TMessage message,
        MessageSendOptions options,
        CancellationToken cancellationToken
    )
        where TMessage : IMessage
    {
        await using var scope = _scopeFactory.CreateAsyncScope();

        var context = new OutgoingMessageContext
        {
            Message = message,
            MessageType = typeof(TMessage),
            Options = options,
            QueueName = MessageMetadata<TMessage>.QueueName,
            MessageTypes = MessageMetadata<TMessage>.MessageTypes,
            ServiceProvider = scope.ServiceProvider,
            CancellationToken = cancellationToken,
        };

        await _sendPipeline.ExecuteAsync(context);
    }

    private static class MessageMetadata<TMessage>
        where TMessage : IMessage
    {
        public static readonly string QueueName = MessageUrn.ForType<TMessage>().ToString();
        public static readonly IReadOnlyList<string> MessageTypes = [QueueName];
    }
}
