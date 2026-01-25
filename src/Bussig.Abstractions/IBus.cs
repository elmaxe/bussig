using Bussig.Abstractions.Messages;

namespace Bussig.Abstractions;

public interface IBus
{
    Task SendAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default)
        where TMessage : IMessage;

    Task SendAsync<TMessage>(
        TMessage message,
        MessageSendOptions options,
        CancellationToken cancellationToken = default
    )
        where TMessage : IMessage;

    Task ScheduleAsync<TMessage>(
        TMessage message,
        TimeSpan delay,
        Guid? schedulingToken = null,
        CancellationToken cancellationToken = default
    )
        where TMessage : IMessage;

    Task ScheduleAsync<TMessage>(
        TMessage message,
        MessageSendOptions options,
        CancellationToken cancellationToken = default
    )
        where TMessage : IMessage;

    Task ScheduleAsync<TMessage>(
        TMessage message,
        DateTimeOffset visibleAt,
        Guid? schedulingToken = null,
        CancellationToken cancellationToken = default
    )
        where TMessage : IMessage;

    Task<bool> CancelScheduledAsync(
        Guid schedulingToken,
        CancellationToken cancellationToken = default
    );
}
