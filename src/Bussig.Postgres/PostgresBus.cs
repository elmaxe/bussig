using Bussig.Abstractions;
using Bussig.Abstractions.Messages;

namespace Bussig.Postgres;

public sealed class PostgresBus(PostgresQueueSender queueSender) : IBus
{
    public Task SendAsync<TMessage>(TMessage message, CancellationToken cancellationToken)
        where TMessage : ICommand
    {
        throw new NotImplementedException();
    }

    public Task SendAsync<TMessage>(
        IEnumerable<TMessage> messages,
        CancellationToken cancellationToken
    )
        where TMessage : ICommand
    {
        throw new NotImplementedException();
    }

    public Task ScheduleAsync<TMessage>(TMessage message, CancellationToken cancellationToken)
        where TMessage : ICommand
    {
        throw new NotImplementedException();
    }

    public Task ScheduleAsync<TMessage>(
        IEnumerable<TMessage> messages,
        CancellationToken cancellationToken
    )
        where TMessage : ICommand
    {
        throw new NotImplementedException();
    }
}
