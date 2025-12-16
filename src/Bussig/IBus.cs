using Bussig.Messages;

namespace Bussig;

public interface IBus
{
    Task SendAsync<TMessage>(TMessage message, CancellationToken cancellationToken)
        where TMessage : ICommand;

    Task SendAsync<TMessage>(IEnumerable<TMessage> messages, CancellationToken cancellationToken)
        where TMessage : ICommand;

    Task ScheduleAsync<TMessage>(TMessage message, CancellationToken cancellationToken)
        where TMessage : ICommand;

    Task ScheduleAsync<TMessage>(
        IEnumerable<TMessage> messages,
        CancellationToken cancellationToken
    )
        where TMessage : ICommand;
}
