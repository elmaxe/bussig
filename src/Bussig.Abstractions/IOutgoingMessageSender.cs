namespace Bussig.Abstractions;

public interface IOutgoingMessageSender
{
    Task SendAsync(OutgoingMessage message, CancellationToken cancellationToken);
    Task<bool> CancelAsync(Guid schedulingToken, CancellationToken cancellationToken);
}
