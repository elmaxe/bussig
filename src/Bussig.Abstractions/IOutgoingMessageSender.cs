namespace Bussig.Abstractions;

public interface IOutgoingMessageSender
{
    Task<long> SendAsync(OutgoingMessage message, CancellationToken cancellationToken);
    Task<bool> CancelAsync(Guid schedulingToken, CancellationToken cancellationToken);
}
