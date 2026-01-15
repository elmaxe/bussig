namespace Bussig.Abstractions;

public interface IOutgoingMessageSender
{
    Task<long> SendAsync(OutgoingMessage message, CancellationToken cancellationToken);
    Task<long> CancelAsync(Guid schedulingToken, CancellationToken cancellationToken);
}
