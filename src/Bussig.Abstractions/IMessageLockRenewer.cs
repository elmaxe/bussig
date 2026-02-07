namespace Bussig.Abstractions;

public interface IMessageLockRenewer
{
    Task<bool> RenewLockAsync(
        long messageDeliveryId,
        Guid lockId,
        TimeSpan duration,
        CancellationToken cancellationToken
    );
}
