using Bussig.Abstractions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Bussig.EntityFrameworkCore;

internal sealed class OutboxSender(
    [FromKeyedServices(OutboxServiceKeys.InnerSender)] IOutgoingMessageSender innerSender,
    OutboxTransactionContext outboxContext
) : IOutgoingMessageSender
{
    public Task SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        if (!outboxContext.IsActive)
        {
            return innerSender.SendAsync(message, cancellationToken);
        }

        var dbContext = outboxContext.DbContext;
        dbContext.Set<OutboxMessage>().Add(MapToEntity(message));

        return Task.CompletedTask;
    }

    public async Task<bool> CancelAsync(Guid schedulingToken, CancellationToken cancellationToken)
    {
        if (outboxContext.IsActive)
        {
            var dbContext = outboxContext.DbContext;
            var deleted = await dbContext
                .Set<OutboxMessage>()
                .Where(m => m.SchedulingTokenId == schedulingToken && m.PublishedAt == null)
                .ExecuteDeleteAsync(cancellationToken);

            if (deleted > 0)
            {
                return true;
            }
        }

        return await innerSender.CancelAsync(schedulingToken, cancellationToken);
    }

    private static OutboxMessage MapToEntity(OutgoingMessage message) =>
        new()
        {
            MessageId = message.MessageId,
            QueueName = message.QueueName,
            Body = message.Body,
            HeadersJson = message.HeadersJson,
            Priority = message.Priority,
            Delay = message.Delay,
            MessageVersion = message.MessageVersion,
            ExpirationTime = message.ExpirationTime,
            SchedulingTokenId = message.SchedulingTokenId,
            CreatedAt = DateTimeOffset.UtcNow,
        };
}
