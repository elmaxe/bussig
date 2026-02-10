using Bussig.Abstractions;
using Bussig.EntityFrameworkCore.StatementProviders;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Bussig.EntityFrameworkCore;

internal sealed class OutboxForwarder<TDbContext>(
    IServiceScopeFactory scopeFactory,
    IOptions<OutboxOptions> options,
    OutboxSqlStatementProvider sqlProvider,
    ILogger<OutboxForwarder<TDbContext>> logger
) : BackgroundService
    where TDbContext : DbContext
{
    private string? _selectSql;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Outbox forwarder started");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var forwarded = await ForwardBatchAsync(stoppingToken);

                if (forwarded == 0)
                {
                    await Task.Delay(options.Value.PollingInterval, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error in outbox forwarder, retrying after delay");
                await Task.Delay(options.Value.PollingInterval, stoppingToken);
            }
        }

        logger.LogInformation("Outbox forwarder stopped");
    }

    private async Task<int> ForwardBatchAsync(CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();
        var innerSender = scope.ServiceProvider.GetRequiredKeyedService<IOutgoingMessageSender>(
            OutboxServiceKeys.InnerSender
        );

        _selectSql ??= BuildSelectSql(dbContext);

        await using var tx = await dbContext.Database.BeginTransactionAsync(cancellationToken);

        var messages = await dbContext
            .Set<OutboxMessage>()
            .FromSqlRaw(_selectSql, options.Value.BatchSize)
            .ToListAsync(cancellationToken);

        if (messages.Count == 0)
        {
            await tx.CommitAsync(cancellationToken);
            return 0;
        }

        foreach (var msg in messages)
        {
            var outgoing = new OutgoingMessage(
                msg.MessageId,
                msg.QueueName,
                msg.Body,
                msg.HeadersJson ?? "{}"
            )
            {
                Priority = msg.Priority,
                Delay = msg.Delay,
                MessageVersion = msg.MessageVersion,
                ExpirationTime = msg.ExpirationTime,
                SchedulingTokenId = msg.SchedulingTokenId,
            };

            try
            {
                await innerSender.SendAsync(outgoing, cancellationToken);
            }
            catch (DbUpdateException ex)
                when (ex.InnerException is { } inner && inner.Message.Contains("23505"))
            {
                logger.LogDebug(
                    "Message {MessageId} already exists in bus (duplicate key), marking as published",
                    msg.MessageId
                );
            }

            msg.PublishedAt = DateTimeOffset.UtcNow;
        }

        await dbContext.SaveChangesAsync(cancellationToken);
        await tx.CommitAsync(cancellationToken);

        logger.LogDebug("Forwarded {Count} outbox messages", messages.Count);

        await CleanupAsync(cancellationToken);

        return messages.Count;
    }

    private async Task CleanupAsync(CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();

        var cutoff = DateTimeOffset.UtcNow - options.Value.PublishedRetention;

        var deleted = await dbContext
            .Set<OutboxMessage>()
            .Where(m => m.PublishedAt != null && m.PublishedAt < cutoff)
            .ExecuteDeleteAsync(cancellationToken);

        if (deleted > 0)
        {
            logger.LogDebug("Cleaned up {Count} published outbox messages", deleted);
        }
    }

    private string BuildSelectSql(TDbContext dbContext)
    {
        var entityType =
            dbContext.Model.FindEntityType(typeof(OutboxMessage))
            ?? throw new InvalidOperationException(
                "OutboxMessage entity is not configured in the DbContext model. "
                    + "Call modelBuilder.AddOutboxMessageEntity() in OnModelCreating."
            );

        var tableName =
            entityType.GetTableName()
            ?? throw new InvalidOperationException(
                "Could not determine table name for OutboxMessage."
            );
        var schema = entityType.GetSchema();
        var storeObject = StoreObjectIdentifier.Table(tableName, schema);

        var columnMap = new OutboxColumnMap
        {
            Id = GetColumnName(entityType, nameof(OutboxMessage.Id), storeObject),
            MessageId = GetColumnName(entityType, nameof(OutboxMessage.MessageId), storeObject),
            QueueName = GetColumnName(entityType, nameof(OutboxMessage.QueueName), storeObject),
            Body = GetColumnName(entityType, nameof(OutboxMessage.Body), storeObject),
            HeadersJson = GetColumnName(entityType, nameof(OutboxMessage.HeadersJson), storeObject),
            Priority = GetColumnName(entityType, nameof(OutboxMessage.Priority), storeObject),
            Delay = GetColumnName(entityType, nameof(OutboxMessage.Delay), storeObject),
            MessageVersion = GetColumnName(
                entityType,
                nameof(OutboxMessage.MessageVersion),
                storeObject
            ),
            ExpirationTime = GetColumnName(
                entityType,
                nameof(OutboxMessage.ExpirationTime),
                storeObject
            ),
            SchedulingTokenId = GetColumnName(
                entityType,
                nameof(OutboxMessage.SchedulingTokenId),
                storeObject
            ),
            CreatedAt = GetColumnName(entityType, nameof(OutboxMessage.CreatedAt), storeObject),
            PublishedAt = GetColumnName(entityType, nameof(OutboxMessage.PublishedAt), storeObject),
        };

        return sqlProvider.BuildSelectPendingBatchSql(tableName, schema, columnMap);
    }

    private static string GetColumnName(
        IEntityType entityType,
        string propertyName,
        StoreObjectIdentifier storeObject
    )
    {
        var property =
            entityType.FindProperty(propertyName)
            ?? throw new InvalidOperationException(
                $"Property '{propertyName}' not found on OutboxMessage entity."
            );

        return property.GetColumnName(storeObject)
            ?? throw new InvalidOperationException(
                $"Could not determine column name for property '{propertyName}'."
            );
    }
}
