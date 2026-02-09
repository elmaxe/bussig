using Bussig.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Bussig.Outbox.Npgsql;

internal sealed class NpgsqlOutboxForwarder : BackgroundService
{
    private readonly IOutgoingMessageSender _innerSender;
    private readonly NpgsqlDataSource _dataSource;
    private readonly NpgsqlOutboxOptions _options;
    private readonly ILogger<NpgsqlOutboxForwarder> _logger;
    private readonly string _selectPendingSql;
    private readonly string _markPublishedSql;
    private readonly string _cleanupSql;

    public NpgsqlOutboxForwarder(
        [FromKeyedServices(OutboxServiceKeys.InnerSender)] IOutgoingMessageSender innerSender,
        [FromKeyedServices(OutboxServiceKeys.OutboxNpgsqlDataSource)] NpgsqlDataSource dataSource,
        IOptions<NpgsqlOutboxOptions> options,
        ILogger<NpgsqlOutboxForwarder> logger
    )
    {
        _innerSender = innerSender;
        _dataSource = dataSource;
        _options = options.Value;
        _logger = logger;
        _selectPendingSql = OutboxSqlStatements.SelectPending(_options.Schema);
        _markPublishedSql = OutboxSqlStatements.MarkPublished(_options.Schema);
        _cleanupSql = OutboxSqlStatements.CleanupPublished(_options.Schema);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox forwarder started");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var forwarded = await ForwardBatchAsync(stoppingToken);

                if (forwarded == 0)
                {
                    await Task.Delay(_options.PollingInterval, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in outbox forwarder, retrying after delay");
                await Task.Delay(_options.PollingInterval, stoppingToken);
            }
        }

        _logger.LogInformation("Outbox forwarder stopped");
    }

    private async Task<int> ForwardBatchAsync(CancellationToken cancellationToken)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        var forwarded = 0;

        try
        {
            await using var selectCmd = new NpgsqlCommand(_selectPendingSql, conn, tx);
            selectCmd.Parameters.Add(new NpgsqlParameter<int> { TypedValue = _options.BatchSize });

            await using var reader = await selectCmd.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                var outboxId = reader.GetInt64(0);
                var messageId = reader.GetGuid(1);
                var queueName = reader.GetString(2);
                var body = (byte[])reader.GetValue(3);
                var headersJson = reader.IsDBNull(4) ? null : reader.GetString(4);
                var priority = reader.IsDBNull(5) ? (short?)null : reader.GetInt16(5);
                var delay = reader.IsDBNull(6) ? (TimeSpan?)null : reader.GetTimeSpan(6);
                var messageVersion = reader.GetInt32(7);
                var expirationTime = reader.IsDBNull(8)
                    ? (DateTimeOffset?)null
                    : reader.GetFieldValue<DateTimeOffset>(8);
                var schedulingTokenId = reader.IsDBNull(9) ? (Guid?)null : reader.GetGuid(9);

                var outgoingMessage = new OutgoingMessage(
                    messageId,
                    queueName,
                    body,
                    headersJson ?? "{}"
                )
                {
                    Priority = priority,
                    Delay = delay,
                    MessageVersion = messageVersion,
                    ExpirationTime = expirationTime,
                    SchedulingTokenId = schedulingTokenId,
                };

                try
                {
                    await _innerSender.SendAsync(outgoingMessage, cancellationToken);
                }
                catch (PostgresException ex) when (ex.SqlState == "23505")
                {
                    _logger.LogDebug(
                        "Message {MessageId} already exists in bus (duplicate key), marking as published",
                        messageId
                    );
                }

                await using var markCmd = new NpgsqlCommand(_markPublishedSql, conn, tx);
                markCmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = outboxId });
                await markCmd.ExecuteNonQueryAsync(cancellationToken);

                forwarded++;
            }

            await tx.CommitAsync(cancellationToken);

            if (forwarded > 0)
            {
                _logger.LogDebug("Forwarded {Count} outbox messages", forwarded);
            }

            await CleanupAsync(cancellationToken);
        }
        catch
        {
            await tx.RollbackAsync(CancellationToken.None);
            throw;
        }

        return forwarded;
    }

    private async Task CleanupAsync(CancellationToken cancellationToken)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(_cleanupSql, conn);
        cmd.Parameters.Add(
            new NpgsqlParameter<TimeSpan> { TypedValue = _options.PublishedRetention }
        );

        var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken);

        if (deleted > 0)
        {
            _logger.LogDebug("Cleaned up {Count} published outbox messages", deleted);
        }
    }
}
