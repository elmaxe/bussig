using System.Globalization;
using Bussig.Abstractions;
using Bussig.Configuration;
using Bussig.Constants;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace Bussig;

public sealed class PostgresMessageReceiver : IMessageLockRenewer
{
    private readonly NpgsqlDataSource _npgsqlDataSource;
    private readonly string _getMessagesSql;
    private readonly string _completeMessageSql;
    private readonly string _completeMessagesSql;
    private readonly string _abandonMessageSql;
    private readonly string _abandonMessagesSql;
    private readonly string _deadletterMessageSql;
    private readonly string _renewMessageLockSql;

    public PostgresMessageReceiver(
        [FromKeyedServices(ServiceKeys.BussigNpgsql)] NpgsqlDataSource npgsqlDataSource,
        IOptions<PostgresSettings> settings
    )
    {
        _npgsqlDataSource = npgsqlDataSource;
        var schema = settings.Value.Schema;

        _getMessagesSql = string.Format(
            CultureInfo.InvariantCulture,
            PsqlStatements.GetMessages,
            schema
        );
        _completeMessageSql = string.Format(
            CultureInfo.InvariantCulture,
            PsqlStatements.CompleteMessage,
            schema
        );
        _completeMessagesSql = string.Format(
            CultureInfo.InvariantCulture,
            PsqlStatements.CompleteMessages,
            schema
        );
        _abandonMessageSql = string.Format(
            CultureInfo.InvariantCulture,
            PsqlStatements.AbandonMessage,
            schema
        );
        _abandonMessagesSql = string.Format(
            CultureInfo.InvariantCulture,
            PsqlStatements.AbandonMessages,
            schema
        );
        _deadletterMessageSql = string.Format(
            CultureInfo.InvariantCulture,
            PsqlStatements.DeadLetterMessage,
            schema
        );
        _renewMessageLockSql = string.Format(
            CultureInfo.InvariantCulture,
            PsqlStatements.RenewMessageLock,
            schema
        );
    }

    public async Task<IReadOnlyList<IncomingMessage>> ReceiveAsync(
        string queueName,
        Guid lockId,
        TimeSpan lockDuration,
        int count,
        CancellationToken cancellationToken
    )
    {
        await using var conn = await _npgsqlDataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(_getMessagesSql, conn);

        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = queueName });
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = lockId });
        cmd.Parameters.Add(new NpgsqlParameter<TimeSpan> { TypedValue = lockDuration });
        cmd.Parameters.Add(new NpgsqlParameter<int> { TypedValue = count });

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        var messages = new List<IncomingMessage>();
        while (await reader.ReadAsync(cancellationToken))
        {
            messages.Add(
                new IncomingMessage
                {
                    MessageId = reader.GetGuid(0),
                    MessageDeliveryId = reader.GetInt64(1),
                    // priority = reader.GetInt16(2)
                    // queue_id = reader.GetInt64(3)
                    VisibleAt = reader.GetDateTime(4),
                    Body = reader.IsDBNull(5) ? [] : reader.GetFieldValue<byte[]>(5),
                    Headers = reader.IsDBNull(6) ? null : reader.GetString(6),
                    MessageDeliveryHeaders = reader.IsDBNull(7) ? null : reader.GetString(7),
                    MessageVersion = reader.GetInt32(8),
                    // scheduling_token_id = reader.IsDBNull(9) ? null : reader.GetGuid(9)
                    LockId = reader.GetGuid(10),
                    EnqueuedAt = reader.GetDateTime(11),
                    LastDeliveredAt = reader.IsDBNull(12) ? null : reader.GetDateTime(12),
                    DeliveryCount = reader.GetInt32(13),
                    MaxDeliveryCount = reader.GetInt32(14),
                    ExpirationTime = reader.IsDBNull(15) ? null : reader.GetDateTime(15),
                }
            );
        }

        return messages;
    }

    public async Task<bool> CompleteAsync(
        long messageDeliveryId,
        Guid lockId,
        CancellationToken cancellationToken
    )
    {
        await using var conn = await _npgsqlDataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(_completeMessageSql, conn);

        cmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = messageDeliveryId });
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = lockId });

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is long;
    }

    public async Task<bool> CompleteAsync(
        IEnumerable<long> messageDeliveryIds,
        IEnumerable<Guid> lockIds,
        CancellationToken cancellationToken
    )
    {
        await using var conn = await _npgsqlDataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(_completeMessagesSql, conn);

        cmd.Parameters.Add(
            new NpgsqlParameter<long[]> { TypedValue = messageDeliveryIds.ToArray() }
        );
        cmd.Parameters.Add(new NpgsqlParameter<Guid[]> { TypedValue = lockIds.ToArray() });

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is long;
    }

    public async Task<bool> AbandonAsync(
        long messageDeliveryId,
        Guid lockId,
        string? headers,
        TimeSpan delay,
        CancellationToken cancellationToken
    )
    {
        await using var conn = await _npgsqlDataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(_abandonMessageSql, conn);

        cmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = messageDeliveryId });
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = lockId });
        cmd.Parameters.Add(
            new NpgsqlParameter<string?> { TypedValue = headers, NpgsqlDbType = NpgsqlDbType.Jsonb }
        );
        cmd.Parameters.Add(new NpgsqlParameter<TimeSpan> { TypedValue = delay });

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is long;
    }

    public async Task<long> AbandonAsync(
        IEnumerable<long> messageDeliveryIds,
        IEnumerable<Guid> lockIds,
        IEnumerable<string?> headers,
        TimeSpan delay,
        CancellationToken cancellationToken
    )
    {
        await using var conn = await _npgsqlDataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(_abandonMessagesSql, conn);

        cmd.Parameters.Add(
            new NpgsqlParameter<long[]> { TypedValue = messageDeliveryIds.ToArray() }
        );
        cmd.Parameters.Add(new NpgsqlParameter<Guid[]> { TypedValue = lockIds.ToArray() });
        cmd.Parameters.Add(
            new NpgsqlParameter
            {
                Value = headers.ToArray(),
                NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Jsonb,
            }
        );
        cmd.Parameters.Add(new NpgsqlParameter<TimeSpan> { TypedValue = delay });

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is long count ? count : 0;
    }

    public async Task<bool> DeadletterAsync(
        long messageDeliveryId,
        Guid lockId,
        string queueName,
        string? headers,
        CancellationToken cancellationToken
    )
    {
        await using var conn = await _npgsqlDataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(_deadletterMessageSql, conn);

        cmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = messageDeliveryId });
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = lockId });
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = queueName });
        cmd.Parameters.Add(
            new NpgsqlParameter<string?> { TypedValue = headers, NpgsqlDbType = NpgsqlDbType.Jsonb }
        );

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is long;
    }

    public async Task<bool> RenewLockAsync(
        long messageDeliveryId,
        Guid lockId,
        TimeSpan duration,
        CancellationToken cancellationToken
    )
    {
        await using var conn = await _npgsqlDataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(_renewMessageLockSql, conn);

        cmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = messageDeliveryId });
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = lockId });
        cmd.Parameters.Add(new NpgsqlParameter<TimeSpan> { TypedValue = duration });

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is long;
    }

    public async Task<bool> CompleteWithinTransactionAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long messageDeliveryId,
        Guid lockId,
        CancellationToken cancellationToken
    )
    {
        await using var cmd = new NpgsqlCommand(_completeMessageSql, connection, transaction);

        cmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = messageDeliveryId });
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = lockId });

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is long;
    }
}
