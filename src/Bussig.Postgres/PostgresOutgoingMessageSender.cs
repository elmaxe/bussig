using Bussig.Abstractions;
using Npgsql;
using NpgsqlTypes;

namespace Bussig.Postgres;

public sealed class PostgresOutgoingMessageSender : IOutgoingMessageSender
{
    private readonly IPostgresConnectionContext _connectionContext;
    private readonly IPostgresTransactionAccessor _transactionAccessor;
    private readonly string _sendMessageSql;
    private readonly string _cancelScheduledSql;

    public PostgresOutgoingMessageSender(
        IPostgresConnectionContext connectionContext,
        IPostgresTransactionAccessor transactionAccessor,
        IPostgresSettings settings
    )
    {
        _connectionContext = connectionContext;
        _transactionAccessor = transactionAccessor;
        _sendMessageSql = string.Format(PsqlStatements.SendMessage, settings.Schema);
        _cancelScheduledSql = string.Format(PsqlStatements.CancelScheduledMessage, settings.Schema);
    }

    public async Task<long> SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        var parameters = BuildSendParameters(message);
        var transaction = _transactionAccessor.CurrentTransaction;

        if (transaction is null)
        {
            return await _connectionContext.Query<long>(
                _sendMessageSql,
                parameters,
                cancellationToken
            );
        }

        var connection = transaction.Connection;
        if (connection is null)
        {
            throw new InvalidOperationException("Ambient transaction has no active connection.");
        }

        await using var command = new NpgsqlCommand(_sendMessageSql, connection, transaction);
        command.Parameters.AddRange(parameters);
        return (long)await command.ExecuteScalarAsync(cancellationToken);
    }

    public async Task<bool> CancelAsync(Guid schedulingToken, CancellationToken cancellationToken)
    {
        NpgsqlParameter[] parameters = [new NpgsqlParameter<Guid> { TypedValue = schedulingToken }];
        var transaction = _transactionAccessor.CurrentTransaction;
        if (transaction is null)
        {
            return (
                await _connectionContext.Query<Guid?>(
                    _cancelScheduledSql,
                    parameters,
                    cancellationToken
                )
            ).HasValue;
        }

        var connection = transaction.Connection;
        if (connection is null)
        {
            throw new InvalidOperationException("Ambient transaction has no active connection");
        }

        await using var command = new NpgsqlCommand(_cancelScheduledSql, connection, transaction);
        command.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = schedulingToken });
        var result = (Guid?)await command.ExecuteScalarAsync(cancellationToken);

        return result.HasValue;
    }

    private static NpgsqlParameter[] BuildSendParameters(OutgoingMessage message)
    {
        return
        [
            new NpgsqlParameter<string> { TypedValue = message.QueueName },
            new NpgsqlParameter<Guid> { TypedValue = message.MessageId },
            new NpgsqlParameter<short?> { TypedValue = message.Priority },
            new NpgsqlParameter<byte[]> { TypedValue = message.Body },
            new NpgsqlParameter<TimeSpan> { TypedValue = message.Delay ?? TimeSpan.Zero },
            new NpgsqlParameter<string>
            {
                TypedValue = message.HeadersJson,
                NpgsqlDbType = NpgsqlDbType.Jsonb,
            },
            new NpgsqlParameter<int> { TypedValue = message.MessageVersion },
            new NpgsqlParameter<DateTimeOffset?> { TypedValue = message.ExpirationTime },
            new NpgsqlParameter<Guid?> { TypedValue = message.SchedulingTokenId },
        ];
    }
}
