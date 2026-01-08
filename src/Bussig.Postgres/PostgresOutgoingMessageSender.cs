using Bussig.Abstractions;
using Npgsql;
using NpgsqlTypes;

namespace Bussig.Postgres;

public sealed class PostgresOutgoingMessageSender : IOutgoingMessageSender
{
    private readonly IPostgresConnectionContext _connectionContext;
    private readonly IPostgresTransactionAccessor _transactionAccessor;
    private readonly string _sendMessageSql;

    public PostgresOutgoingMessageSender(
        IPostgresConnectionContext connectionContext,
        IPostgresTransactionAccessor transactionAccessor,
        IPostgresSettings settings
    )
    {
        _connectionContext = connectionContext;
        _transactionAccessor = transactionAccessor;
        _sendMessageSql = string.Format(PsqlStatements.SendMessage, settings.Schema);
    }

    public async Task SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        var parameters = BuildSendParameters(message);
        var transaction = _transactionAccessor.CurrentTransaction;

        if (transaction is null)
        {
            await _connectionContext.Query<long>(_sendMessageSql, parameters, cancellationToken);
            return;
        }

        var connection = transaction.Connection;
        if (connection is null)
        {
            throw new InvalidOperationException(
                "Ambient transaction has no active connection."
            );
        }

        await using var command = new NpgsqlCommand(_sendMessageSql, connection, transaction);
        command.Parameters.AddRange(parameters);
        await command.ExecuteScalarAsync(cancellationToken);
    }

    private static NpgsqlParameter[] BuildSendParameters(OutgoingMessage message)
    {
        return
        [
            new NpgsqlParameter<string> { TypedValue = message.QueueName },
            new NpgsqlParameter<Guid> { TypedValue = message.MessageId },
            new NpgsqlParameter<int?> { TypedValue = message.Priority },
            new NpgsqlParameter<byte[]> { TypedValue = message.Body },
            new NpgsqlParameter<TimeSpan>
            {
                TypedValue = message.Delay ?? TimeSpan.Zero,
            },
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
