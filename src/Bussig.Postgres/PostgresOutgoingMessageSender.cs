using System.Globalization;
using Bussig.Abstractions;
using Bussig.Constants;
using Bussig.Postgres.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace Bussig.Postgres;

public sealed class PostgresOutgoingMessageSender : IOutgoingMessageSender
{
    private readonly NpgsqlDataSource _npgsqlDataSource;
    private readonly IPostgresTransactionAccessor _transactionAccessor;
    private readonly PostgresSettings _settings;
    private readonly string _sendMessageSql;
    private readonly string _cancelScheduledSql;

    public PostgresOutgoingMessageSender(
        [FromKeyedServices(ServiceKeys.BussigNpgsql)] NpgsqlDataSource npgsqlDataSource,
        IPostgresTransactionAccessor transactionAccessor,
        IOptions<PostgresSettings> settings
    )
    {
        _npgsqlDataSource = npgsqlDataSource;
        _transactionAccessor = transactionAccessor;
        _settings = settings.Value;
        _sendMessageSql = string.Format(
            CultureInfo.InvariantCulture,
            PsqlStatements.SendMessage,
            _settings.Schema
        );
        _cancelScheduledSql = string.Format(
            CultureInfo.InvariantCulture,
            PsqlStatements.CancelScheduledMessage,
            _settings.Schema
        );
    }

    public async Task SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        var parameters = BuildSendParameters(message);
        var transaction = _transactionAccessor.CurrentTransaction;

        if (transaction is null)
        {
            await using var conn = await _npgsqlDataSource.OpenConnectionAsync(cancellationToken);
            await using var cmd = new NpgsqlCommand(_sendMessageSql, conn);
            cmd.Parameters.AddRange(parameters);
            await cmd.ExecuteNonQueryAsync(cancellationToken);

            return;
        }

        var connection = transaction.Connection;
        if (connection is null)
        {
            throw new InvalidOperationException("Ambient transaction has no active connection.");
        }

        await using var command = new NpgsqlCommand(_sendMessageSql, connection, transaction);
        command.Parameters.AddRange(parameters);
        await command.ExecuteScalarAsync(cancellationToken);
    }

    public async Task<bool> CancelAsync(Guid schedulingToken, CancellationToken cancellationToken)
    {
        NpgsqlParameter[] parameters = [new NpgsqlParameter<Guid> { TypedValue = schedulingToken }];
        var transaction = _transactionAccessor.CurrentTransaction;
        if (transaction is null)
        {
            await using var conn = await _npgsqlDataSource.OpenConnectionAsync(cancellationToken);
            await using var cmd = new NpgsqlCommand(_cancelScheduledSql, conn);
            cmd.Parameters.AddRange(parameters);
            return ((Guid?)await cmd.ExecuteScalarAsync(cancellationToken)).HasValue;
        }

        var connection = transaction.Connection;
        if (connection is null)
        {
            throw new InvalidOperationException("Ambient transaction has no active connection");
        }

        await using var command = new NpgsqlCommand(_cancelScheduledSql, connection, transaction);
        command.Parameters.AddRange(parameters);
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
