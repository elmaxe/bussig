using Bussig.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace Bussig.Outbox.Npgsql;

internal sealed class NpgsqlOutboxSender : IOutgoingMessageSender
{
    private readonly IOutgoingMessageSender _innerSender;
    private readonly NpgsqlOutboxTransactionContext _outboxContext;
    private readonly string _insertSql;
    private readonly string _cancelSql;

    public NpgsqlOutboxSender(
        [FromKeyedServices(OutboxServiceKeys.InnerSender)] IOutgoingMessageSender innerSender,
        NpgsqlOutboxTransactionContext outboxContext,
        IOptions<NpgsqlOutboxOptions> options
    )
    {
        _innerSender = innerSender;
        _outboxContext = outboxContext;
        _insertSql = OutboxSqlStatements.Insert(options.Value.Schema);
        _cancelSql = OutboxSqlStatements.CancelBySchedulingToken(options.Value.Schema);
    }

    public async Task SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        if (!_outboxContext.IsActive)
        {
            await _innerSender.SendAsync(message, cancellationToken);
            return;
        }

        var conn = _outboxContext.Connection;
        var tx = _outboxContext.Transaction;

        await using var cmd = new NpgsqlCommand(_insertSql, conn, tx);
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = message.MessageId });
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = message.QueueName });
        cmd.Parameters.Add(new NpgsqlParameter<byte[]> { TypedValue = message.Body });
        cmd.Parameters.Add(
            new NpgsqlParameter<string?>
            {
                TypedValue = message.HeadersJson,
                NpgsqlDbType = NpgsqlDbType.Text,
            }
        );
        cmd.Parameters.Add(new NpgsqlParameter<short?> { TypedValue = message.Priority });
        cmd.Parameters.Add(
            new NpgsqlParameter<TimeSpan?> { TypedValue = message.Delay ?? TimeSpan.Zero }
        );
        cmd.Parameters.Add(new NpgsqlParameter<int> { TypedValue = message.MessageVersion });
        cmd.Parameters.Add(
            new NpgsqlParameter<DateTimeOffset?> { TypedValue = message.ExpirationTime }
        );
        cmd.Parameters.Add(new NpgsqlParameter<Guid?> { TypedValue = message.SchedulingTokenId });

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<bool> CancelAsync(Guid schedulingToken, CancellationToken cancellationToken)
    {
        if (_outboxContext.IsActive)
        {
            var conn = _outboxContext.Connection;
            var tx = _outboxContext.Transaction;

            await using var cmd = new NpgsqlCommand(_cancelSql, conn, tx);
            cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = schedulingToken });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);

            if (result is not null)
            {
                return true;
            }
        }

        return await _innerSender.CancelAsync(schedulingToken, cancellationToken);
    }
}
