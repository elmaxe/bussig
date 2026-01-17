using Bussig.Abstractions;
using Npgsql;

namespace Bussig.Postgres;

public sealed class PostgresQueueCreator : IQueueCreator
{
    private readonly PostgresConnectionContext _postgresConnectionContext;
    private readonly string _createQueueSql;

    public PostgresQueueCreator(PostgresConnectionContext postgresConnectionContext)
    {
        _postgresConnectionContext = postgresConnectionContext;

        _createQueueSql = string.Format(
            PsqlStatements.CreateQueue,
            _postgresConnectionContext.Settings.Schema
        );
    }

    public async Task CreateQueue(IQueue queue, CancellationToken cancellationToken)
    {
        await _postgresConnectionContext.Query<long>(
            _createQueueSql,
            [
                new NpgsqlParameter<string> { TypedValue = queue.Name },
                new NpgsqlParameter<int?> { TypedValue = queue.MaxDeliveryCount },
            ],
            cancellationToken
        );
    }
}
