using System.Globalization;
using Bussig.Abstractions;
using Microsoft.Extensions.Options;

namespace Bussig.Postgres;

public sealed class PostgresQueueCreator : IQueueCreator
{
    private readonly string _createQueueSql;

    public PostgresQueueCreator(IOptions<PostgresSettings> options)
    {
        var settings = options.Value;
        _createQueueSql = string.Format(
            CultureInfo.InvariantCulture,
            PsqlStatements.CreateQueue,
            settings.Schema
        );
    }

    public Task CreateQueue(IQueue queue, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
        // await _postgresConnectionContext.Query<long>(
        //     _createQueueSql,
        //     [
        //         new NpgsqlParameter<string> { TypedValue = queue.Name },
        //         new NpgsqlParameter<int?> { TypedValue = queue.MaxDeliveryCount },
        //     ],
        //     cancellationToken
        // );
    }
}
