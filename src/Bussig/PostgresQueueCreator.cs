using System.Globalization;
using Bussig.Abstractions;
using Bussig.Configuration;
using Bussig.Constants;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Bussig;

public sealed class PostgresQueueCreator : IQueueCreator
{
    private readonly NpgsqlDataSource _npgsqlDataSource;
    private readonly string _createQueueSql;

    public PostgresQueueCreator(
        [FromKeyedServices(ServiceKeys.BussigNpgsql)] NpgsqlDataSource npgsqlDataSource,
        IOptions<PostgresSettings> options
    )
    {
        _npgsqlDataSource = npgsqlDataSource;
        var settings = options.Value;
        _createQueueSql = string.Format(
            CultureInfo.InvariantCulture,
            PsqlStatements.CreateQueue,
            settings.Schema
        );
    }

    public async Task CreateQueue(IQueue queue, CancellationToken cancellationToken)
    {
        await using var conn = await _npgsqlDataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(_createQueueSql, conn);

        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = queue.Name });
        cmd.Parameters.Add(
            new NpgsqlParameter { Value = queue.MaxDeliveryCount ?? (object)DBNull.Value }
        );

        await cmd.ExecuteScalarAsync(cancellationToken);
    }
}
