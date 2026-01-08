using Bussig.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Bussig.Postgres;

public interface IPostgresConnectionContext
{
    Task<T> Query<T>(string sql, NpgsqlParameter[] parameters, CancellationToken cancellationToken);
}

public class PostgresConnectionContext : IPostgresConnectionContext
{
    private readonly NpgsqlDataSource _npgsqlDataSource;
    public readonly IPostgresSettings Settings;

    public PostgresConnectionContext(
        [FromKeyedServices(BussigServiceKeys.BussigNpgsql)] NpgsqlDataSource npgsqlDataSource,
        IPostgresSettings settings
    )
    {
        _npgsqlDataSource = npgsqlDataSource;
        Settings = settings;
    }

    public async Task<T> Query<T>(
        string sql,
        NpgsqlParameter[] parameters,
        CancellationToken cancellationToken
    )
    {
        await using var connection = await _npgsqlDataSource.OpenConnectionAsync(cancellationToken);
        var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddRange(parameters);

        var result = await command.ExecuteScalarAsync(cancellationToken);

        await transaction.CommitAsync(cancellationToken);

        return (T)result;
    }
}
