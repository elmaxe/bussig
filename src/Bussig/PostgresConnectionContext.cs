namespace Bussig;

// public interface IPostgresConnectionContext
// {
//     Task<T> Query<T>(string sql, NpgsqlParameter[] parameters, CancellationToken cancellationToken);
// }
//
// public class PostgresConnectionContext : IPostgresConnectionContext
// {
//     private readonly NpgsqlDataSource _npgsqlDataSource;
//
//     public PostgresConnectionContext(
//         [FromKeyedServices(ServiceKeys.BussigNpgsql)] NpgsqlDataSource npgsqlDataSource,
//         IOptions<PostgresSettings> settings
//     )
//     {
//         _npgsqlDataSource = npgsqlDataSource;
//     }
//
//     public async Task<T> Query<T>(
//         string sql,
//         NpgsqlParameter[] parameters,
//         CancellationToken cancellationToken
//     )
//     {
//         await using var connection = await _npgsqlDataSource.OpenConnectionAsync(cancellationToken);
//         await using var command = new NpgsqlCommand(sql, connection);
//         command.Parameters.AddRange(parameters);
//
//         var result = await command.ExecuteScalarAsync(cancellationToken);
//
//         return (T)result;
//     }
// }
