using Bussig.Abstractions;
using Npgsql;

namespace Bussig.Postgres;

public class PostgresSettings : IPostgresSettings
{
    public string Schema { get; private set; }

    public PostgresSettings(string connectionString)
    {
        var connectionStringBuilder = new NpgsqlConnectionStringBuilder(connectionString);

        Schema = connectionStringBuilder.SearchPath ?? "bussig";
    }
}
