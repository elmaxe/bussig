using Npgsql;

namespace Bussig.Postgres;

public class PostgresSettings
{
    public string Schema { get; private set; }

    public PostgresSettings(string connectionString)
    {
        var connectionStringBuilder = new NpgsqlConnectionStringBuilder(connectionString);

        Schema = connectionStringBuilder.SearchPath ?? "bussig";
    }
}
