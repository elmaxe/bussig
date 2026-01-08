using Bussig.Abstractions;
using Npgsql;

namespace Bussig.Postgres.Configuration;

public class PostgresSettings : IPostgresSettings
{
    public string Schema { get; }

    public PostgresSettings(string connectionString, string? schema = null)
    {
        var connectionStringBuilder = new NpgsqlConnectionStringBuilder(connectionString);

        Schema = ResolveSchema(schema, connectionStringBuilder.SearchPath);
    }

    private static string ResolveSchema(string? schema, string? searchPath)
    {
        var resolved = NormalizeSchema(schema) ?? NormalizeSchema(searchPath);

        return resolved ?? TransportConstants.DefaultSchemaName;
    }

    private static string? NormalizeSchema(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var parts = value.Split(',', StringSplitOptions.RemoveEmptyEntries);
        return parts.Length == 0 ? null : parts[0].Trim();
    }
}
