using Npgsql;

namespace Bussig.Configuration;

public sealed class PostgresSettings
{
    public string Host { get; set; } = null!;
    public int? Port { get; set; }
    public string Username { get; set; } = null!;
    public string Password { get; set; } = null!;
    public string Database { get; set; } = null!;
    public string Schema { get; set; } = null!;
    public string ConnectionString { get; set; } = null!;

    public PostgresSettings Apply()
    {
        var builder = new NpgsqlConnectionStringBuilder(ConnectionString);

        if (!string.IsNullOrWhiteSpace(Database))
            builder.Database = Database;
        else if (!string.IsNullOrWhiteSpace(builder.Database))
            Database = builder.Database;

        if (!string.IsNullOrWhiteSpace(Host))
            builder.Host = Host;
        else if (!string.IsNullOrWhiteSpace(builder.Host))
            Host = builder.Host;

        if (!string.IsNullOrWhiteSpace(Username))
            builder.Username = Username;
        else if (!string.IsNullOrWhiteSpace(builder.Username))
            Username = builder.Username;

        if (!string.IsNullOrWhiteSpace(Password))
            builder.Password = Password;
        else if (!string.IsNullOrWhiteSpace(builder.Password))
            Password = builder.Password;

        if (Port.HasValue)
            builder.Port = Port.Value;
        else
            Port = builder.Port;

        if (string.IsNullOrWhiteSpace(Schema))
        {
            Schema = builder.SearchPath ?? TransportConstants.DefaultSchemaName;
            builder.SearchPath = Schema;
        }

        ConnectionString = builder.ToString();

        return this;
    }
}

public static class PostgresSettingsFactory
{
    public static PostgresSettings Build(PostgresSettings settings) => settings.Apply();
}
