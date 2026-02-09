namespace Bussig.Outbox.Npgsql;

public sealed class NpgsqlOutboxOptions
{
    public string ConnectionString { get; set; } = null!;
    public string Schema { get; set; } = "public";
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);
    public int BatchSize { get; set; } = 100;
    public TimeSpan PublishedRetention { get; set; } = TimeSpan.FromHours(24);
}
