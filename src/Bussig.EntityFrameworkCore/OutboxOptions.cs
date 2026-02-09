namespace Bussig.EntityFrameworkCore;

public sealed class OutboxOptions
{
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);
    public int BatchSize { get; set; } = 100;
    public TimeSpan PublishedRetention { get; set; } = TimeSpan.FromHours(24);
}
