namespace Bussig.EntityFrameworkCore;

public sealed class OutboxMessage
{
    public long Id { get; set; }
    public Guid MessageId { get; set; }
    public string QueueName { get; set; } = null!;
    public byte[] Body { get; set; } = null!;
    public string? HeadersJson { get; set; }
    public short? Priority { get; set; }
    public TimeSpan? Delay { get; set; }
    public int MessageVersion { get; set; } = 1;
    public DateTimeOffset? ExpirationTime { get; set; }
    public Guid? SchedulingTokenId { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? PublishedAt { get; set; }
}
