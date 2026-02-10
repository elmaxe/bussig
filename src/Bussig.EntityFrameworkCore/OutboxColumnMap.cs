namespace Bussig.EntityFrameworkCore;

public sealed record OutboxColumnMap
{
    public required string Id { get; init; }
    public required string MessageId { get; init; }
    public required string QueueName { get; init; }
    public required string Body { get; init; }
    public required string HeadersJson { get; init; }
    public required string Priority { get; init; }
    public required string Delay { get; init; }
    public required string MessageVersion { get; init; }
    public required string ExpirationTime { get; init; }
    public required string SchedulingTokenId { get; init; }
    public required string CreatedAt { get; init; }
    public required string PublishedAt { get; init; }
}
