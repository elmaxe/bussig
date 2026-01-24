namespace Bussig.Abstractions;

public record MessageSendOptions
{
    public Guid? MessageId { get; init; }
    public TimeSpan? Delay { get; init; }
    public int? Priority { get; init; }
    public int MessageVersion { get; init; }
    public Guid? SchedulingToken { get; init; }
    public Dictionary<string, object> Headers { get; init; } = new();
}
