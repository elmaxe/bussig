namespace Bussig.Abstractions;

public sealed class OutgoingMessage
{
    public string QueueName { get; }
    public byte[] Body { get; }
    public string HeadersJson { get; }

    public Guid MessageId { get; init; }
    public short? Priority { get; init; }
    public TimeSpan? Delay { get; init; }
    public int MessageVersion { get; init; }
    public DateTimeOffset? ExpirationTime { get; init; }
    public Guid? SchedulingTokenId { get; init; }

    public OutgoingMessage(Guid messageId, string queueName, byte[] body, string headersJson)
    {
        if (string.IsNullOrWhiteSpace(queueName))
        {
            throw new ArgumentException("Queue name cannot be empty.", nameof(queueName));
        }

        MessageId = messageId;
        QueueName = queueName;
        Body = body ?? throw new ArgumentNullException(nameof(body));
        HeadersJson = headersJson ?? throw new ArgumentNullException(nameof(headersJson));
    }
}
