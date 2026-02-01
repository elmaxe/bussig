namespace Bussig.Processing.Internal;

/// <summary>
/// Strategy interface for message processing (single vs batch).
/// </summary>
internal interface IMessageProcessingStrategy
{
    Task PollAsync(CancellationToken stoppingToken);
}
