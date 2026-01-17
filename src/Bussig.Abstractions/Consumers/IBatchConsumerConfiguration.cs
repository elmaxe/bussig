namespace Bussig.Abstractions;

public interface IBatchConsumerConfiguration
{
    /// <summary>
    /// If less than <see cref="MessageLimit"/> are delivered within this time limit, end the batch early if at least one message has been received.
    /// </summary>
    TimeSpan TimeLimit { get; }

    /// <summary>
    /// Maximum amount of messages to be delivered in a batch.
    ///
    /// <remarks>Must be less than <see cref="PrefetchCount"/></remarks>
    /// </summary>
    uint MessageLimit { get; }

    /// <summary>
    /// <remarks>Must be greater than <see cref="MessageLimit"/></remarks>
    /// </summary>
    uint PrefetchCount { get; }
}