using Bussig.Abstractions.Options;

namespace Bussig.Processing;

/// <summary>
/// Represents a registered processor with its configuration.
/// </summary>
public sealed record ProcessorRegistration
{
    public required Type MessageType { get; init; }
    public required Type ProcessorType { get; init; }
    public required string QueueName { get; init; }
    public required ProcessorOptions Options { get; init; }

    /// <summary>
    /// The response message type if the processor is IProcessor&lt;TMessage, TSend&gt;.
    /// Null for fire-and-forget processors (IProcessor&lt;TMessage&gt;).
    /// </summary>
    public Type? ResponseMessageType { get; init; }

    /// <summary>
    /// Whether the processor is a batch processor (IProcessor&lt;Batch&lt;TMessage&gt;&gt;).
    /// </summary>
    public bool IsBatchProcessor { get; init; }

    /// <summary>
    /// The inner message type for batch processors (the TMessage in Batch&lt;TMessage&gt;).
    /// Null for non-batch processors.
    /// </summary>
    public Type? BatchMessageType { get; init; }
}
