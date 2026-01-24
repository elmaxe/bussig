using Bussig.Abstractions;

namespace Bussig.Processing;

/// <summary>
/// Represents a registered processor with its configuration.
/// </summary>
public sealed record ProcessorRegistration
{
    public required Type MessageType { get; init; }
    public required Type ProcessorType { get; init; }
    public required string QueueName { get; init; }
    public required ConsumerOptions Options { get; init; }
}
