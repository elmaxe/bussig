using Bussig.Abstractions.Options;

namespace Bussig.Processing.Internal;

/// <summary>
/// Value object that groups processor configuration.
/// </summary>
internal sealed record ProcessorConfiguration
{
    public required string QueueName { get; init; }
    public required Type MessageType { get; init; }
    public required Type ProcessorType { get; init; }
    public required Type? ResponseMessageType { get; init; }
    public required Type? BatchMessageType { get; init; }
    public required ProcessorOptions Options { get; init; }

    /// <summary>
    /// Global middleware types to run for all processors (both single-message and batch).
    /// </summary>
    public IReadOnlyList<Type> GlobalMiddleware { get; init; } = [];
}
