using System.Collections;
using Bussig.Abstractions;
using Bussig.Abstractions.Messages;

namespace Bussig.Processing;

public sealed class MessageBatch<TMessage> : Batch<TMessage>
    where TMessage : class, IMessage
{
    private readonly List<MessageProcessorContext<TMessage>> _contexts;

    public MessageBatch(IEnumerable<MessageProcessorContext<TMessage>> contexts)
    {
        _contexts = contexts.ToList();
    }

    public int Length => _contexts.Count;

    internal IReadOnlyList<MessageProcessorContext<TMessage>> Contexts => _contexts;

    public IEnumerator<ProcessorContext<TMessage>> GetEnumerator() => _contexts.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
