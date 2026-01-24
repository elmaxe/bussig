using Bussig.Abstractions;
using Bussig.Processing;

namespace Bussig;

public class BussigRegistrationConfigurator : IBussigRegistrationConfigurator
{
    private readonly HashSet<Type> _messages = [];
    private readonly Dictionary<Type, QueueOptions> _queueOptions = new();
    private readonly List<ProcessorRegistration> _processorRegistrations = [];

    public IReadOnlyCollection<Type> Messages => _messages;

    public IReadOnlyList<ProcessorRegistration> ProcessorRegistrations => _processorRegistrations;

    public void AddMessage<TMessage>(Action<QueueOptions>? configure = null)
    {
        AddMessage(typeof(TMessage), configure);
    }

    public void AddMessage(Type message, Action<QueueOptions>? configure = null)
    {
        _messages.Add(message);
        configure?.Invoke(GetOrCreateOptions(message));
    }

    public bool TryGetQueueOptions(Type message, out QueueOptions options)
    {
        return _queueOptions.TryGetValue(message, out options!);
    }

    public void AddProcessor<TMessage, TProcessor>(Action<ConsumerOptions>? configure = null)
        where TMessage : class
        where TProcessor : class, IProcessor<TMessage>
    {
        AddProcessor(typeof(TMessage), typeof(TProcessor), configure);
    }

    public void AddProcessor(
        Type messageType,
        Type processorType,
        Action<ConsumerOptions>? configure = null
    )
    {
        // Validate processor type implements IProcessor<TMessage>
        var processorInterface = typeof(IProcessor<>).MakeGenericType(messageType);
        if (!processorInterface.IsAssignableFrom(processorType))
        {
            throw new ArgumentException(
                $"Processor type {processorType.Name} must implement IProcessor<{messageType.Name}>",
                nameof(processorType)
            );
        }

        // Also register the message
        AddMessage(messageType);

        var options = new ConsumerOptions();
        configure?.Invoke(options);

        var queueName = MessageUrn.ForType(messageType).ToString();

        _processorRegistrations.Add(
            new ProcessorRegistration
            {
                MessageType = messageType,
                ProcessorType = processorType,
                QueueName = queueName,
                Options = options,
            }
        );
    }

    private QueueOptions GetOrCreateOptions(Type message)
    {
        if (_queueOptions.TryGetValue(message, out var options))
        {
            return options;
        }

        options = new QueueOptions();
        _queueOptions[message] = options;
        return options;
    }
}
