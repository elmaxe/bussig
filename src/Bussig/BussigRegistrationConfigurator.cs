using System.Reflection;
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
        // Get processor interface info including response type and batch info
        var info = GetProcessorTypes(processorType);

        // Validate processor type implements IProcessor<TMessage> or IProcessor<TMessage, TSend>
        var singleInterface = typeof(IProcessor<>).MakeGenericType(messageType);
        var dualInterfaceValid =
            info.ResponseType is not null
            && typeof(IProcessor<,>)
                .MakeGenericType(messageType, info.ResponseType)
                .IsAssignableFrom(processorType);

        if (!singleInterface.IsAssignableFrom(processorType) && !dualInterfaceValid)
        {
            throw new ArgumentException(
                $"Processor type {processorType.Name} must implement IProcessor<{messageType.Name}> or IProcessor<{messageType.Name}, TSend>",
                nameof(processorType)
            );
        }

        // For batch processors, use the inner message type for queue name
        var queueMessageType =
            info.IsBatchProcessor && info.BatchMessageType is not null
                ? info.BatchMessageType
                : messageType;

        // Also register the message type for queue creation
        AddMessage(queueMessageType);

        var options = new ConsumerOptions();
        configure?.Invoke(options);

        var queueName = MessageUrn.ForType(queueMessageType).ToString();

        _processorRegistrations.Add(
            new ProcessorRegistration
            {
                MessageType = messageType,
                ProcessorType = processorType,
                QueueName = queueName,
                Options = options,
                ResponseMessageType = info.ResponseType,
                IsBatchProcessor = info.IsBatchProcessor,
                BatchMessageType = info.BatchMessageType,
            }
        );
    }

    public void AddProcessor<TProcessor>(Action<ConsumerOptions>? configure = null)
        where TProcessor : class, IProcessor
    {
        AddProcessor(typeof(TProcessor), configure);
    }

    public void AddProcessor(Type processorType, Action<ConsumerOptions>? configure = null)
    {
        var info = GetProcessorTypes(processorType);
        if (info.MessageType is null)
        {
            throw new ArgumentException(
                $"Type {processorType.Name} does not implement IProcessor<TMessage> or IProcessor<TMessage, TSend>",
                nameof(processorType)
            );
        }

        AddProcessor(info.MessageType, processorType, configure);
    }

    public void AddProcessorsFromAssembly(
        Assembly assembly,
        Action<ConsumerOptions>? defaultConfigure = null
    )
    {
        var processorTypes = assembly
            .GetTypes()
            .Where(t =>
                t is { IsClass: true, IsAbstract: false } && typeof(IProcessor).IsAssignableFrom(t)
            );

        foreach (var processorType in processorTypes)
        {
            AddProcessor(processorType, defaultConfigure);
        }
    }

    private sealed record ProcessorTypeInfo(
        Type? MessageType,
        Type? ResponseType,
        bool IsBatchProcessor,
        Type? BatchMessageType
    );

    private static ProcessorTypeInfo GetProcessorTypes(Type processorType)
    {
        // Check for IProcessor<TMessage, TSend> first (more specific)
        var dualInterface = processorType
            .GetInterfaces()
            .FirstOrDefault(i =>
                i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IProcessor<,>)
            );

        if (dualInterface is not null)
        {
            var args = dualInterface.GetGenericArguments();
            var messageArg = args[0];

            // Check if the message type is Batch<TMessage>
            if (
                messageArg.IsGenericType
                && messageArg.GetGenericTypeDefinition() == typeof(Batch<>)
            )
            {
                var batchMessageType = messageArg.GetGenericArguments()[0];
                return new ProcessorTypeInfo(messageArg, args[1], true, batchMessageType);
            }

            return new ProcessorTypeInfo(args[0], args[1], false, null);
        }

        // Check for IProcessor<TMessage>
        var singleInterface = processorType
            .GetInterfaces()
            .FirstOrDefault(i =>
                i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IProcessor<>)
            );

        if (singleInterface is not null)
        {
            var messageType = singleInterface.GetGenericArguments()[0];

            // Check if the message type is Batch<TMessage>
            if (
                messageType.IsGenericType
                && messageType.GetGenericTypeDefinition() == typeof(Batch<>)
            )
            {
                var batchMessageType = messageType.GetGenericArguments()[0];
                return new ProcessorTypeInfo(messageType, null, true, batchMessageType);
            }

            return new ProcessorTypeInfo(messageType, null, false, null);
        }

        return new ProcessorTypeInfo(null, null, false, null);
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
