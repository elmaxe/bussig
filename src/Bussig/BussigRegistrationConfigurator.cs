using System.Reflection;
using Bussig.Abstractions;
using Bussig.Abstractions.Messages;
using Bussig.Abstractions.Middleware;
using Bussig.Abstractions.Options;
using Bussig.Attachments;
using Bussig.Processing;

namespace Bussig;

public class BussigRegistrationConfigurator : IBussigRegistrationConfigurator
{
    private readonly HashSet<Type> _messages = [];
    private readonly Dictionary<Type, QueueOptions> _queueOptions = new();
    private readonly List<ProcessorRegistration> _processorRegistrations = [];
    private readonly List<Type> _globalMiddleware = [];
    private readonly List<Type> _globalSendMiddleware = [];

    public IReadOnlyCollection<Type> Messages => _messages;

    public IReadOnlyList<ProcessorRegistration> ProcessorRegistrations => _processorRegistrations;

    /// <summary>
    /// Gets the global middleware types for all processors.
    /// </summary>
    public IReadOnlyList<Type> GlobalMiddleware => _globalMiddleware;

    /// <summary>
    /// Gets the global middleware types for outgoing messages.
    /// </summary>
    public IReadOnlyList<Type> GlobalSendMiddleware => _globalSendMiddleware;

    /// <summary>
    /// Gets whether attachments are enabled.
    /// </summary>
    public bool AttachmentsEnabled { get; internal set; }

    /// <summary>
    /// Gets the attachment options configuration action.
    /// </summary>
    internal Action<AttachmentOptions>? ConfigureAttachmentOptions { get; set; }

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

    public void AddProcessor<TMessage, TProcessor>(Action<ProcessorOptions>? configure = null)
        where TMessage : class, IMessage
        where TProcessor : class, IProcessor<TMessage>
    {
        AddProcessor(typeof(TMessage), typeof(TProcessor), configure);
    }

    public void AddProcessor(
        Type messageType,
        Type processorType,
        Action<ProcessorOptions>? configure = null
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

        var options = new ProcessorOptions();
        configure?.Invoke(options);

        if (typeof(ISingletonProcessor).IsAssignableFrom(processorType))
        {
            options.Polling.SingletonProcessing.EnableSingletonProcessing = true;
        }

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

    public void AddProcessor<TProcessor>(Action<ProcessorOptions>? configure = null)
        where TProcessor : class, IProcessor
    {
        AddProcessor(typeof(TProcessor), configure);
    }

    public void AddProcessor(Type processorType, Action<ProcessorOptions>? configure = null)
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
        Action<ProcessorOptions>? defaultConfigure = null
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

    public void UseMiddleware<TMiddleware>()
        where TMiddleware : class, IMessageMiddleware
    {
        UseMiddleware(typeof(TMiddleware));
    }

    public void UseMiddleware(Type middlewareType)
    {
        if (!typeof(IMessageMiddleware).IsAssignableFrom(middlewareType))
        {
            throw new ArgumentException(
                $"Type {middlewareType.Name} must implement {nameof(IMessageMiddleware)}",
                nameof(middlewareType)
            );
        }

        _globalMiddleware.Add(middlewareType);
    }

    public void UseSendMiddleware<TMiddleware>()
        where TMiddleware : class, IOutgoingMessageMiddleware
    {
        UseSendMiddleware(typeof(TMiddleware));
    }

    public void UseSendMiddleware(Type middlewareType)
    {
        if (!typeof(IOutgoingMessageMiddleware).IsAssignableFrom(middlewareType))
        {
            throw new ArgumentException(
                $"Type {middlewareType.Name} must implement {nameof(IOutgoingMessageMiddleware)}",
                nameof(middlewareType)
            );
        }

        _globalSendMiddleware.Add(middlewareType);
    }
}
