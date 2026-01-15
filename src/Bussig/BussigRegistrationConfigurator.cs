using Bussig.Abstractions;

namespace Bussig;

public class BussigRegistrationConfigurator : IBussigRegistrationConfigurator
{
    private readonly HashSet<Type> _messages = [];
    private readonly Dictionary<Type, QueueOptions> _queueOptions = new();

    public string ConnectionString { get; set; } = null!;
    public string? Schema { get; set; }
    public IPostgresSettings? Settings { get; set; } = null;
    public bool CreateQueuesOnStartup { get; set; } = true;
    public IReadOnlyCollection<Type> Messages => _messages;

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
