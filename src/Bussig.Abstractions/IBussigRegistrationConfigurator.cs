namespace Bussig.Abstractions;

public interface IBussigRegistrationConfigurator
{
    string ConnectionString { get; set; } // TODO: NpgsqlDataSourceBuilder
    string? Schema { get; set; }
    IPostgresSettings? Settings { get; set; }
    bool CreateQueuesOnStartup { get; set; }
    IReadOnlyCollection<Type> Messages { get; }

    void AddMessage<TMessage>(Action<QueueOptions>? configure = null);
    void AddMessage(Type message, Action<QueueOptions>? configure = null);
    bool TryGetQueueOptions(Type message, out QueueOptions options);
}
