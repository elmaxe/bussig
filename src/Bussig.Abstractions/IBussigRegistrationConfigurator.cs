namespace Bussig.Abstractions;

public interface IBussigRegistrationConfigurator
{
    string ConnectionString { get; set; } // TODO: NpgsqlDataSourceBuilder
    IPostgresSettings? Settings { get; set; }

    void AddMessage<TMessage>();
    void AddMessage(Type message);
}
