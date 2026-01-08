using Bussig.Abstractions;

namespace Bussig;

public class BussigRegistrationConfigurator : IBussigRegistrationConfigurator
{
    public string ConnectionString { get; set; } = null!;
    public IPostgresSettings? Settings { get; set; } = null;
    public readonly HashSet<Type> Messages = [];

    public void AddMessage<TMessage>()
    {
        Messages.Add(typeof(TMessage));
    }

    public void AddMessage(Type message)
    {
        Messages.Add(message);
    }
}
