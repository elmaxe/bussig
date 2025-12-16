namespace Bussig.Abstractions;

public interface IEnvelope
{
    IEnumerable<string> MessageTypes { get; }
    Dictionary<string, object> Headers { get; }
}
