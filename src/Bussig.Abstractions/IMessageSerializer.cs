namespace Bussig.Abstractions;

public interface IMessageSerializer
{
    /// <summary>
    /// Returns UTF-8 encoded JSON bytes for the message.
    /// </summary>
    byte[] SerializeToUtf8Bytes<T>(T message);
}
