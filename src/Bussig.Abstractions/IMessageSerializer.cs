namespace Bussig.Abstractions;

public interface IMessageSerializer
{
    /// <summary>
    /// Returns UTF-8 encoded JSON bytes for the message.
    /// </summary>
    byte[] SerializeToUtf8Bytes<T>(T message);

    /// <summary>
    /// Deserializes UTF-8 encoded JSON bytes to a message of type T.
    /// </summary>
    T? Deserialize<T>(byte[] data);

    /// <summary>
    /// Deserializes UTF-8 encoded JSON bytes to a message of the specified type.
    /// </summary>
    object? Deserialize(byte[] data, Type type);
}
