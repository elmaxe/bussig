using System.Text.Json;
using Bussig.Abstractions;

namespace Bussig.Serialization;

public sealed class SystemTextJsonMessageSerializer : IMessageSerializer
{
    private readonly JsonSerializerOptions _options;

    public SystemTextJsonMessageSerializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);
    }

    public byte[] SerializeToUtf8Bytes<T>(T message) =>
        JsonSerializer.SerializeToUtf8Bytes(message, _options);

    public T? Deserialize<T>(byte[] data) => JsonSerializer.Deserialize<T>(data, _options);

    public object? Deserialize(byte[] data, Type type) =>
        JsonSerializer.Deserialize(data, type, _options);
}
