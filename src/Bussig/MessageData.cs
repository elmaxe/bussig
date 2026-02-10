using System.Diagnostics.CodeAnalysis;

namespace Bussig;

[SuppressMessage("Design", "CA1001", Justification = "Stream lifecycle managed by middleware")]
public sealed class MessageData
{
    public Uri? Address { get; init; }
    public byte[]? InlineData { get; init; }

    private Stream? _stream;
    private Func<CancellationToken, Task<Stream>>? _valueFactory;
    private Task<Stream>? _cachedValue;

    public MessageData() { }

    public MessageData(Stream stream) => _stream = stream;

    public MessageData(byte[] data) => _stream = new MemoryStream(data);

    public bool HasValue =>
        _stream is not null || _valueFactory is not null || InlineData is not null;

    public Task<Stream> GetValueAsync(CancellationToken ct = default)
    {
        if (_cachedValue is not null)
            return _cachedValue;
        if (_stream is not null)
            return Task.FromResult(_stream);
        if (InlineData is not null)
            return _cachedValue = Task.FromResult<Stream>(new MemoryStream(InlineData));
        if (_valueFactory is not null)
            return _cachedValue = _valueFactory(ct);
        throw new InvalidOperationException("No attachment data available.");
    }

    internal Stream? GetSendStream() => _stream;

    internal void SetValueFactory(Func<CancellationToken, Task<Stream>> factory) =>
        _valueFactory = factory;

    internal bool WasAccessed => _cachedValue is not null;

    internal Task<Stream>? GetCachedValue() => _cachedValue;
}
