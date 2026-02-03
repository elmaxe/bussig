using System.Diagnostics.CodeAnalysis;

namespace Bussig;

public sealed class MessageData
{
    public Uri? Address { get; init; }
    private Stream? Data { get; set; }

    [MemberNotNullWhen(true, nameof(Data))]
    public bool HasData => Data is not null;

    public MessageData() { }

    public MessageData(Stream stream) => Data = stream;

    /// <summary>
    /// Opens the attachment data stream for reading.
    /// </summary>
    /// <returns>The attachment data stream.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no data is available.</exception>
    public Stream OpenRead() =>
        Data ?? throw new InvalidOperationException("No attachment data available.");

    internal void SetData(Stream data) => Data = data;

    internal Stream? GetData() => Data;
}
