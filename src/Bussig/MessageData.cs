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

    internal void SetData(Stream data) => Data = data;

    internal Stream? GetData() => Data;
}
