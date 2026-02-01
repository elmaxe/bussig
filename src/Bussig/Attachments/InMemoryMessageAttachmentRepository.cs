using System.Collections.Concurrent;
using Bussig.Abstractions;
using Bussig.Exceptions;

namespace Bussig.Attachments;

public sealed class InMemoryMessageAttachmentRepository : IMessageAttachmentRepository
{
    private readonly ConcurrentDictionary<Uri, byte[]> _attachments = new();

    public async Task<Uri> PutAsync(Stream stream, CancellationToken cancellationToken = default)
    {
        var uri = new Uri($"https://{Guid.NewGuid().ToString()}.se");
        var ms = new MemoryStream();
        await stream.CopyToAsync(ms, cancellationToken);
        _attachments.TryAdd(uri, ms.ToArray());
        return uri;
    }

    public Task<Stream> GetAsync(Uri address, CancellationToken cancellationToken = default)
    {
        if (_attachments.TryGetValue(address, out var value))
        {
            var ms = new MemoryStream(value);
            return Task.FromResult<Stream>(ms);
        }

        throw new MessageDataException($"MessageData content not found for {address.ToString()}");
    }

    public Task DeleteAsync(Uri address, CancellationToken cancellationToken = default)
    {
        _attachments.TryRemove(address, out _);
        return Task.CompletedTask;
    }
}
