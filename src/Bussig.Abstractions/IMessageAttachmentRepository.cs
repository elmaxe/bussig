namespace Bussig.Abstractions;

public interface IMessageAttachmentRepository
{
    Task<Uri> PutAsync(Stream stream, CancellationToken cancellationToken = default);
    Task<Stream> GetAsync(Uri address, CancellationToken cancellationToken = default);
}
