using System.IO.Compression;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Bussig.Abstractions;
using Bussig.Exceptions;
using Microsoft.Extensions.Options;

namespace Bussig.Azure.Storage;

public sealed class AzureBlobStorageAttachmentRepository : IMessageAttachmentRepository
{
    // TODO: Create container on start
    private readonly AzureBlobStorageAttachmentRepositoryOptions _options;
    private readonly BlobContainerClient _blobContainerClient;

    public AzureBlobStorageAttachmentRepository(
        IOptions<AzureBlobStorageAttachmentRepositoryOptions> options
    )
    {
        _options = options.Value;

        _blobContainerClient = new BlobServiceClient(
            _options.ConnectionString
        ).GetBlobContainerClient(_options.ContainerName);
    }

    public async Task<Uri> PutAsync(Stream stream, CancellationToken cancellationToken = default)
    {
        var blobName = BlobNameGenerator.Generate();
        var blobHttpHeaders = new BlobHttpHeaders();
        if (_options.UseCompression)
        {
            blobName += ".gzip";
            blobHttpHeaders.ContentEncoding = "gzip";
        }
        var blobClient = _blobContainerClient.GetBlobClient(blobName);

        if (_options.UseCompression)
        {
            await using var compressed = new MemoryStream();
            await using (
                var compressor = new GZipStream(
                    compressed,
                    CompressionLevel.Fastest,
                    leaveOpen: true
                )
            )
            {
                await stream.CopyToAsync(compressor, cancellationToken);
            }

            compressed.Position = 0;
            await blobClient.UploadAsync(
                compressed,
                httpHeaders: blobHttpHeaders,
                cancellationToken: cancellationToken
            );

            return blobClient.Uri;
        }

        await blobClient.UploadAsync(stream, cancellationToken);

        return blobClient.Uri;
    }

    public async Task<Stream> GetAsync(Uri address, CancellationToken cancellationToken = default)
    {
        var blobName = new BlobUriBuilder(address).BlobName;

        var blobClient = _blobContainerClient.GetBlobClient(blobName);
        try
        {
            var stream = await blobClient.OpenReadAsync(
                new BlobOpenReadOptions(false),
                cancellationToken
            );

            if (
                _options.UseCompression
                || blobName.EndsWith(".gzip", StringComparison.InvariantCultureIgnoreCase)
            )
            {
                return new GZipStream(stream, CompressionMode.Decompress, false);
            }

            return stream;
        }
        catch (RequestFailedException e)
        {
            throw new MessageDataException(
                $"MessageData content not found for {blobClient.BlobContainerName}/{blobClient.Name}",
                e
            );
        }
    }

    public async Task DeleteAsync(Uri address, CancellationToken cancellationToken = default)
    {
        if (!_options.DeleteConsumedBlobs)
            return;

        var blobName = new BlobUriBuilder(address).BlobName;

        var blobClient = _blobContainerClient.GetBlobClient(blobName);
        await blobClient.DeleteIfExistsAsync(
            DeleteSnapshotsOption.IncludeSnapshots,
            cancellationToken: cancellationToken
        );
    }
}

public static class BlobNameGenerator
{
    public static string Generate() => Guid.NewGuid().ToString();
}
