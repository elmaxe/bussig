using System.IO.Compression;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Bussig.Abstractions;
using Bussig.Abstractions.Middleware;
using Bussig.Exceptions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Bussig.Azure.Storage;

public sealed class AzureBlobStorageAttachmentRepository
    : IMessageAttachmentRepository,
        IBusObserver
{
    private readonly AzureBlobStorageAttachmentRepositoryOptions _options;
    private readonly IBlobNameGenerator _blobNameGenerator;
    private readonly BlobContainerClient _blobContainerClient;
    private readonly ILogger<AzureBlobStorageAttachmentRepository> _logger;

    public AzureBlobStorageAttachmentRepository(
        IOptions<AzureBlobStorageAttachmentRepositoryOptions> options,
        IBlobNameGenerator blobNameGenerator,
        ILogger<AzureBlobStorageAttachmentRepository> logger
    )
    {
        _blobNameGenerator = blobNameGenerator;
        _logger = logger;
        _options = options.Value;

        var blobServiceClient = _options switch
        {
            { TokenCredential: not null, StorageAccountName: not null } => new BlobServiceClient(
                new Uri($"https://{_options.StorageAccountName}.blob.core.windows.net"),
                _options.TokenCredential,
                _options.BlobClientOptions
            ),
            { ConnectionString: not null } => new BlobServiceClient(
                _options.ConnectionString,
                _options.BlobClientOptions
            ),
            _ => throw new BussigConfigurationException(
                "Invalid Azure Blob Service configuration. Use ConnectionString or EntraId"
            ),
        };
        _blobContainerClient = blobServiceClient.GetBlobContainerClient(_options.ContainerName);
    }

    public async Task PreStartAsync()
    {
        if (!_options.CreateContainerOnStartup)
        {
            _logger.LogInformation(
                "Skipping attachment container creation for container {ContainerName}",
                _options.ContainerName
            );
            return;
        }

        try
        {
            if (!await _blobContainerClient.ExistsAsync())
            {
                await _blobContainerClient.CreateIfNotExistsAsync();
            }
        }
        catch (Exception e)
        {
            _logger.LogError(
                e,
                "Failed to create container {ContainerName}",
                _options.ContainerName
            );
        }
    }

    public Task PostStartAsync()
    {
        return Task.CompletedTask;
    }

    public Task PreStopAsync()
    {
        return Task.CompletedTask;
    }

    public Task PostStopAsync()
    {
        return Task.CompletedTask;
    }

    public async Task<Uri> PutAsync(
        Stream stream,
        OutgoingMessageContext messageContext,
        CancellationToken cancellationToken = default
    )
    {
        var blobName = _blobNameGenerator.Generate(messageContext);
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
