using Azure.Storage.Blobs;
using Bussig.Abstractions;
using Bussig.Abstractions.Middleware;
using Bussig.Exceptions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Testcontainers.Azurite;

namespace Bussig.Azure.Storage.Tests.Integration;

public class AzureBlobStorageAttachmentRepositoryTests
{
    private AzuriteContainer _container = null!;
    private BlobContainerClient _containerClient = null!;
    private const string ContainerName = "test-attachments";

    [Before(Test)]
    public async Task Setup()
    {
        _container = new AzuriteBuilder()
            .WithImage("mcr.microsoft.com/azure-storage/azurite:latest")
            .WithCommand("--skipApiVersionCheck")
            .Build();

        await _container.StartAsync();

        var blobServiceClient = new BlobServiceClient(_container.GetConnectionString());
        _containerClient = blobServiceClient.GetBlobContainerClient(ContainerName);
        await _containerClient.CreateIfNotExistsAsync();
    }

    [After(Test)]
    public async Task Cleanup()
    {
        await _container.DisposeAsync();
    }

    [Test]
    public async Task PutAsync_UploadsBlobAndReturnsUri()
    {
        // Arrange
        var repository = CreateRepository();
        var content = "test content for upload"u8.ToArray();
        var stream = new MemoryStream(content);

        // Act
        var uri = await repository.PutAsync(stream, CreateMessageContext());

        // Assert
        await Assert.That(uri).IsNotNull();
        await Assert.That(uri.ToString()).Contains(ContainerName);

        // Verify the blob exists
        var blobName = new BlobUriBuilder(uri).BlobName;
        var blobClient = _containerClient.GetBlobClient(blobName);
        var exists = await blobClient.ExistsAsync();
        await Assert.That(exists.Value).IsTrue();
    }

    [Test]
    public async Task GetAsync_RetrievesUploadedContent()
    {
        // Arrange
        var repository = CreateRepository();
        var originalContent = "content for retrieval test"u8.ToArray();
        var inputStream = new MemoryStream(originalContent);

        var uri = await repository.PutAsync(inputStream, CreateMessageContext());

        // Act
        var retrievedStream = await repository.GetAsync(uri);

        // Assert
        using var memoryStream = new MemoryStream();
        await retrievedStream.CopyToAsync(memoryStream);
        var retrievedContent = memoryStream.ToArray();

        await Assert.That(retrievedContent).IsEquivalentTo(originalContent);
    }

    [Test]
    public async Task GetAsync_ThrowsMessageDataException_WhenBlobNotFound()
    {
        // Arrange
        var repository = CreateRepository();
        var nonExistentUri = new Uri($"{_containerClient.Uri}/nonexistent-blob");

        // Act & Assert
        await Assert
            .That(async () => await repository.GetAsync(nonExistentUri))
            .Throws<MessageDataException>();
    }

    [Test]
    public async Task DeleteAsync_RemovesBlob_WhenDeleteConsumedBlobsIsTrue()
    {
        // Arrange
        var repository = CreateRepository(deleteConsumedBlobs: true);
        var content = "content to delete"u8.ToArray();
        var stream = new MemoryStream(content);

        var uri = await repository.PutAsync(stream, CreateMessageContext());

        // Verify it exists
        var blobName = new BlobUriBuilder(uri).BlobName;
        var blobClient = _containerClient.GetBlobClient(blobName);
        var existsBefore = await blobClient.ExistsAsync();
        await Assert.That(existsBefore.Value).IsTrue();

        // Act
        await repository.DeleteAsync(uri);

        // Assert
        var existsAfter = await blobClient.ExistsAsync();
        await Assert.That(existsAfter.Value).IsFalse();
    }

    [Test]
    public async Task DeleteAsync_KeepsBlob_WhenDeleteConsumedBlobsIsFalse()
    {
        // Arrange
        var repository = CreateRepository(deleteConsumedBlobs: false);
        var content = "content to keep"u8.ToArray();
        var stream = new MemoryStream(content);

        var uri = await repository.PutAsync(stream, CreateMessageContext());

        // Act
        await repository.DeleteAsync(uri);

        // Assert - Blob should still exist
        var blobName = new BlobUriBuilder(uri).BlobName;
        var blobClient = _containerClient.GetBlobClient(blobName);
        var exists = await blobClient.ExistsAsync();
        await Assert.That(exists.Value).IsTrue();
    }

    [Test]
    public async Task DeleteAsync_DoesNotThrow_WhenBlobDoesNotExist()
    {
        // Arrange
        var repository = CreateRepository(deleteConsumedBlobs: true);
        var nonExistentUri = new Uri($"{_containerClient.Uri}/nonexistent-blob");

        // Act & Assert - Should not throw
        await Assert.That(async () => await repository.DeleteAsync(nonExistentUri)).ThrowsNothing();
    }

    [Test]
    public async Task PutAsync_WithCompression_UploadsGzippedBlob()
    {
        // Arrange
        var repository = CreateRepository(useCompression: true);
        var content =
            "content to compress - should be repeated to compress better content to compress"u8.ToArray();
        var stream = new MemoryStream(content);

        // Act
        var uri = await repository.PutAsync(stream, CreateMessageContext());

        // Assert
        await Assert.That(uri.ToString()).EndsWith(".gzip");

        // The blob should exist with gzip extension
        var blobName = new BlobUriBuilder(uri).BlobName;
        var blobClient = _containerClient.GetBlobClient(blobName);
        var exists = await blobClient.ExistsAsync();
        await Assert.That(exists.Value).IsTrue();
    }

    [Test]
    public async Task GetAsync_WithCompression_DecompressesContent()
    {
        // Arrange
        var repository = CreateRepository(useCompression: true);
        var originalContent =
            "compressed content test - make it longer for better compression ratio"u8.ToArray();
        var inputStream = new MemoryStream(originalContent);

        var uri = await repository.PutAsync(inputStream, CreateMessageContext());

        // Act
        var retrievedStream = await repository.GetAsync(uri);

        // Assert
        using var memoryStream = new MemoryStream();
        await retrievedStream.CopyToAsync(memoryStream);
        var retrievedContent = memoryStream.ToArray();

        await Assert.That(retrievedContent).IsEquivalentTo(originalContent);
    }

    [Test]
    public async Task RoundTrip_WithCompression_PreservesData()
    {
        // Arrange
        var repository = CreateRepository(useCompression: true);
        var originalContent = new byte[10000];
        new Random(42).NextBytes(originalContent);
        var inputStream = new MemoryStream(originalContent);

        // Act
        var uri = await repository.PutAsync(inputStream, CreateMessageContext());
        var retrievedStream = await repository.GetAsync(uri);

        // Assert
        using var memoryStream = new MemoryStream();
        await retrievedStream.CopyToAsync(memoryStream);

        await Assert.That(memoryStream.ToArray()).IsEquivalentTo(originalContent);
    }

    [Test]
    public async Task RoundTrip_WithoutCompression_PreservesData()
    {
        // Arrange
        var repository = CreateRepository(useCompression: false);
        var originalContent = new byte[10000];
        new Random(42).NextBytes(originalContent);
        var inputStream = new MemoryStream(originalContent);

        // Act
        var uri = await repository.PutAsync(inputStream, CreateMessageContext());
        var retrievedStream = await repository.GetAsync(uri);

        // Assert
        using var memoryStream = new MemoryStream();
        await retrievedStream.CopyToAsync(memoryStream);

        await Assert.That(memoryStream.ToArray()).IsEquivalentTo(originalContent);
    }

    [Test]
    public async Task PutAsync_GeneratesUniqueNames()
    {
        // Arrange
        var repository = CreateRepository();

        // Act
        var uri1 = await repository.PutAsync(
            new MemoryStream("content1"u8.ToArray()),
            CreateMessageContext()
        );
        var uri2 = await repository.PutAsync(
            new MemoryStream("content2"u8.ToArray()),
            CreateMessageContext()
        );

        // Assert
        await Assert.That(uri1).IsNotEqualTo(uri2);
    }

    [Test]
    public async Task PutAsync_HandlesEmptyStream()
    {
        // Arrange
        var repository = CreateRepository();
        var emptyStream = new MemoryStream();

        // Act
        var uri = await repository.PutAsync(emptyStream, CreateMessageContext());
        var retrievedStream = await repository.GetAsync(uri);

        // Assert
        using var memoryStream = new MemoryStream();
        await retrievedStream.CopyToAsync(memoryStream);

        await Assert.That(memoryStream.Length).IsEqualTo(0);
    }

    private AzureBlobStorageAttachmentRepository CreateRepository(
        bool useCompression = false,
        bool deleteConsumedBlobs = true
    )
    {
        var options = Options.Create(
            new AzureBlobStorageAttachmentRepositoryOptions
            {
                ConnectionString = _container.GetConnectionString(),
                ContainerName = ContainerName,
                UseCompression = useCompression,
                DeleteConsumedBlobs = deleteConsumedBlobs,
                CreateContainerOnStartup = false,
            }
        );

        return new AzureBlobStorageAttachmentRepository(
            options,
            new BlobNameGenerator(),
            NullLogger<AzureBlobStorageAttachmentRepository>.Instance
        );
    }

    private static OutgoingMessageContext CreateMessageContext() =>
        new()
        {
            Message = new object(),
            MessageType = typeof(object),
            Options = new MessageSendOptions(),
            QueueName = "test-queue",
            MessageTypes = ["test-queue"],
            ServiceProvider = null!,
            CancellationToken = CancellationToken.None,
        };
}
