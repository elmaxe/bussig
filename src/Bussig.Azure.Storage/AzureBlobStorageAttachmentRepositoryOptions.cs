using Azure.Core;
using Azure.Storage.Blobs;

namespace Bussig.Azure.Storage;

public sealed class AzureBlobStorageAttachmentRepositoryOptions
{
    /// <summary>
    /// If <c>true</c>, compresses blobs with gzip before uploading them to blob storage
    /// </summary>
    public bool UseCompression { get; set; } = true;

    /// <summary>
    /// Connection string to the storage account
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Used for EntraID authentication together with <see cref="TokenCredential"/>
    /// </summary>
    public string? StorageAccountName { get; set; }

    /// <summary>
    /// Used for EntraID authentication together with <see cref="StorageAccountName"/>
    /// </summary>
    public TokenCredential? TokenCredential { get; set; }
    public BlobClientOptions? BlobClientOptions { get; set; }

    /// <summary>
    /// Container name where blobs will be put
    /// </summary>
    public string? ContainerName { get; set; } = "bussig-attachments";

    /// <summary>
    /// If <c>true</c>, tries to create the container on startup.
    /// </summary>
    public bool CreateContainerOnStartup { get; set; } = true;

    /// <summary>
    /// Delete blobs when message is marked as complete. Defaults to <c>true</c>
    /// </summary>
    public bool DeleteConsumedBlobs { get; set; } = true;
}
