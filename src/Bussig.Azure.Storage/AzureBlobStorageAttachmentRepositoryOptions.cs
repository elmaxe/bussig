namespace Bussig.Azure.Storage;

public sealed class AzureBlobStorageAttachmentRepositoryOptions
{
    public bool UseCompression { get; set; }
    public required string ConnectionString { get; set; }
    public string? ContainerName { get; set; } = "bussig-attachments";
    public bool DeleteConsumedBlobs { get; set; } = true;
}
