# Bussig.Azure.Storage

Azure Blob Storage integration for Bussig message attachments. This package implements the [Claim Check pattern](https://www.enterpriseintegrationpatterns.com/patterns/messaging/StoreInLibrary.html) to enable sending large binary data through messages without storing them in PostgreSQL.

## The Claim Check Pattern

When sending large payloads through a message bus, storing the entire payload in the message can cause performance issues, increase costs, and hit message size limits. The Claim Check pattern solves this by:

1. **Store** - The sender stores the payload in external storage (Azure Blob Storage)
2. **Reference** - A "claim check" (URI reference) replaces the payload in the message
3. **Retrieve** - The receiver uses the claim check to retrieve the original payload

This package implements this pattern transparently—you work with streams and `MessageData` objects while Bussig handles the storage and retrieval automatically.

## Installation

```bash
dotnet add package Bussig.Azure.Storage
```

## Quick Start

Register the attachment repository during service configuration:

```csharp
services.AddBussig((configurator, services) =>
{
    configurator.UseAttachments();
    services.UseAzureBlobStorageAttachments(attachments =>
    {
        attachments.ConnectionString = "UseDevelopmentStorage=true"; // For Azurite
        attachments.ContainerName = "my-attachments";
    });
});
```

## Configuration

### Using Connection String

```csharp
services.AddBussig((configurator, services) =>
{
    configurator.UseAttachments();
    options.UseAzureBlobStorageAttachments(attachments =>
    {
        attachments.ConnectionString = "DefaultEndpointsProtocol=https;AccountName=...";
        attachments.ContainerName = "bussig-attachments"; // default
    });
    // ...
});
```

### Using Entra ID (Managed Identity)

```csharp
services.AddBussig((configurator, services) =>
{
    configurator.UseAttachments();
    options.UseAzureBlobStorageAttachments(attachments =>
    {
        attachments.StorageAccountName = "mystorageaccount";
        attachments.TokenCredential = new DefaultAzureCredential();
    });
    // ...
});
```

### Configuration Options

| Option | Type | Default                | Description |
|--------|------|------------------------|-------------|
| `ConnectionString` | `string?` | `null`                 | Azure Storage connection string |
| `StorageAccountName` | `string?` | `null`                 | Storage account name (for Entra ID) |
| `TokenCredential` | `TokenCredential?` | `null`                 | Azure credential (for Entra ID) |
| `ContainerName` | `string` | `"bussig-attachments"` | Target blob container name |
| `UseCompression` | `bool` | `true`                 | Enable GZIP compression for blobs |
| `CreateContainerOnStartup` | `bool` | `true`                 | Auto-create container if missing |
| `DeleteConsumedBlobs` | `bool` | `true`                 | Delete blobs after message processing |
| `BlobClientOptions` | `BlobClientOptions?` | `null`                 | Advanced Azure SDK client options |

## Usage

### Sending Messages with Attachments

Use the `MessageData` type to attach binary data to messages:

```csharp
public record ProcessFileCommand(string FileName, MessageData FileContent) : ICommand;

// Send a message with an attachment
await using var fileStream = File.OpenRead("document.pdf");

await bus.SendAsync(new ProcessFileCommand(
    FileName: "document.pdf",
    FileContent: new MessageData(fileStream)
));
```

### Processing Messages with Attachments

```csharp
public class ProcessFileProcessor : IProcessor<ProcessFileCommand>
{
    public async Task ProcessAsync(ProcessFileCommand message, CancellationToken ct)
    {
        // The attachment stream is automatically downloaded
        await using var stream = message.FileContent.OpenRead();

        // Process the file content...
    }
}
```

## How It Works

### Outgoing Messages

1. When sending a message with `MessageData` containing a stream, the outgoing middleware uploads the data to Azure Blob Storage
2. The stream is replaced with a URI reference
3. The message is serialized and sent through the bus with only the URI (not the data)

### Incoming Messages

1. When processing a message, the attachment middleware detects `MessageData` properties with URIs
2. The blob is downloaded from Azure Blob Storage
3. The stream is available via `MessageData.OpenRead()`
4. After processing completes, the blob is deleted (if `DeleteConsumedBlobs` is enabled)

## Features

### Compression

Enable GZIP compression to reduce storage costs and transfer times:

```csharp
options.UseAzureBlobStorageAttachments(attachments =>
{
    attachments.ConnectionString = connectionString;
    attachments.UseCompression = true;
});
```

Compression is transparent—data is compressed on upload and decompressed on download automatically.

### Custom Blob Naming

Replace the default GUID-based blob naming by implementing `IBlobNameGenerator`:

```csharp
public class CustomBlobNameGenerator : IBlobNameGenerator
{
    public string GenerateBlobName(OutgoingMessageContext messageContext) => $"attachments/{DateTime.UtcNow:yyyy/MM/dd}/{Guid.NewGuid()}";
}

// Register before UseAzureBlobStorageAttachments
services.AddSingleton<IBlobNameGenerator, CustomBlobNameGenerator>();
```

## Local Development

Use [Azurite](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite) for local development:

```bash
# Start Azurite with Docker
docker run -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite
```

```csharp
options.UseAzureBlobStorageAttachments(attachments =>
{
    attachments.ConnectionString = "UseDevelopmentStorage=true";
});
```

## Dependencies

- [Azure.Storage.Blobs](https://www.nuget.org/packages/Azure.Storage.Blobs) - Azure Blob Storage SDK
- [Azure.Identity](https://www.nuget.org/packages/Azure.Identity) - Entra ID authentication
