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
    configurator.UseAttachments(options =>
    {
        options.InlineThreshold = 1024; // Optional: inline payloads ≤ 1KB
    });
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
        // The attachment stream is lazily fetched when accessed
        await using var stream = await message.FileContent.GetValueAsync(ct);

        // Process the file content...
    }
}
```

## How It Works

### Outgoing Messages

1. When sending a message with `MessageData` containing a stream, the outgoing middleware evaluates the payload size against the inline threshold:
   - **Small payloads** (≤ threshold): Serialized directly as `InlineData` in the message body (no external storage)
   - **Large payloads** (> threshold): Uploaded to Azure Blob Storage and replaced with an `Address` URI reference
2. The message is serialized and sent through the bus

### Incoming Messages

1. When processing a message, the attachment middleware detects `MessageData` properties:
   - **Inline data**: Available directly from the message body (no download)
   - **External references**: Download is deferred until `GetValueAsync()` is called (lazy loading)
2. The stream is downloaded on first access and cached for subsequent calls
3. After processing completes, external blobs are deleted (if `DeleteConsumedBlobs` is enabled)

## Features

### Inline Threshold

Configure an inline threshold to avoid external storage round-trips for small payloads:

```csharp
configurator.UseAttachments(options =>
{
    options.InlineThreshold = 4096; // Inline payloads ≤ 4KB
});
```

When set, payloads at or below the threshold are serialized directly in the message body as `InlineData`. Larger payloads are uploaded to Azure Blob Storage. Setting the threshold to `0` (default) disables inlining—all attachments use external storage.

**Trade-offs:**
- **Inline**: Lower latency (no blob storage round-trip), but increases message size in PostgreSQL
- **External**: Smaller messages in the database, but requires blob storage download (lazy loaded)

### Lazy Loading

Attachments stored in Azure Blob Storage are downloaded lazily—only when you call `GetValueAsync()`. If your processor doesn't access the attachment (e.g., filters messages early), the download never happens:

```csharp
public async Task ProcessAsync(ProcessFileCommand message, CancellationToken ct)
{
    if (message.FileName.EndsWith(".txt"))
        return; // No download for .txt files

    // Download only happens here, when needed
    await using var stream = await message.FileContent.GetValueAsync(ct);
    // ...
}
```

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

## Limitations

### Batch Processing Not Supported

Attachments are **not supported** for batch processors (`IProcessor<Batch<TMessage>>`). If you attempt to process messages with `MessageData` properties in a batch processor, a `NotSupportedException` will be thrown at runtime.

**Workaround:** Use single-message processors for messages that include attachments:

```csharp
// ✅ Supported - single message processor
public class ProcessFileProcessor : IProcessor<ProcessFileCommand>
{
    public async Task ProcessAsync(ProcessFileCommand message, CancellationToken ct)
    {
        await using var stream = await message.FileContent.GetValueAsync(ct);
        // ...
    }
}

// ❌ Not supported - batch processor with attachments
public class ProcessFileBatchProcessor : IProcessor<Batch<ProcessFileCommand>>
{
    // Will throw NotSupportedException if ProcessFileCommand has MessageData
}
```

This limitation exists because batch processing optimizes for throughput by processing multiple messages in a single transaction, while attachment management requires individual stream lifecycle handling.


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
