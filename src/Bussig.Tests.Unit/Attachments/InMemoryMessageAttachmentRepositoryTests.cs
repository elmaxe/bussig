using Bussig.Attachments;
using Bussig.Exceptions;

namespace Bussig.Tests.Unit.Attachments;

public class InMemoryMessageAttachmentRepositoryTests
{
    [Test]
    public async Task PutAsync_StoresDataAndReturnsUri()
    {
        // Arrange
        var repository = new InMemoryMessageAttachmentRepository();
        var content = "test content"u8.ToArray();
        var stream = new MemoryStream(content);

        // Act
        var uri = await repository.PutAsync(stream);

        // Assert
        await Assert.That(uri).IsNotNull();
        await Assert.That(uri.Scheme).IsEqualTo("https");
    }

    [Test]
    public async Task PutAsync_GeneratesUniqueUris()
    {
        // Arrange
        var repository = new InMemoryMessageAttachmentRepository();
        var stream1 = new MemoryStream("content1"u8.ToArray());
        var stream2 = new MemoryStream("content2"u8.ToArray());

        // Act
        var uri1 = await repository.PutAsync(stream1);
        var uri2 = await repository.PutAsync(stream2);

        // Assert
        await Assert.That(uri1).IsNotEqualTo(uri2);
    }

    [Test]
    public async Task GetAsync_ReturnsStoredData()
    {
        // Arrange
        var repository = new InMemoryMessageAttachmentRepository();
        var originalContent = "test content for retrieval"u8.ToArray();
        var inputStream = new MemoryStream(originalContent);

        var uri = await repository.PutAsync(inputStream);

        // Act
        var retrievedStream = await repository.GetAsync(uri);

        // Assert
        using var memoryStream = new MemoryStream();
        await retrievedStream.CopyToAsync(memoryStream);
        var retrievedContent = memoryStream.ToArray();

        await Assert.That(retrievedContent).IsEquivalentTo(originalContent);
    }

    [Test]
    public async Task GetAsync_ThrowsMessageDataException_WhenUriNotFound()
    {
        // Arrange
        var repository = new InMemoryMessageAttachmentRepository();
        var nonExistentUri = new Uri("https://nonexistent.se");

        // Act & Assert
        await Assert
            .That(async () => await repository.GetAsync(nonExistentUri))
            .Throws<MessageDataException>();
    }

    [Test]
    public async Task GetAsync_ThrowsWithCorrectMessage_WhenUriNotFound()
    {
        // Arrange
        var repository = new InMemoryMessageAttachmentRepository();
        var nonExistentUri = new Uri("https://nonexistent.se");

        // Act & Assert
        await Assert
            .That(async () => await repository.GetAsync(nonExistentUri))
            .Throws<MessageDataException>()
            .WithMessageContaining(nonExistentUri.ToString());
    }

    [Test]
    public async Task DeleteAsync_RemovesData()
    {
        // Arrange
        var repository = new InMemoryMessageAttachmentRepository();
        var content = "content to delete"u8.ToArray();
        var stream = new MemoryStream(content);

        var uri = await repository.PutAsync(stream);

        // Verify it exists first
        var existsBeforeDelete = await repository.GetAsync(uri);
        await Assert.That(existsBeforeDelete).IsNotNull();

        // Act
        await repository.DeleteAsync(uri);

        // Assert - Should throw because data was deleted
        await Assert
            .That(async () => await repository.GetAsync(uri))
            .Throws<MessageDataException>();
    }

    [Test]
    public async Task DeleteAsync_DoesNotThrow_WhenUriDoesNotExist()
    {
        // Arrange
        var repository = new InMemoryMessageAttachmentRepository();
        var nonExistentUri = new Uri("https://nonexistent.se");

        // Act & Assert - Should not throw
        await Assert.That(async () => await repository.DeleteAsync(nonExistentUri)).ThrowsNothing();
    }

    [Test]
    public async Task PutAsync_CopiesStreamContent()
    {
        // Arrange
        var repository = new InMemoryMessageAttachmentRepository();
        var content = "stream content"u8.ToArray();
        var inputStream = new MemoryStream(content);

        // Act
        var uri = await repository.PutAsync(inputStream);

        // Dispose original stream
        inputStream.Dispose();

        // Assert - Should still be able to retrieve the data
        var retrievedStream = await repository.GetAsync(uri);
        using var memoryStream = new MemoryStream();
        await retrievedStream.CopyToAsync(memoryStream);

        await Assert.That(memoryStream.ToArray()).IsEquivalentTo(content);
    }
}
