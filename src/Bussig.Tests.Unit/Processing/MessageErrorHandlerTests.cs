using System.Globalization;
using System.Text.Json;
using Bussig.Processing.Internal;

namespace Bussig.Tests.Unit.Processing;

public class MessageErrorHandlerTests
{
    [Test]
    public async Task BuildErrorHeaders_WithNullExistingHeaders_CreatesNewHeaders()
    {
        // Act
        var result = MessageErrorHandler.BuildErrorHeaders(null, "Test error", "TestCode");

        // Assert
        var headers = JsonSerializer.Deserialize<Dictionary<string, object>>(result);
        await Assert.That(headers).IsNotNull();
        await Assert.That(headers!.ContainsKey("error-message")).IsTrue();
        await Assert.That(headers.ContainsKey("error-code")).IsTrue();
        await Assert.That(headers.ContainsKey("error-timestamp")).IsTrue();
    }

    [Test]
    public async Task BuildErrorHeaders_WithEmptyExistingHeaders_CreatesNewHeaders()
    {
        // Act
        var result = MessageErrorHandler.BuildErrorHeaders("", "Test error", "TestCode");

        // Assert
        var headers = JsonSerializer.Deserialize<Dictionary<string, object>>(result);
        await Assert.That(headers).IsNotNull();
        await Assert.That(headers!.ContainsKey("error-message")).IsTrue();
    }

    [Test]
    public async Task BuildErrorHeaders_SetsCorrectErrorMessage()
    {
        // Act
        var result = MessageErrorHandler.BuildErrorHeaders(
            null,
            "Something went wrong",
            "ErrorCode"
        );

        // Assert
        var headers = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(result);
        await Assert.That(headers!["error-message"].GetString()).IsEqualTo("Something went wrong");
    }

    [Test]
    public async Task BuildErrorHeaders_SetsCorrectErrorCode()
    {
        // Act
        var result = MessageErrorHandler.BuildErrorHeaders(
            null,
            "Test error",
            "MaxRetriesExceeded"
        );

        // Assert
        var headers = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(result);
        await Assert.That(headers!["error-code"].GetString()).IsEqualTo("MaxRetriesExceeded");
    }

    [Test]
    public async Task BuildErrorHeaders_SetsTimestampInIso8601Format()
    {
        // Act
        var beforeCall = DateTimeOffset.UtcNow;
        var result = MessageErrorHandler.BuildErrorHeaders(null, "Test error", "TestCode");
        var afterCall = DateTimeOffset.UtcNow;

        // Assert
        var headers = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(result);
        var timestamp = DateTimeOffset.Parse(
            headers!["error-timestamp"].GetString()!,
            CultureInfo.InvariantCulture
        );

        await Assert.That(timestamp).IsGreaterThanOrEqualTo(beforeCall);
        await Assert.That(timestamp).IsLessThanOrEqualTo(afterCall);
    }

    [Test]
    public async Task BuildErrorHeaders_PreservesExistingHeaders()
    {
        // Arrange
        var existingHeaders = JsonSerializer.Serialize(
            new Dictionary<string, object>
            {
                ["custom-header"] = "custom-value",
                ["another-header"] = 123,
            }
        );

        // Act
        var result = MessageErrorHandler.BuildErrorHeaders(
            existingHeaders,
            "Test error",
            "TestCode"
        );

        // Assert
        var headers = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(result);
        await Assert.That(headers!.ContainsKey("custom-header")).IsTrue();
        await Assert.That(headers["custom-header"].GetString()).IsEqualTo("custom-value");
        await Assert.That(headers.ContainsKey("error-message")).IsTrue();
    }

    [Test]
    public async Task BuildErrorHeaders_OverwritesPreviousErrorHeaders()
    {
        // Arrange
        var existingHeaders = JsonSerializer.Serialize(
            new Dictionary<string, object>
            {
                ["error-message"] = "Old error",
                ["error-code"] = "OldCode",
                ["error-timestamp"] = "2020-01-01T00:00:00Z",
            }
        );

        // Act
        var result = MessageErrorHandler.BuildErrorHeaders(existingHeaders, "New error", "NewCode");

        // Assert
        var headers = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(result);
        await Assert.That(headers!["error-message"].GetString()).IsEqualTo("New error");
        await Assert.That(headers["error-code"].GetString()).IsEqualTo("NewCode");
        await Assert
            .That(headers["error-timestamp"].GetString())
            .IsNotEqualTo("2020-01-01T00:00:00Z");
    }

    [Test]
    public async Task BuildErrorHeaders_ReturnsValidJson()
    {
        // Act
        var result = MessageErrorHandler.BuildErrorHeaders(
            null,
            "Error with \"quotes\" and special chars: <>&",
            "Code123"
        );

        // Assert - should not throw
        var headers = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(result);
        await Assert.That(headers).IsNotNull();
        await Assert
            .That(headers!["error-message"].GetString())
            .IsEqualTo("Error with \"quotes\" and special chars: <>&");
    }

    [Test]
    [Arguments("DeserializationFailed")]
    [Arguments("NullMessage")]
    [Arguments("MaxRetriesExceeded")]
    [Arguments("ProcessingFailed")]
    [Arguments("BatchProcessingFailed")]
    public async Task BuildErrorHeaders_AcceptsCommonErrorCodes(string errorCode)
    {
        // Act
        var result = MessageErrorHandler.BuildErrorHeaders(null, "Test error", errorCode);

        // Assert
        var headers = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(result);
        await Assert.That(headers!["error-code"].GetString()).IsEqualTo(errorCode);
    }
}
