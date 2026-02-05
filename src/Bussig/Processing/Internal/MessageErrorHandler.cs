using System.Text.Json;
using Bussig.Abstractions;

namespace Bussig.Processing.Internal;

/// <summary>
/// Handles error scenarios during message processing including deadlettering and abandoning.
/// </summary>
internal sealed class MessageErrorHandler
{
    private readonly PostgresMessageReceiver _receiver;

    public MessageErrorHandler(PostgresMessageReceiver receiver)
    {
        _receiver = receiver;
    }

    public async Task DeadletterAsync(
        IncomingMessage message,
        string queueName,
        Exception? errorException,
        string? errorMessage,
        string errorCode,
        CancellationToken cancellationToken
    )
    {
        var headers = BuildErrorHeaders(
            message.MessageDeliveryHeaders,
            errorException,
            errorMessage,
            errorCode
        );
        await _receiver.DeadletterAsync(
            message.MessageDeliveryId,
            message.LockId,
            queueName,
            headers,
            cancellationToken
        );
    }

    public async Task AbandonAsync(
        IncomingMessage message,
        Exception? errorException,
        string errorMessage,
        string errorCode,
        TimeSpan delay,
        CancellationToken cancellationToken
    )
    {
        var headers = BuildErrorHeaders(
            message.MessageDeliveryHeaders,
            errorException,
            errorMessage,
            errorCode
        );
        await _receiver.AbandonAsync(
            message.MessageDeliveryId,
            message.LockId,
            headers,
            delay,
            cancellationToken
        );
    }

    public async Task AbandonAsync(
        IReadOnlyList<IncomingMessage> messages,
        Exception? errorException,
        string errorMessage,
        string errorCode,
        TimeSpan delay,
        CancellationToken cancellationToken
    )
    {
        var deliveryIds = messages.Select(m => m.MessageDeliveryId).ToArray();
        var lockIds = messages.Select(m => m.LockId).ToArray();
        var headers = messages.Select(m =>
            BuildErrorHeaders(m.MessageDeliveryHeaders, errorException, errorMessage, errorCode)
        );
        await _receiver.AbandonAsync(deliveryIds, lockIds, headers, delay, cancellationToken);
    }

    public async Task AbandonWithoutErrorAsync(
        IncomingMessage message,
        CancellationToken cancellationToken
    )
    {
        await _receiver.AbandonAsync(
            message.MessageDeliveryId,
            message.LockId,
            message.MessageDeliveryHeaders,
            TimeSpan.Zero,
            cancellationToken
        );
    }

    public static string BuildErrorHeaders(
        string? existingHeaders,
        Exception? errorException,
        string? errorMessage,
        string errorCode
    )
    {
        var headers = string.IsNullOrWhiteSpace(existingHeaders)
            ? new Dictionary<string, object?>()
            : JsonSerializer.Deserialize<Dictionary<string, object?>>(existingHeaders) ?? [];

        headers["error-exception"] = errorException?.ToString();
        headers["error-message"] = errorException?.Message ?? errorMessage;
        headers["error-code"] = errorCode;
        headers["error-timestamp"] = DateTimeOffset.UtcNow.ToString("O");

        return JsonSerializer.Serialize(headers);
    }
}
