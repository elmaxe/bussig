using System.Text.Json;
using Bussig.Abstractions;
using Microsoft.Extensions.Logging;

namespace Bussig.Processing.Internal;

/// <summary>
/// Handles error scenarios during message processing including deadlettering and abandoning.
/// </summary>
internal sealed class MessageErrorHandler
{
    private readonly PostgresMessageReceiver _receiver;
    private readonly RetryDelayCalculator _retryDelayCalculator;
    private readonly ILogger _logger;

    public MessageErrorHandler(
        PostgresMessageReceiver receiver,
        RetryDelayCalculator retryDelayCalculator,
        ILogger logger
    )
    {
        _receiver = receiver;
        _retryDelayCalculator = retryDelayCalculator;
        _logger = logger;
    }

    public async Task HandleErrorAsync(
        IncomingMessage message,
        string queueName,
        Exception exception,
        CancellationToken cancellationToken
    )
    {
        _logger.LogError(
            exception,
            "Error processing message {MessageId} from queue {QueueName} (delivery {DeliveryCount}/{MaxDeliveryCount})",
            message.MessageId,
            queueName,
            message.DeliveryCount,
            message.MaxDeliveryCount
        );

        // Check if we've exceeded max delivery count
        if (message.DeliveryCount >= message.MaxDeliveryCount)
        {
            _logger.LogWarning(
                "Message {MessageId} exceeded max delivery count, sending to dead letter queue",
                message.MessageId
            );

            await DeadletterAsync(
                message,
                queueName,
                exception.Message,
                "MaxRetriesExceeded",
                cancellationToken
            );
        }
        else
        {
            // Abandon with delay for retry
            await AbandonAsync(
                message,
                exception.Message,
                "ProcessingFailed",
                _retryDelayCalculator.CalculateDelay(message, exception),
                cancellationToken
            );
        }
    }

    public async Task DeadletterAsync(
        IncomingMessage message,
        string queueName,
        string errorMessage,
        string errorCode,
        CancellationToken cancellationToken
    )
    {
        var headers = BuildErrorHeaders(message.MessageDeliveryHeaders, errorMessage, errorCode);
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
        string errorMessage,
        string errorCode,
        TimeSpan delay,
        CancellationToken cancellationToken
    )
    {
        var headers = BuildErrorHeaders(message.MessageDeliveryHeaders, errorMessage, errorCode);
        await _receiver.AbandonAsync(
            message.MessageDeliveryId,
            message.LockId,
            headers,
            delay,
            cancellationToken
        );
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
        string errorMessage,
        string errorCode
    )
    {
        var headers = string.IsNullOrEmpty(existingHeaders)
            ? new Dictionary<string, object>()
            : JsonSerializer.Deserialize<Dictionary<string, object>>(existingHeaders) ?? [];

        headers["error-message"] = errorMessage;
        headers["error-code"] = errorCode;
        headers["error-timestamp"] = DateTimeOffset.UtcNow.ToString("O");

        return JsonSerializer.Serialize(headers);
    }
}
