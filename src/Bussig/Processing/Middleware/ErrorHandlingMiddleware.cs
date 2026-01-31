using Bussig.Abstractions.Middleware;
using Bussig.Processing.Internal;
using Microsoft.Extensions.Logging;

namespace Bussig.Processing.Middleware;

/// <summary>
/// Outermost middleware that handles exceptions, retry logic, and deadlettering.
/// Handles both single-message and batch processing with atomic semantics.
/// </summary>
internal sealed class ErrorHandlingMiddleware : IMessageMiddleware
{
    private readonly PostgresMessageReceiver _receiver;
    private readonly ILogger<ErrorHandlingMiddleware> _logger;

    public ErrorHandlingMiddleware(
        PostgresMessageReceiver receiver,
        ILogger<ErrorHandlingMiddleware> logger
    )
    {
        _receiver = receiver;
        _logger = logger;
    }

    public async Task InvokeAsync(MessageContext context, MessageMiddlewareDelegate nextMiddleware)
    {
        // Create error handler with processor-specific options
        var retryCalculator = new RetryDelayCalculator(context.Options.Retry);
        var errorHandler = new MessageErrorHandler(_receiver, retryCalculator, _logger);

        try
        {
            await nextMiddleware(context);

            // Check if any middleware signaled a deserialization failure
            if (
                context.Items.TryGetValue(MiddlewareConstants.DeserializationFailed, out var failed)
                && failed is true
            )
            {
                var errorMessage = context.Items.TryGetValue(
                    MiddlewareConstants.ErrorMessage,
                    out var msg
                )
                    ? msg as string ?? "Deserialization failed"
                    : "Deserialization failed";

                var errorCode = context.Items.TryGetValue(
                    MiddlewareConstants.ErrorCode,
                    out var code
                )
                    ? code as string ?? "DeserializationFailed"
                    : "DeserializationFailed";

                _logger.LogError(
                    "Deserialization failed for {Count} message(s) from queue {QueueName}, sending to dead letter",
                    context.Messages.Count,
                    context.QueueName
                );

                await DeadletterAllAsync(context, errorHandler, errorMessage, errorCode);
                context.IsHandled = true;
            }
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            // Abandon all messages when shutting down so they can be reprocessed
            _logger.LogWarning(
                "Processing cancelled for {Count} message(s) from queue {QueueName}, abandoning",
                context.Messages.Count,
                context.QueueName
            );

            await context.AbandonAllAsync(TimeSpan.Zero);
            context.Exception = null;
            context.IsHandled = true;
        }
        catch (Exception ex)
        {
            context.Exception = ex;

            _logger.LogError(
                ex,
                "Error processing {Count} message(s) from queue {QueueName}",
                context.Messages.Count,
                context.QueueName
            );

            // Check if any message has exceeded max delivery count
            var exceededMax = context.Messages.Any(m => m.DeliveryCount >= m.MaxDeliveryCount);

            if (exceededMax)
            {
                _logger.LogWarning(
                    "Message(s) exceeded max delivery count, sending {Count} message(s) to dead letter queue",
                    context.Messages.Count
                );

                await DeadletterAllAsync(context, errorHandler, ex.Message, "MaxRetriesExceeded");
            }
            else
            {
                // Use the message with the highest delivery count for retry calculation
                var representativeMessage = context.Messages.MaxBy(m => m.DeliveryCount)!;
                var delay = retryCalculator.CalculateDelay(representativeMessage, ex);

                await context.AbandonAllAsync(delay);
            }

            context.IsHandled = true;
        }
    }

    private static async Task DeadletterAllAsync(
        MessageContext context,
        MessageErrorHandler errorHandler,
        string errorMessage,
        string errorCode
    )
    {
        foreach (var message in context.Messages)
        {
            await errorHandler.DeadletterAsync(
                message,
                context.QueueName,
                errorMessage,
                errorCode,
                CancellationToken.None
            );
        }
    }
}
