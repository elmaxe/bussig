using Bussig.Abstractions.Middleware;
using Bussig.Exceptions;
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
        var errorHandler = new MessageErrorHandler(_receiver);

        try
        {
            await nextMiddleware(context);
        }
        catch (DeserializationException ex)
        {
            context.Exception = ex;
            _logger.LogError(
                "Deserialization failed for {Count} message(s) from queue {QueueName}, sending to dead letter",
                context.Messages.Count,
                context.QueueName
            );

            await DeadletterAllAsync(context, errorHandler, context.Exception, null, "NullMessage");
            context.IsHandled = true;
        }
        catch (OperationCanceledException ex)
            when (context.CancellationToken.IsCancellationRequested)
        {
            // Abandon all messages when shutting down so they can be reprocessed
            _logger.LogWarning(
                "Processing cancelled for {Count} message(s) from queue {QueueName}, abandoning",
                context.Messages.Count,
                context.QueueName
            );

            await context.AbandonAllAsync(TimeSpan.Zero, ex, null, null);
            context.Exception = null;
            context.IsHandled = true;
        }
        catch (Exception ex)
        {
            // ex is System.Reflection.TargetInvocationException, its inner exception is what we want
            context.Exception = ex.InnerException;

            _logger.LogError(
                context.Exception,
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

                await DeadletterAllAsync(
                    context,
                    errorHandler,
                    context.Exception,
                    context.Exception?.Message,
                    "MaxRetriesExceeded"
                );
            }
            else
            {
                // Use the message with the highest delivery count for retry calculation
                var representativeMessage = context.Messages.MaxBy(m => m.DeliveryCount)!;
                var delay = retryCalculator.CalculateDelay(
                    representativeMessage,
                    context.Exception
                );

                await context.AbandonAllAsync(
                    delay,
                    context.Exception,
                    context.Exception?.Message,
                    null
                );
            }

            context.IsHandled = true;
        }
    }

    private static async Task DeadletterAllAsync(
        MessageContext context,
        MessageErrorHandler errorHandler,
        Exception? errorException,
        string? errorMessage,
        string errorCode
    )
    {
        foreach (var message in context.Messages)
        {
            await errorHandler.DeadletterAsync(
                message,
                context.QueueName,
                errorException,
                errorMessage,
                errorCode,
                CancellationToken.None
            );
        }
    }
}
