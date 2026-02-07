using Bussig.Abstractions;
using Bussig.Abstractions.Middleware;
using Bussig.Processing.Internal;
using Microsoft.Extensions.Logging;

namespace Bussig.Processing.Middleware;

/// <summary>
/// Middleware that manages lock renewal during message processing.
/// Starts lock renewal for a message before calling next and stops it after.
/// Handles only single-message processing.
///
/// Batch processors are supposed to be fast processing messages, long running lock renewal is not something we should do.
/// </summary>
internal sealed class LockRenewalMiddleware : IMessageMiddleware
{
    private readonly IMessageLockRenewer _renewer;
    private readonly ILogger<LockRenewalMiddleware> _logger;

    public LockRenewalMiddleware(IMessageLockRenewer renewer, ILogger<LockRenewalMiddleware> logger)
    {
        _renewer = renewer;
        _logger = logger;
    }

    public async Task InvokeAsync(MessageContext context, MessageMiddlewareDelegate nextMiddleware)
    {
        if (context.IsHandled || context.IsBatchProcessor)
        {
            await nextMiddleware(context);
            return;
        }

        // Create lock manager with processor-specific options
        var lockManager = new MessageLockManager(_renewer, context.Options.Lock, _logger);

        using var lockRenewalCts = CancellationTokenSource.CreateLinkedTokenSource(
            context.CancellationToken
        );

        // Start lock renewal for the message
        var lockRenewalTask = lockManager.RunLockRenewalAsync(
            context.Message.MessageDeliveryId,
            context.Message.LockId,
            lockRenewalCts.Token
        );

        try
        {
            await nextMiddleware(context);
        }
        finally
        {
            await lockRenewalCts.CancelAsync();

            try
            {
                await lockRenewalTask;
            }
            catch (OperationCanceledException)
            {
                // Expected when processing completes
            }
        }
    }
}
