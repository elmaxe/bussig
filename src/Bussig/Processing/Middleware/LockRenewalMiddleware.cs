using Bussig.Abstractions.Middleware;
using Bussig.Processing.Internal;
using Microsoft.Extensions.Logging;

namespace Bussig.Processing.Middleware;

/// <summary>
/// Middleware that manages lock renewal during message processing.
/// Starts lock renewal for all messages before calling next and stops it after.
/// Handles both single-message and batch processing.
/// </summary>
internal sealed class LockRenewalMiddleware : IMessageMiddleware
{
    private readonly PostgresMessageReceiver _receiver;
    private readonly ILogger<LockRenewalMiddleware> _logger;

    public LockRenewalMiddleware(
        PostgresMessageReceiver receiver,
        ILogger<LockRenewalMiddleware> logger
    )
    {
        _receiver = receiver;
        _logger = logger;
    }

    public async Task InvokeAsync(MessageContext context, MessageMiddlewareDelegate nextMiddleware)
    {
        if (context.IsHandled)
        {
            await nextMiddleware(context);
            return;
        }

        // Create lock manager with processor-specific options
        var lockManager = new MessageLockManager(_receiver, context.Options.Lock, _logger);

        using var lockRenewalCts = CancellationTokenSource.CreateLinkedTokenSource(
            context.CancellationToken
        );

        // Start lock renewal for all messages
        var lockRenewalTasks = context
            .Messages.Select(m =>
                lockManager.RunLockRenewalAsync(m.MessageDeliveryId, m.LockId, lockRenewalCts.Token)
            )
            .ToList();

        // Store for potential access by other middleware
        context.SetItem(MiddlewareConstants.LockRenewalCts, lockRenewalCts);
        context.SetItem(MiddlewareConstants.LockRenewalTask, Task.WhenAll(lockRenewalTasks));

        try
        {
            await nextMiddleware(context);
        }
        finally
        {
            await lockRenewalCts.CancelAsync();

            try
            {
                await Task.WhenAll(lockRenewalTasks);
            }
            catch (OperationCanceledException)
            {
                // Expected when processing completes
            }
        }
    }
}
