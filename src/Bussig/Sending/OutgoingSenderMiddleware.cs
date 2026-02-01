using Bussig.Abstractions;
using Bussig.Abstractions.Middleware;
using Microsoft.Extensions.DependencyInjection;
using SecurityDriven;

namespace Bussig.Sending;

/// <summary>
/// Terminal middleware that builds the OutgoingMessage and sends it.
/// This is the final middleware in the outgoing pipeline.
/// </summary>
internal sealed class OutgoingSenderMiddleware : IOutgoingMessageMiddleware
{
    public async Task InvokeAsync(
        OutgoingMessageContext context,
        OutgoingMessageMiddlewareDelegate nextMiddleware
    )
    {
        // Short-circuit if already sent (e.g., by outbox middleware)
        if (context.IsSent)
        {
            await nextMiddleware(context);
            return;
        }

        // Build the outgoing message
        var outgoing = new OutgoingMessage(
            context.Options.MessageId ?? FastGuid.NewPostgreSqlGuid(),
            context.QueueName,
            context.SerializedBody
                ?? throw new InvalidOperationException(
                    $"nameof{context.SerializedBody} must be set before OutgoingSenderMiddleware"
                ),
            context.FinalHeadersJson
                ?? throw new InvalidOperationException(
                    "FinalHeadersJson must be set before OutgoingSenderMiddleware"
                )
        )
        {
            Delay = context.Options.Delay,
            SchedulingTokenId = context.Options.SchedulingToken,
            MessageVersion = context.Options.MessageVersion,
            Priority = context.Options.Priority.HasValue ? (short)context.Options.Priority : null,
        };

        context.OutgoingMessage = outgoing;

        // Send the message
        var messageSender = context.ServiceProvider.GetRequiredService<IOutgoingMessageSender>();
        await messageSender.SendAsync(outgoing, context.CancellationToken);

        context.IsSent = true;

        await nextMiddleware(context);
    }
}
