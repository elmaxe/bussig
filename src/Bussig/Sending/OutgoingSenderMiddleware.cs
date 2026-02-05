using Bussig.Abstractions;
using Bussig.Abstractions.Middleware;
using Microsoft.Extensions.DependencyInjection;

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
        var envelope =
            context.Envelope
            ?? throw new InvalidOperationException(
                "Envelope must be set before OutgoingSenderMiddleware"
            );

        var outgoing = new OutgoingMessage(
            envelope.MessageId,
            context.QueueName,
            context.SerializedBody
                ?? throw new InvalidOperationException(
                    "SerializedBody must be set before OutgoingSenderMiddleware"
                ),
            context.EnvelopeJson
                ?? throw new InvalidOperationException(
                    "EnvelopeJson must be set before OutgoingSenderMiddleware"
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
