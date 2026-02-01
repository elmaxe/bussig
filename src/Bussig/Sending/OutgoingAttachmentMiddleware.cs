using System.Reflection;
using Bussig.Abstractions;
using Bussig.Abstractions.Middleware;
using Microsoft.Extensions.DependencyInjection;

namespace Bussig.Sending;

/// <summary>
/// Middleware that uploads MessageData streams to the attachment repository.
/// Creates new MessageData instances with the Address set.
/// </summary>
internal sealed class OutgoingAttachmentMiddleware : IOutgoingMessageMiddleware
{
    public async Task InvokeAsync(
        OutgoingMessageContext context,
        OutgoingMessageMiddlewareDelegate nextMiddleware
    )
    {
        var attachmentRepository =
            context.ServiceProvider.GetRequiredService<IMessageAttachmentRepository>();

        // Find all MessageData properties on the message
        var messageDataProperties = context
            .MessageType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p =>
                p.PropertyType == typeof(MessageData) && p is { CanRead: true, CanWrite: true }
            )
            .ToList();

        foreach (var property in messageDataProperties)
        {
            var messageData = (MessageData?)property.GetValue(context.Message);
            if (messageData is null || !messageData.HasData)
            {
                continue;
            }

            // Upload the data and get the address
            var address = await attachmentRepository.PutAsync(
                messageData.GetData()!,
                context.CancellationToken
            );

            // Create a new MessageData with the address set
            property.SetValue(context.Message, new MessageData { Address = address });
        }

        await nextMiddleware(context);
    }
}
