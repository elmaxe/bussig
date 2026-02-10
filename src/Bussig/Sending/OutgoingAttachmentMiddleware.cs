using System.Reflection;
using Bussig.Abstractions;
using Bussig.Abstractions.Middleware;
using Bussig.Attachments;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Bussig.Sending;

/// <summary>
/// Middleware that uploads MessageData streams to the attachment repository.
/// Creates new MessageData instances with the Address or InlineData set.
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
        var options = context
            .ServiceProvider.GetRequiredService<IOptions<AttachmentOptions>>()
            .Value;

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
            var stream = messageData?.GetSendStream();
            if (stream is null)
            {
                continue;
            }

            if (options.InlineThreshold > 0)
            {
                await using var memoryStream = new MemoryStream();
                await stream.CopyToAsync(memoryStream, context.CancellationToken);
                var bytes = memoryStream.ToArray();

                if (bytes.Length <= options.InlineThreshold)
                {
                    property.SetValue(context.Message, new MessageData { InlineData = bytes });
                    continue;
                }

                // Upload the buffered data
                var address = await attachmentRepository.PutAsync(
                    new MemoryStream(bytes),
                    context,
                    context.CancellationToken
                );
                property.SetValue(context.Message, new MessageData { Address = address });
            }
            else
            {
                // Upload the data directly and get the address
                var address = await attachmentRepository.PutAsync(
                    stream,
                    context,
                    context.CancellationToken
                );
                await stream.DisposeAsync();
                property.SetValue(context.Message, new MessageData { Address = address });
            }
        }

        await nextMiddleware(context);
    }
}
