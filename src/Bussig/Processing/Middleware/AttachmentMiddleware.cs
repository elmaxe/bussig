using System.Reflection;
using Bussig.Abstractions;
using Bussig.Abstractions.Middleware;

namespace Bussig.Processing.Middleware;

public class AttachmentMiddleware(IMessageAttachmentRepository messageAttachmentRepository)
    : IMessageMiddleware
{
    public async Task InvokeAsync(MessageContext context, MessageMiddlewareDelegate nextMiddleware)
    {
        if (context.IsHandled || context.DeserializedMessage is null)
        {
            return;
        }

        if (context.IsBatchProcessor)
        {
            throw new NotSupportedException("Attachments not supported for batch consumption");
        }

        var messageDataProperties = context
            .MessageType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.PropertyType == typeof(MessageData))
            .ToList();
        if (messageDataProperties.Count == 0)
        {
            await nextMiddleware(context);
            return;
        }

        try
        {
            foreach (var messageDataProperty in messageDataProperties)
            {
                var messageData = (MessageData?)
                    messageDataProperty.GetValue(context.DeserializedMessage);
                if (messageData?.Address is null)
                {
                    continue;
                }

                var stream = await messageAttachmentRepository.GetAsync(
                    messageData.Address,
                    context.CancellationToken
                );
                messageData.SetData(stream);
            }

            await nextMiddleware(context);
        }
        finally
        {
            foreach (var messageDataProperty in messageDataProperties)
            {
                var messageData = (MessageData?)
                    messageDataProperty.GetValue(context.DeserializedMessage);
                var stream = messageData?.GetData();
                if (stream is not null)
                {
                    await stream.DisposeAsync();
                    await messageAttachmentRepository.DeleteAsync(
                        messageData!.Address!,
                        context.CancellationToken
                    );
                }
            }
        }
    }
}
