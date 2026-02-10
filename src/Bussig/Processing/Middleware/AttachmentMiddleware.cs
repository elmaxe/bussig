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

        // Set up lazy value factories for external addresses
        foreach (var messageDataProperty in messageDataProperties)
        {
            var messageData = (MessageData?)
                messageDataProperty.GetValue(context.DeserializedMessage);
            if (messageData?.Address is null)
            {
                continue;
            }

            var address = messageData.Address;
            messageData.SetValueFactory(ct => messageAttachmentRepository.GetAsync(address, ct));
        }

        try
        {
            await nextMiddleware(context);
        }
        finally
        {
            foreach (var messageDataProperty in messageDataProperties)
            {
                var messageData = (MessageData?)
                    messageDataProperty.GetValue(context.DeserializedMessage);
                if (messageData is null)
                {
                    continue;
                }

                // Dispose stream if it was accessed and completed successfully
                if (messageData.WasAccessed)
                {
                    var cachedTask = messageData.GetCachedValue();
                    if (cachedTask is { IsCompletedSuccessfully: true })
                    {
                        await cachedTask.Result.DisposeAsync();
                    }
                }

                // Delete from external storage if it had an address
                if (messageData.Address is not null)
                {
                    await messageAttachmentRepository.DeleteAsync(
                        messageData.Address,
                        context.CancellationToken
                    );
                }
            }
        }
    }
}
