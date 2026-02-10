using Bussig.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace Bussig.Attachments;

public class AttachmentOptions
{
    public int InlineThreshold { get; set; }
}

public static class AttachmentExtensions
{
    public static void UseAttachments(
        this IBussigRegistrationConfigurator configurator,
        Action<AttachmentOptions>? configure = null
    )
    {
        if (configurator is BussigRegistrationConfigurator impl)
        {
            impl.AttachmentsEnabled = true;
            if (configure is not null)
            {
                impl.ConfigureAttachmentOptions = configure;
            }
        }
    }

    public static void UseInMemoryAttachments(this IServiceCollection services)
    {
        if (services.Any(x => x.ServiceType == typeof(IMessageAttachmentRepository)))
        {
            throw new InvalidOperationException(
                $"An implementation of {nameof(IMessageAttachmentRepository)} is already registered"
            );
        }
        services.AddSingleton<IMessageAttachmentRepository, InMemoryMessageAttachmentRepository>();
    }
}
