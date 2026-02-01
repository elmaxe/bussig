using Bussig.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace Bussig.Attachments;

public static class AttachmentExtensions
{
    public static void UseAttachments(this IBussigRegistrationConfigurator configurator)
    {
        if (configurator is BussigRegistrationConfigurator impl)
        {
            impl.AttachmentsEnabled = true;
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
