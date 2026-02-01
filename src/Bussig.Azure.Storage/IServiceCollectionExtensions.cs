using Bussig.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace Bussig.Azure.Storage;

public static class IServiceCollectionExtensions
{
    public static IServiceCollection UseAzureBlobStorageAttachments(
        this IServiceCollection services
    )
    {
        if (services.Any(x => x.ServiceType == typeof(IMessageAttachmentRepository)))
        {
            throw new InvalidOperationException(
                $"An implementation of {nameof(IMessageAttachmentRepository)} is already registered"
            );
        }
        services.AddSingleton<IMessageAttachmentRepository, AzureBlobStorageAttachmentRepository>();

        return services;
    }
}
