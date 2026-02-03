using Bussig.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace Bussig.Azure.Storage;

public static class IServiceCollectionExtensions
{
    public static IServiceCollection UseAzureBlobStorageAttachments(
        this IServiceCollection services,
        Action<AzureBlobStorageAttachmentRepositoryOptions, IServiceProvider> configure
    )
    {
        if (services.Any(x => x.ServiceType == typeof(IMessageAttachmentRepository)))
        {
            throw new InvalidOperationException(
                $"An implementation of {nameof(IMessageAttachmentRepository)} is already registered"
            );
        }
        services.AddSingleton<IMessageAttachmentRepository, AzureBlobStorageAttachmentRepository>();
        if (services.All(x => x.ServiceType != typeof(IBlobNameGenerator)))
        {
            services.AddSingleton<IBlobNameGenerator, BlobNameGenerator>();
        }

        services
            .AddOptions<AzureBlobStorageAttachmentRepositoryOptions>()
            .Configure<IServiceProvider>(configure.Invoke);

        return services;
    }
}
