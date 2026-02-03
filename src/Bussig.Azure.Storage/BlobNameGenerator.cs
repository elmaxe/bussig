using Bussig.Abstractions.Middleware;

namespace Bussig.Azure.Storage;

public interface IBlobNameGenerator
{
    string Generate(OutgoingMessageContext messageContext);
}

public sealed class BlobNameGenerator : IBlobNameGenerator
{
    public string Generate(OutgoingMessageContext messageContext) => Guid.NewGuid().ToString();
}
