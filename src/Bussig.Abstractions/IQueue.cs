namespace Bussig.Abstractions;

public interface IQueue
{
    string Name { get; }
    int? MaxDeliveryCount { get; }
}
