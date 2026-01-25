using Bussig.Abstractions;

namespace Bussig;

public sealed record Queue(string Name, int? MaxDeliveryCount = null) : IQueue;
