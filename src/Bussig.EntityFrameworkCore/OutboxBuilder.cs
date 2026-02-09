using Microsoft.Extensions.DependencyInjection;

namespace Bussig.EntityFrameworkCore;

public sealed class OutboxBuilder(IServiceCollection services)
{
    public IServiceCollection Services { get; } = services;
}
