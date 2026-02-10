using Microsoft.EntityFrameworkCore;

namespace Bussig.EntityFrameworkCore;

#pragma warning disable CA1822 // Instance members are intentional for DI — state is in AsyncLocal
public sealed class OutboxTransactionContext
{
    private static readonly AsyncLocal<DbContext?> Current = new();

    internal bool IsActive => Current.Value is not null;

    internal DbContext DbContext =>
        Current.Value
        ?? throw new InvalidOperationException("No active outbox transaction context.");

    public IDisposable Use(DbContext dbContext)
    {
        ArgumentNullException.ThrowIfNull(dbContext);

        var prior = Current.Value;
        Current.Value = dbContext;
        return new Scope(prior);
    }

    private sealed class Scope(DbContext? prior) : IDisposable
    {
        public void Dispose()
        {
            Current.Value = prior;
        }
    }
}
#pragma warning restore CA1822
