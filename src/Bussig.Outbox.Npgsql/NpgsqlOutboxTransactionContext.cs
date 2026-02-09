using Npgsql;

namespace Bussig.Outbox.Npgsql;

#pragma warning disable CA1822 // Instance members are intentional for DI — state is in AsyncLocal
public sealed class NpgsqlOutboxTransactionContext
{
    private static readonly AsyncLocal<TransactionState?> Current = new();

    internal bool IsActive => Current.Value is not null;
    internal NpgsqlConnection Connection =>
        Current.Value?.Connection
        ?? throw new InvalidOperationException("No active outbox transaction context.");
    internal NpgsqlTransaction Transaction =>
        Current.Value?.Transaction
        ?? throw new InvalidOperationException("No active outbox transaction context.");

    public IDisposable Use(NpgsqlConnection connection, NpgsqlTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);

        var prior = Current.Value;
        Current.Value = new TransactionState(connection, transaction);
        return new Scope(prior);
    }

    private sealed record TransactionState(
        NpgsqlConnection Connection,
        NpgsqlTransaction Transaction
    );

    private sealed class Scope(TransactionState? prior) : IDisposable
    {
        public void Dispose()
        {
            Current.Value = prior;
        }
    }
}
#pragma warning restore CA1822
