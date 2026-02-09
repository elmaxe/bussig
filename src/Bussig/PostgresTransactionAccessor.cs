using Npgsql;

namespace Bussig;

internal interface IPostgresTransactionAccessor
{
    NpgsqlTransaction? CurrentTransaction { get; }
    IDisposable Use(NpgsqlTransaction transaction);
}

internal sealed class PostgresTransactionAccessor : IPostgresTransactionAccessor
{
    private static readonly AsyncLocal<NpgsqlTransaction?> Current = new();

    public NpgsqlTransaction? CurrentTransaction => Current.Value;

    public IDisposable Use(NpgsqlTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);

        var prior = Current.Value;
        Current.Value = transaction;
        return new Scope(prior);
    }

    private sealed class Scope : IDisposable
    {
        private readonly NpgsqlTransaction? _prior;

        public Scope(NpgsqlTransaction? prior)
        {
            _prior = prior;
        }

        public void Dispose()
        {
            Current.Value = _prior;
        }
    }
}
