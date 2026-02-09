using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;

namespace Bussig.Outbox.Npgsql.EntityFrameworkCore;

public static class EfCoreOutboxExtensions
{
    /// <summary>
    /// Activates the outbox context using the DbContext's current transaction.
    /// A transaction must already be started via Database.BeginTransactionAsync().
    /// </summary>
    public static IDisposable Use(this NpgsqlOutboxTransactionContext context, DbContext dbContext)
    {
        ArgumentNullException.ThrowIfNull(dbContext);

        var conn =
            dbContext.Database.GetDbConnection() as NpgsqlConnection
            ?? throw new InvalidOperationException(
                "The DbContext connection is not an NpgsqlConnection."
            );

        var currentTransaction =
            dbContext.Database.CurrentTransaction
            ?? throw new InvalidOperationException(
                "No active transaction. Call Database.BeginTransactionAsync() first."
            );

        var npgsqlTx =
            currentTransaction.GetDbTransaction() as NpgsqlTransaction
            ?? throw new InvalidOperationException(
                "The current transaction is not an NpgsqlTransaction."
            );

        return context.Use(conn, npgsqlTx);
    }
}
