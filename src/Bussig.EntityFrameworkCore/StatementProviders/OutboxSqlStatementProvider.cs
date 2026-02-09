namespace Bussig.EntityFrameworkCore.StatementProviders;

public abstract class OutboxSqlStatementProvider
{
    /// <summary>
    /// Returns SQL to select a batch of unpublished messages with row-level locking.
    /// Must use {0} as the EF Core parameter placeholder for batch size.
    /// Must return all columns for the OutboxMessage entity.
    /// </summary>
    public abstract string BuildSelectPendingBatchSql(
        string tableName,
        string? schema,
        OutboxColumnMap columns
    );
}
