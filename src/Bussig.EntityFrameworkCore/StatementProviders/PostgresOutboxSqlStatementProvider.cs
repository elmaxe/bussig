using System.Text;

namespace Bussig.EntityFrameworkCore.StatementProviders;

public sealed class PostgresOutboxSqlStatementProvider : OutboxSqlStatementProvider
{
    public override string BuildSelectPendingBatchSql(
        string tableName,
        string? schema,
        OutboxColumnMap columns
    )
    {
        return SqlStatements.GetOrAdd(
            $"{nameof(BuildSelectPendingBatchSql)}",
            _ =>
            {
                var sb = new StringBuilder();
                sb.Append("SELECT ");
                sb.Append(Q(columns.Id)).Append(", ");
                sb.Append(Q(columns.MessageId)).Append(", ");
                sb.Append(Q(columns.QueueName)).Append(", ");
                sb.Append(Q(columns.Body)).Append(", ");
                sb.Append(Q(columns.HeadersJson)).Append(", ");
                sb.Append(Q(columns.Priority)).Append(", ");
                sb.Append(Q(columns.Delay)).Append(", ");
                sb.Append(Q(columns.MessageVersion)).Append(", ");
                sb.Append(Q(columns.ExpirationTime)).Append(", ");
                sb.Append(Q(columns.SchedulingTokenId)).Append(", ");
                sb.Append(Q(columns.CreatedAt)).Append(", ");
                sb.Append(Q(columns.PublishedAt));
                sb.Append(" FROM ");

                if (schema is not null)
                {
                    sb.Append(Q(schema)).Append('.');
                }

                sb.Append(Q(tableName));
                sb.Append(" WHERE ").Append(Q(columns.PublishedAt)).Append(" IS NULL");
                sb.Append(" ORDER BY ").Append(Q(columns.Id));
                sb.Append(" LIMIT {0}");
                sb.Append(" FOR UPDATE SKIP LOCKED");

                return sb.ToString();
            }
        );
    }

    private static string Q(string identifier) => $"\"{identifier}\"";
}
