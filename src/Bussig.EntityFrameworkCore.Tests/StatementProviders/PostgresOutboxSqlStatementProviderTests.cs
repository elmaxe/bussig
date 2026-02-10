using Bussig.EntityFrameworkCore.StatementProviders;

namespace Bussig.EntityFrameworkCore.Tests.StatementProviders;

/// <summary>
/// Helper to clear the static ConcurrentDictionary cache on OutboxSqlStatementProvider.
/// </summary>
internal sealed class SqlStatementCacheClearer : OutboxSqlStatementProvider
{
    public override string BuildSelectPendingBatchSql(
        string tableName,
        string? schema,
        OutboxColumnMap columns
    ) => throw new NotSupportedException();

    public static void ClearCache() => SqlStatements.Clear();
}

[NotInParallel]
public class PostgresOutboxSqlStatementProviderTests
{
    private readonly PostgresOutboxSqlStatementProvider _provider = new();

    [Before(Test)]
    public void ClearCache()
    {
        SqlStatementCacheClearer.ClearCache();
    }

    [Test]
    public async Task BuildSelectPendingBatchSql_WithoutSchema_GeneratesCorrectSql()
    {
        var columns = CreateDefaultColumnMap();

        var sql = _provider.BuildSelectPendingBatchSql("OutboxMessages", null, columns);

        await Assert.That(sql).Contains("SELECT ");
        await Assert.That(sql).Contains("FROM \"OutboxMessages\"");
        await Assert.That(sql).Contains("WHERE \"PublishedAt\" IS NULL");
        await Assert.That(sql).Contains("ORDER BY \"Id\"");
        await Assert.That(sql).Contains("LIMIT {0}");
        await Assert.That(sql).Contains("FOR UPDATE SKIP LOCKED");
        await Assert.That(sql).DoesNotContain(".");
    }

    [Test]
    public async Task BuildSelectPendingBatchSql_WithSchema_IncludesSchemaQualification()
    {
        var columns = CreateDefaultColumnMap();

        var sql = _provider.BuildSelectPendingBatchSql("OutboxMessages", "myschema", columns);

        await Assert.That(sql).Contains("FROM \"myschema\".\"OutboxMessages\"");
    }

    [Test]
    public async Task BuildSelectPendingBatchSql_AllColumnsAreDoubleQuoted()
    {
        var columns = CreateDefaultColumnMap();

        var sql = _provider.BuildSelectPendingBatchSql("OutboxMessages", null, columns);

        await Assert.That(sql).Contains("\"Id\"");
        await Assert.That(sql).Contains("\"MessageId\"");
        await Assert.That(sql).Contains("\"QueueName\"");
        await Assert.That(sql).Contains("\"Body\"");
        await Assert.That(sql).Contains("\"HeadersJson\"");
        await Assert.That(sql).Contains("\"Priority\"");
        await Assert.That(sql).Contains("\"Delay\"");
        await Assert.That(sql).Contains("\"MessageVersion\"");
        await Assert.That(sql).Contains("\"ExpirationTime\"");
        await Assert.That(sql).Contains("\"SchedulingTokenId\"");
        await Assert.That(sql).Contains("\"CreatedAt\"");
        await Assert.That(sql).Contains("\"PublishedAt\"");
    }

    [Test]
    public async Task BuildSelectPendingBatchSql_CustomColumnNames_UsedCorrectly()
    {
        var columns = new OutboxColumnMap
        {
            Id = "outbox_id",
            MessageId = "msg_id",
            QueueName = "queue",
            Body = "payload",
            HeadersJson = "headers",
            Priority = "prio",
            Delay = "delay_ms",
            MessageVersion = "ver",
            ExpirationTime = "expires_at",
            SchedulingTokenId = "sched_token",
            CreatedAt = "created",
            PublishedAt = "published",
        };

        var sql = _provider.BuildSelectPendingBatchSql("outbox", null, columns);

        await Assert.That(sql).Contains("\"outbox_id\"");
        await Assert.That(sql).Contains("\"msg_id\"");
        await Assert.That(sql).Contains("\"payload\"");
        await Assert.That(sql).Contains("WHERE \"published\" IS NULL");
        await Assert.That(sql).Contains("ORDER BY \"outbox_id\"");
    }

    private static OutboxColumnMap CreateDefaultColumnMap() =>
        new()
        {
            Id = "Id",
            MessageId = "MessageId",
            QueueName = "QueueName",
            Body = "Body",
            HeadersJson = "HeadersJson",
            Priority = "Priority",
            Delay = "Delay",
            MessageVersion = "MessageVersion",
            ExpirationTime = "ExpirationTime",
            SchedulingTokenId = "SchedulingTokenId",
            CreatedAt = "CreatedAt",
            PublishedAt = "PublishedAt",
        };
}
