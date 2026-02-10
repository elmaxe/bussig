using Microsoft.EntityFrameworkCore;

namespace Bussig.EntityFrameworkCore.Tests;

[ClassDataSource<PostgresContainerFixture>(Shared = SharedType.PerClass)]
public class OutboxMessageEntityConfigurationTests(PostgresContainerFixture fixture)
{
    [Test]
    public async Task PrimaryKey_IsOnId()
    {
        await using var dbContext = TestDbContextFactory.Create(fixture.ConnectionString);
        var entityType = dbContext.Model.FindEntityType(typeof(OutboxMessage))!;
        var pk = entityType.FindPrimaryKey();

        await Assert.That(pk).IsNotNull();
        await Assert.That(pk!.Properties.Select(p => p.Name)).Contains("Id");
    }

    [Test]
    public async Task RequiredProperties_AreConfigured()
    {
        await using var dbContext = TestDbContextFactory.Create(fixture.ConnectionString);
        var entityType = dbContext.Model.FindEntityType(typeof(OutboxMessage))!;

        await Assert.That(entityType.FindProperty("MessageId")!.IsNullable).IsFalse();
        await Assert.That(entityType.FindProperty("QueueName")!.IsNullable).IsFalse();
        await Assert.That(entityType.FindProperty("Body")!.IsNullable).IsFalse();
        await Assert.That(entityType.FindProperty("MessageVersion")!.IsNullable).IsFalse();
        await Assert.That(entityType.FindProperty("CreatedAt")!.IsNullable).IsFalse();
    }

    [Test]
    public async Task MessageVersion_HasDefaultValue()
    {
        await using var dbContext = TestDbContextFactory.Create(fixture.ConnectionString);
        var entityType = dbContext.Model.FindEntityType(typeof(OutboxMessage))!;
        var prop = entityType.FindProperty("MessageVersion")!;

        await Assert.That(prop.GetDefaultValue()).IsEqualTo(1);
    }

    [Test]
    public async Task Indexes_AreConfigured()
    {
        await using var dbContext = TestDbContextFactory.Create(fixture.ConnectionString);
        var entityType = dbContext.Model.FindEntityType(typeof(OutboxMessage))!;
        var indexes = entityType.GetIndexes().ToList();

        var indexProperties = indexes.SelectMany(i => i.Properties).Select(p => p.Name).ToList();

        await Assert.That(indexProperties).Contains("Id");
        await Assert.That(indexProperties).Contains("SchedulingTokenId");
        await Assert.That(indexProperties).Contains("PublishedAt");
    }
}
