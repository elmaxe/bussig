using Microsoft.EntityFrameworkCore;

namespace Bussig.EntityFrameworkCore.Tests.Extensions;

public class ModelBuilderExtensionsTests
{
    [Test]
    public async Task AddOutboxMessageEntity_RegistersEntityInModel()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseInMemoryDatabase("ModelBuilderTest_Entity")
            .Options;

        await using var dbContext = new TestDbContext(options);
        var entityType = dbContext.Model.FindEntityType(typeof(OutboxMessage));

        await Assert.That(entityType).IsNotNull();
    }

    [Test]
    public async Task AddOutboxMessageEntity_CustomConfiguration_Applied()
    {
        var optionsBuilder = new DbContextOptionsBuilder().UseInMemoryDatabase(
            "ModelBuilderTest_Custom"
        );

        await using var dbContext = new CustomTableDbContext(optionsBuilder.Options);
        var entityType = dbContext.Model.FindEntityType(typeof(OutboxMessage));

        await Assert.That(entityType).IsNotNull();
        await Assert.That(entityType!.GetTableName()).IsEqualTo("my_outbox");
    }

    private sealed class CustomTableDbContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.AddOutboxMessageEntity(e => e.ToTable("my_outbox"));
        }
    }
}
