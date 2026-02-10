using Microsoft.EntityFrameworkCore;

namespace Bussig.EntityFrameworkCore.Tests;

public sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.AddOutboxMessageEntity();
    }
}
