using Microsoft.EntityFrameworkCore;

namespace Bussig.EntityFrameworkCore.Tests;

public static class TestDbContextFactory
{
    public static TestDbContext Create(string connectionString)
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseNpgsql(connectionString)
            .Options;

        var dbContext = new TestDbContext(options);
        dbContext.Database.EnsureCreated();
        return dbContext;
    }
}
