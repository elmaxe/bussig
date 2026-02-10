using Microsoft.EntityFrameworkCore;
using Moq;

namespace Bussig.EntityFrameworkCore.Tests;

public class OutboxTransactionContextTests
{
    [Test]
    public async Task IsActive_WhenNoScope_ReturnsFalse()
    {
        var context = new OutboxTransactionContext();

        await Assert.That(context.IsActive).IsFalse();
    }

    [Test]
    public async Task IsActive_WhenScopeActive_ReturnsTrue()
    {
        var context = new OutboxTransactionContext();
        var dbContext = new Mock<DbContext>().Object;

        using var scope = context.Use(dbContext);

        await Assert.That(context.IsActive).IsTrue();
    }

    [Test]
    public async Task IsActive_AfterScopeDisposed_ReturnsFalse()
    {
        var context = new OutboxTransactionContext();
        var dbContext = new Mock<DbContext>().Object;

        var scope = context.Use(dbContext);
        scope.Dispose();

        await Assert.That(context.IsActive).IsFalse();
    }

    [Test]
    public async Task DbContext_WhenScopeActive_ReturnsCorrectInstance()
    {
        var context = new OutboxTransactionContext();
        var dbContext = new Mock<DbContext>().Object;

        using var scope = context.Use(dbContext);

        await Assert.That(context.DbContext).IsSameReferenceAs(dbContext);
    }

    [Test]
    public async Task DbContext_WhenNoScope_ThrowsInvalidOperationException()
    {
        var context = new OutboxTransactionContext();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Task.FromResult(context.DbContext)
        );
    }

    [Test]
    public async Task Use_WithNull_ThrowsArgumentNullException()
    {
        var context = new OutboxTransactionContext();

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
        {
            context.Use(null!);
            return Task.CompletedTask;
        });
    }

    [Test]
    public async Task NestedScopes_RestorePriorContext()
    {
        var context = new OutboxTransactionContext();
        var outerDb = new Mock<DbContext>().Object;
        var innerDb = new Mock<DbContext>().Object;

        using var outerScope = context.Use(outerDb);
        await Assert.That(context.DbContext).IsSameReferenceAs(outerDb);

        using (var innerScope = context.Use(innerDb))
        {
            await Assert.That(context.DbContext).IsSameReferenceAs(innerDb);
        }

        await Assert.That(context.DbContext).IsSameReferenceAs(outerDb);
    }
}
