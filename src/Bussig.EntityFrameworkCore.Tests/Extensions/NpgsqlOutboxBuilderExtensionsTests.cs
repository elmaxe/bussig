using Bussig.Abstractions;
using Bussig.EntityFrameworkCore.StatementProviders;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace Bussig.EntityFrameworkCore.Tests.Extensions;

public class NpgsqlOutboxBuilderExtensionsTests
{
    [Test]
    public async Task UseNpgsql_RegistersPostgresSqlStatementProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IOutgoingMessageSender>(new Mock<IOutgoingMessageSender>().Object);

        services.AddBussigOutbox<TestDbContext>().UseNpgsql();

        var descriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(OutboxSqlStatementProvider)
        );
        await Assert.That(descriptor).IsNotNull();
        await Assert
            .That(descriptor!.ImplementationType)
            .IsEqualTo(typeof(PostgresOutboxSqlStatementProvider));
    }
}
