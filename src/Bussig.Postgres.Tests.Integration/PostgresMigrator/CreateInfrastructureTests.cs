using Microsoft.Extensions.Logging;
using Moq;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Bussig.Postgres.Tests.Integration.PostgresMigrator;

public class CreateInfrastructureTests
{
    [Test]
    [Arguments("postgres:15")]
    [Arguments("postgres:16")]
    [Arguments("postgres:17")]
    [Arguments("postgres:18")]
    public async Task RunsForSupportedVersions(string image)
    {
        // Arrange
        await using var container = new PostgreSqlBuilder().WithImage(image).Build();
        await container.StartAsync();

        var target = new Postgres.PostgresMigrator(
            NpgsqlDataSource.Create(container.GetConnectionString()),
            Mock.Of<ILogger<Postgres.PostgresMigrator>>()
        ); // TODO: FakeLogger
        var options = new TransportOptions();

        await target.CreateSchema(options, CancellationToken.None);

        // Act
        var action = async () => await target.CreateInfrastructure(options, CancellationToken.None);

        // Assert
        using var _ = Assert.Multiple();
        await Assert.That(action).ThrowsNothing();
    }
}
