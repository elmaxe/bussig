using Microsoft.Extensions.Logging;
using Moq;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Bussig.Postgres.Tests.Integration.PostgresMigrator;

public class GenUuidTests
{
    [ClassDataSource<PostgresContainerPool>(Shared = SharedType.PerTestSession)]
    public required PostgresContainerPool Containers { get; init; }

    // TODO: Create test fixture that spins up these containers once
    [Test]
    [Arguments(PostgresVersion.Pg15, 4)]
    [Arguments(PostgresVersion.Pg16, 4)]
    [Arguments(PostgresVersion.Pg17, 4)]
    [Arguments(PostgresVersion.Pg18, 7)]
    public async Task Function_genuuid_ReturnsExpectedUuidVersion(
        PostgresVersion pgVersion,
        int expected
    )
    {
        // Arrange
        await using var container = new PostgreSqlBuilder()
            .WithImage($"postgres:{pgVersion:D}")
            .Build();
        await container.StartAsync();
        var target = new Postgres.PostgresMigrator(
            NpgsqlDataSource.Create(container.GetConnectionString()),
            Mock.Of<ILogger<Postgres.PostgresMigrator>>()
        );
        var options = new TransportOptions { PostgresVersion = pgVersion };

        // TODO: Use respawner and reset state to after migrations are run
        await target.CreateSchema(options, CancellationToken.None);
        await target.CreateInfrastructure(options, CancellationToken.None);

        await using var dataSource = NpgsqlDataSource.Create(container.GetConnectionString());
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand("""SELECT "bussig".genuuid()""", connection);

        // Act
        var result = (Guid)(await command.ExecuteScalarAsync())!;

        // Assert
        await Assert.That(result.Version).EqualTo(expected);
    }
}
