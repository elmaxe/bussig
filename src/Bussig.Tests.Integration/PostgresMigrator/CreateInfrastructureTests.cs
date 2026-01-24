using System.Collections.Concurrent;
using Bussig.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Npgsql;
using Testcontainers.PostgreSql;
using TUnit.Core.Interfaces;

namespace Bussig.Tests.Integration.PostgresMigrator;

public sealed class PostgresContainerPool : IAsyncInitializer, IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, PostgreSqlContainer> _containers = new();

    public async Task<PostgreSqlContainer> GetContainerAsync(
        string image = "postgres:18",
        CancellationToken ct = default
    )
    {
        var container = _containers.GetOrAdd(
            image,
            img => new PostgreSqlBuilder().WithImage(img).Build()
        );
        await container.StartAsync(ct);

        return container;
    }

    public Task InitializeAsync()
    {
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var container in _containers.Values)
        {
            await container.DisposeAsync();
        }
    }
}

public class CreateInfrastructureTests
{
    [ClassDataSource<PostgresContainerPool>(Shared = SharedType.PerClass)]
    public required PostgresContainerPool Containers { get; set; }

    [Test]
    [Arguments("postgres:15")]
    [Arguments("postgres:16")]
    [Arguments("postgres:17")]
    [Arguments("postgres:18")]
    public async Task RunsForSupportedVersions(string image)
    {
        // Arrange
        var container = await Containers.GetContainerAsync(image);

        var options = PostgresSettingsFactory.Build(
            new PostgresSettings { ConnectionString = container.GetConnectionString() }
        );
        var target = new Bussig.PostgresMigrator(
            NpgsqlDataSource.Create(container.GetConnectionString()),
            Options.Create(options),
            Mock.Of<ILogger<Bussig.PostgresMigrator>>()
        );

        await target.CreateSchema(CancellationToken.None);

        // Act
        var action = async () => await target.CreateInfrastructure(CancellationToken.None);

        // Assert
        using var _ = Assert.Multiple();
        await Assert.That(action).ThrowsNothing();
    }
}
