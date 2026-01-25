using System.Globalization;
using Bussig.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Bussig.Tests.Integration.PostgresMigrator;

public class PostgresMigratorTests
{
    [ClassDataSource<PostgresContainerPool>(Shared = SharedType.PerClass)]
    public required PostgresContainerPool Containers { get; set; }

    [Test]
    public async Task CreateDatabase_CreatesNewDatabase()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var databaseName = $"testdb_{Guid.NewGuid():N}";

        var settings = PostgresSettingsFactory.Build(
            new PostgresSettings
            {
                ConnectionString = container.GetConnectionString(),
                Database = databaseName,
            }
        );

        var migrator = new Bussig.PostgresMigrator(
            NpgsqlDataSource.Create(container.GetConnectionString()),
            Options.Create(settings),
            Mock.Of<ILogger<Bussig.PostgresMigrator>>()
        );

        // Act
        await migrator.CreateDatabase(CancellationToken.None);

        // Assert
        var exists = await DatabaseExistsAsync(container, databaseName);
        await Assert.That(exists).IsTrue();
    }

    [Test]
    public async Task CreateDatabase_WhenDatabaseExists_DoesNotThrow()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var databaseName = $"testdb_{Guid.NewGuid():N}";

        var settings = PostgresSettingsFactory.Build(
            new PostgresSettings
            {
                ConnectionString = container.GetConnectionString(),
                Database = databaseName,
            }
        );

        var migrator = new Bussig.PostgresMigrator(
            NpgsqlDataSource.Create(container.GetConnectionString()),
            Options.Create(settings),
            Mock.Of<ILogger<Bussig.PostgresMigrator>>()
        );

        await migrator.CreateDatabase(CancellationToken.None);

        // Act - call again
        var action = async () => await migrator.CreateDatabase(CancellationToken.None);

        // Assert
        await Assert.That(action).ThrowsNothing();
    }

    [Test]
    public async Task CreateSchema_CreatesNewSchema()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schemaName = $"schema_{Guid.NewGuid():N}";

        var settings = PostgresSettingsFactory.Build(
            new PostgresSettings
            {
                ConnectionString = container.GetConnectionString(),
                Schema = schemaName,
            }
        );

        var dataSource = NpgsqlDataSource.Create(settings.ConnectionString);
        var migrator = new Bussig.PostgresMigrator(
            dataSource,
            Options.Create(settings),
            Mock.Of<ILogger<Bussig.PostgresMigrator>>()
        );

        // Act
        await migrator.CreateSchema(CancellationToken.None);

        // Assert
        var exists = await SchemaExistsAsync(container, schemaName);
        await Assert.That(exists).IsTrue();
    }

    [Test]
    public async Task CreateSchema_WhenSchemaExists_DoesNotThrow()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var schemaName = $"schema_{Guid.NewGuid():N}";

        var settings = PostgresSettingsFactory.Build(
            new PostgresSettings
            {
                ConnectionString = container.GetConnectionString(),
                Schema = schemaName,
            }
        );

        var dataSource = NpgsqlDataSource.Create(settings.ConnectionString);
        var migrator = new Bussig.PostgresMigrator(
            dataSource,
            Options.Create(settings),
            Mock.Of<ILogger<Bussig.PostgresMigrator>>()
        );

        await migrator.CreateSchema(CancellationToken.None);

        // Act - call again
        var action = async () => await migrator.CreateSchema(CancellationToken.None);

        // Assert
        await Assert.That(action).ThrowsNothing();
    }

    [Test]
    public async Task DeleteDatabase_RemovesDatabase()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var databaseName = $"testdb_{Guid.NewGuid():N}";

        var settings = PostgresSettingsFactory.Build(
            new PostgresSettings
            {
                ConnectionString = container.GetConnectionString(),
                Database = databaseName,
            }
        );

        // Create the database first using a connection to the postgres database
        await CreateDatabaseAsync(container, databaseName);
        var existsBefore = await DatabaseExistsAsync(container, databaseName);

        // Use a connection to the postgres database (not the one being deleted)
        var adminConnectionString = new NpgsqlConnectionStringBuilder(
            container.GetConnectionString()
        )
        {
            Database = "postgres",
        }.ToString();

        var migrator = new Bussig.PostgresMigrator(
            NpgsqlDataSource.Create(adminConnectionString),
            Options.Create(settings),
            Mock.Of<ILogger<Bussig.PostgresMigrator>>()
        );

        // Act
        await migrator.DeleteDatabase(CancellationToken.None);

        // Assert
        var existsAfter = await DatabaseExistsAsync(container, databaseName);
        await Assert.That(existsBefore).IsTrue();
        await Assert.That(existsAfter).IsFalse();
    }

    [Test]
    public async Task DeleteDatabase_WhenDatabaseDoesNotExist_DoesNotThrow()
    {
        // Arrange
        var container = await Containers.GetContainerAsync();
        var databaseName = $"nonexistent_{Guid.NewGuid():N}";

        var settings = PostgresSettingsFactory.Build(
            new PostgresSettings
            {
                ConnectionString = container.GetConnectionString(),
                Database = databaseName,
            }
        );

        var adminConnectionString = new NpgsqlConnectionStringBuilder(
            container.GetConnectionString()
        )
        {
            Database = "postgres",
        }.ToString();

        var migrator = new Bussig.PostgresMigrator(
            NpgsqlDataSource.Create(adminConnectionString),
            Options.Create(settings),
            Mock.Of<ILogger<Bussig.PostgresMigrator>>()
        );

        // Act
        var action = async () => await migrator.DeleteDatabase(CancellationToken.None);

        // Assert
        await Assert.That(action).ThrowsNothing();
    }

    private static async Task<bool> DatabaseExistsAsync(
        PostgreSqlContainer container,
        string databaseName
    )
    {
        await using var connection = new NpgsqlConnection(container.GetConnectionString());
        await connection.OpenAsync();

        await using var command = new NpgsqlCommand(
            "SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname = $1;",
            connection
        );
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });

        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt64(result, CultureInfo.InvariantCulture) == 1;
    }

    private static async Task<bool> SchemaExistsAsync(
        PostgreSqlContainer container,
        string schemaName
    )
    {
        await using var connection = new NpgsqlConnection(container.GetConnectionString());
        await connection.OpenAsync();

        await using var command = new NpgsqlCommand(
            "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = $1;",
            connection
        );
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = schemaName });

        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt64(result, CultureInfo.InvariantCulture) == 1;
    }

    private static async Task CreateDatabaseAsync(
        PostgreSqlContainer container,
        string databaseName
    )
    {
        await using var connection = new NpgsqlConnection(container.GetConnectionString());
        await connection.OpenAsync();

        await using var command = new NpgsqlCommand(
            $"""CREATE DATABASE "{databaseName}";""",
            connection
        );
        await command.ExecuteNonQueryAsync();
    }
}
