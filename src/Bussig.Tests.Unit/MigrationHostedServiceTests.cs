using Bussig.Abstractions;
using Bussig.Abstractions.Host;
using Bussig.Configuration;
using Bussig.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace Bussig.Tests.Unit;

public class MigrationHostedServiceTests
{
    private readonly Mock<IPostgresMigrator> _migratorMock;
    private readonly MigrationOptions _migrationOptions;
    private readonly PostgresSettings _postgresSettings;
    private readonly Mock<ILogger<MigrationHostedService>> _loggerMock;

    public MigrationHostedServiceTests()
    {
        _migratorMock = new Mock<IPostgresMigrator>();
        _migrationOptions = new MigrationOptions();
        _postgresSettings = new PostgresSettings
        {
            Database = "testdb",
            Schema = "testschema",
            ConnectionString = "Host=localhost;Database=testdb",
        }.Apply();
        _loggerMock = new Mock<ILogger<MigrationHostedService>>();
    }

    private MigrationHostedService CreateSut() =>
        new(
            Options.Create(_migrationOptions),
            Options.Create(_postgresSettings),
            _migratorMock.Object,
            _loggerMock.Object
        );

    [Test]
    public async Task StartAsync_WhenCreateDatabaseIsTrue_CallsCreateDatabase()
    {
        // Arrange
        _migrationOptions.CreateDatabase = true;
        var sut = CreateSut();

        // Act
        await sut.StartAsync(CancellationToken.None);

        // Assert
        _migratorMock.Verify(m => m.CreateDatabase(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Test]
    public async Task StartAsync_WhenCreateDatabaseIsFalse_DoesNotCallCreateDatabase()
    {
        // Arrange
        _migrationOptions.CreateDatabase = false;
        var sut = CreateSut();

        // Act
        await sut.StartAsync(CancellationToken.None);

        // Assert
        _migratorMock.Verify(m => m.CreateDatabase(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Test]
    public async Task StartAsync_WhenCreateSchemaIsTrue_CallsCreateSchema()
    {
        // Arrange
        _migrationOptions.CreateSchema = true;
        var sut = CreateSut();

        // Act
        await sut.StartAsync(CancellationToken.None);

        // Assert
        _migratorMock.Verify(m => m.CreateSchema(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Test]
    public async Task StartAsync_WhenCreateSchemaIsFalse_DoesNotCallCreateSchema()
    {
        // Arrange
        _migrationOptions.CreateSchema = false;
        var sut = CreateSut();

        // Act
        await sut.StartAsync(CancellationToken.None);

        // Assert
        _migratorMock.Verify(m => m.CreateSchema(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Test]
    public async Task StartAsync_WhenCreateInfrastructureIsTrue_CallsCreateInfrastructure()
    {
        // Arrange
        _migrationOptions.CreateInfrastructure = true;
        var sut = CreateSut();

        // Act
        await sut.StartAsync(CancellationToken.None);

        // Assert
        _migratorMock.Verify(
            m => m.CreateInfrastructure(It.IsAny<CancellationToken>()),
            Times.Once
        );
    }

    [Test]
    public async Task StartAsync_WhenCreateInfrastructureIsFalse_DoesNotCallCreateInfrastructure()
    {
        // Arrange
        _migrationOptions.CreateInfrastructure = false;
        var sut = CreateSut();

        // Act
        await sut.StartAsync(CancellationToken.None);

        // Assert
        _migratorMock.Verify(
            m => m.CreateInfrastructure(It.IsAny<CancellationToken>()),
            Times.Never
        );
    }

    [Test]
    public async Task StartAsync_WhenAllOptionsTrue_CallsAllMethods()
    {
        // Arrange
        _migrationOptions.CreateDatabase = true;
        _migrationOptions.CreateSchema = true;
        _migrationOptions.CreateInfrastructure = true;
        var sut = CreateSut();

        // Act
        await sut.StartAsync(CancellationToken.None);

        // Assert
        _migratorMock.Verify(m => m.CreateDatabase(It.IsAny<CancellationToken>()), Times.Once);
        _migratorMock.Verify(m => m.CreateSchema(It.IsAny<CancellationToken>()), Times.Once);
        _migratorMock.Verify(
            m => m.CreateInfrastructure(It.IsAny<CancellationToken>()),
            Times.Once
        );
    }

    [Test]
    public async Task StopAsync_WhenDeleteDatabaseIsTrue_CallsDeleteDatabase()
    {
        // Arrange
        _migrationOptions.DeleteDatabase = true;
        var sut = CreateSut();

        // Act
        await sut.StopAsync(CancellationToken.None);

        // Assert
        _migratorMock.Verify(m => m.DeleteDatabase(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Test]
    public async Task StopAsync_WhenDeleteDatabaseIsFalse_DoesNotCallDeleteDatabase()
    {
        // Arrange
        _migrationOptions.DeleteDatabase = false;
        var sut = CreateSut();

        // Act
        await sut.StopAsync(CancellationToken.None);

        // Assert
        _migratorMock.Verify(m => m.DeleteDatabase(It.IsAny<CancellationToken>()), Times.Never);
    }
}
