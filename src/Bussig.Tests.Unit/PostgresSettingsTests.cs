using Bussig.Configuration;

namespace Bussig.Tests.Unit;

public class PostgresSettingsTests
{
    [Test]
    public async Task Apply_ExtractsDatabaseFromConnectionString()
    {
        // Arrange
        var settings = new PostgresSettings { ConnectionString = "Host=localhost;Database=mydb" };

        // Act
        settings.Apply();

        // Assert
        await Assert.That(settings.Database).IsEqualTo("mydb");
    }

    [Test]
    public async Task Apply_ExtractsHostFromConnectionString()
    {
        // Arrange
        var settings = new PostgresSettings
        {
            ConnectionString = "Host=dbserver.example.com;Database=mydb",
        };

        // Act
        settings.Apply();

        // Assert
        await Assert.That(settings.Host).IsEqualTo("dbserver.example.com");
    }

    [Test]
    public async Task Apply_ExtractsUsernameFromConnectionString()
    {
        // Arrange
        var settings = new PostgresSettings
        {
            ConnectionString = "Host=localhost;User Id=admin;Database=mydb",
        };

        // Act
        settings.Apply();

        // Assert
        await Assert.That(settings.Username).IsEqualTo("admin");
    }

    [Test]
    public async Task Apply_ExtractsPasswordFromConnectionString()
    {
        // Arrange
        var settings = new PostgresSettings
        {
            ConnectionString = "Host=localhost;Password=secret;Database=mydb",
        };

        // Act
        settings.Apply();

        // Assert
        await Assert.That(settings.Password).IsEqualTo("secret");
    }

    [Test]
    public async Task Apply_ExtractsPortFromConnectionString()
    {
        // Arrange
        var settings = new PostgresSettings
        {
            ConnectionString = "Host=localhost;Port=5433;Database=mydb",
        };

        // Act
        settings.Apply();

        // Assert
        await Assert.That(settings.Port).IsEqualTo(5433);
    }

    [Test]
    public async Task Apply_ExtractsSchemaFromSearchPath()
    {
        // Arrange
        var settings = new PostgresSettings
        {
            ConnectionString = "Host=localhost;Database=mydb;Search Path=custom_schema",
        };

        // Act
        settings.Apply();

        // Assert
        await Assert.That(settings.Schema).IsEqualTo("custom_schema");
    }

    [Test]
    public async Task Apply_UsesDefaultSchema_WhenSearchPathNotInConnectionString()
    {
        // Arrange
        var settings = new PostgresSettings { ConnectionString = "Host=localhost;Database=mydb" };

        // Act
        settings.Apply();

        // Assert
        await Assert.That(settings.Schema).IsEqualTo("bussig");
    }

    [Test]
    public async Task Apply_PreservesExplicitDatabase_WhenAlsoInConnectionString()
    {
        // Arrange
        var settings = new PostgresSettings
        {
            ConnectionString = "Host=localhost;Database=connstring_db",
            Database = "explicit_db",
        };

        // Act
        settings.Apply();

        // Assert
        await Assert.That(settings.Database).IsEqualTo("explicit_db");
    }

    [Test]
    public async Task Apply_PreservesExplicitHost_WhenAlsoInConnectionString()
    {
        // Arrange
        var settings = new PostgresSettings
        {
            ConnectionString = "Host=connstring_host;Database=mydb",
            Host = "explicit_host",
        };

        // Act
        settings.Apply();

        // Assert
        await Assert.That(settings.Host).IsEqualTo("explicit_host");
    }

    [Test]
    public async Task Apply_PreservesExplicitPort_WhenAlsoInConnectionString()
    {
        // Arrange
        var settings = new PostgresSettings
        {
            ConnectionString = "Host=localhost;Port=5433;Database=mydb",
            Port = 5434,
        };

        // Act
        settings.Apply();

        // Assert
        await Assert.That(settings.Port).IsEqualTo(5434);
    }

    [Test]
    public async Task Apply_SetsDefaultPort_WhenPortNotInConnectionString()
    {
        // Arrange
        var settings = new PostgresSettings { ConnectionString = "Host=localhost;Database=mydb" };

        // Act
        settings.Apply();

        // Assert
        await Assert.That(settings.Port).IsEqualTo(5432);
    }

    [Test]
    public async Task Apply_ExtractsAllProperties_FromFullConnectionString()
    {
        // Arrange
        var settings = new PostgresSettings
        {
            ConnectionString =
                "Host=dbserver;Port=5433;User Id=admin;Password=secret;Database=testdb;Search Path=myschema",
        };

        // Act
        settings.Apply();

        // Assert
        await Assert.That(settings.Host).IsEqualTo("dbserver");
        await Assert.That(settings.Port).IsEqualTo(5433);
        await Assert.That(settings.Username).IsEqualTo("admin");
        await Assert.That(settings.Password).IsEqualTo("secret");
        await Assert.That(settings.Database).IsEqualTo("testdb");
        await Assert.That(settings.Schema).IsEqualTo("myschema");
    }

    [Test]
    public async Task Apply_UpdatesConnectionString_WithExplicitValues()
    {
        // Arrange
        var settings = new PostgresSettings
        {
            ConnectionString = "Host=localhost;Database=original_db",
            Database = "explicit_db",
        };

        // Act
        settings.Apply();

        // Assert
        await Assert.That(settings.ConnectionString).Contains("Database=explicit_db");
    }

    [Test]
    public async Task Apply_UpdatesConnectionString_WithSchema()
    {
        // Arrange
        var settings = new PostgresSettings { ConnectionString = "Host=localhost;Database=mydb" };

        // Act
        settings.Apply();

        // Assert
        await Assert.That(settings.ConnectionString).Contains("Search Path=bussig");
    }
}
