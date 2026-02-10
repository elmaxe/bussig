using Azure.Core;
using Moq;
using Npgsql;

namespace Bussig.Azure.Postgres.Tests;

public class AzurePostgresExtensionsTests
{
    [Test]
    public async Task UseAzureEntraAuthentication_SetsConfigureDataSource()
    {
        var configurator = new BussigRegistrationConfigurator();

        configurator.UseAzureEntraAuthentication();

        await Assert.That(configurator.ConfigureDataSource).IsNotNull();
    }

    [Test]
    public async Task UseAzureEntraAuthentication_ConfigureCallbackIsInvoked()
    {
        var configurator = new BussigRegistrationConfigurator();
        var serviceProvider = new Mock<IServiceProvider>().Object;
        var callbackInvoked = false;
        IServiceProvider? capturedServiceProvider = null;

        configurator.UseAzureEntraAuthentication(
            (options, sp) =>
            {
                callbackInvoked = true;
                capturedServiceProvider = sp;
            }
        );

        var builder = new NpgsqlDataSourceBuilder("Host=localhost");
        configurator.ConfigureDataSource!(builder, serviceProvider);

        await Assert.That(callbackInvoked).IsTrue();
        await Assert.That(capturedServiceProvider).IsEqualTo(serviceProvider);
    }

    [Test]
    public async Task UseAzureEntraAuthentication_CustomTokenCredential_IsUsedWithCorrectScope()
    {
        var configurator = new BussigRegistrationConfigurator();
        var serviceProvider = new Mock<IServiceProvider>().Object;
        var mockCredential = new Mock<TokenCredential>();

        mockCredential
            .Setup(c =>
                c.GetTokenAsync(
                    It.Is<TokenRequestContext>(ctx =>
                        ctx.Scopes.Length == 1
                        && ctx.Scopes[0] == "https://ossrdbms-aad.database.windows.net/.default"
                    ),
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(new AccessToken("dummy-token", DateTimeOffset.UtcNow.AddHours(1)));

        configurator.UseAzureEntraAuthentication(
            (options, _) =>
            {
                options.TokenCredential = mockCredential.Object;
            }
        );

        var builder = new NpgsqlDataSourceBuilder("Host=localhost");
        configurator.ConfigureDataSource!(builder, serviceProvider);

        // Build triggers the periodic password provider which calls GetTokenAsync
        await using var dataSource = builder.Build();
        await using var connection = dataSource.CreateConnection();

        // Give the periodic provider a moment to fire
        await Task.Delay(500);

        mockCredential.Verify(
            c =>
                c.GetTokenAsync(
                    It.Is<TokenRequestContext>(ctx =>
                        ctx.Scopes.Length == 1
                        && ctx.Scopes[0] == "https://ossrdbms-aad.database.windows.net/.default"
                    ),
                    It.IsAny<CancellationToken>()
                ),
            Times.AtLeastOnce()
        );
    }

    [Test]
    public async Task UseAzureEntraAuthentication_CustomIntervals_ArePassedToOptions()
    {
        var configurator = new BussigRegistrationConfigurator();
        var serviceProvider = new Mock<IServiceProvider>().Object;
        TimeSpan capturedSuccess = default;
        TimeSpan capturedFailure = default;

        configurator.UseAzureEntraAuthentication(
            (options, _) =>
            {
                options.SuccessRefreshInterval = TimeSpan.FromMinutes(30);
                options.FailureRefreshInterval = TimeSpan.FromSeconds(10);
                capturedSuccess = options.SuccessRefreshInterval;
                capturedFailure = options.FailureRefreshInterval;
            }
        );

        var builder = new NpgsqlDataSourceBuilder("Host=localhost");
        configurator.ConfigureDataSource!(builder, serviceProvider);

        await Assert.That(capturedSuccess).IsEqualTo(TimeSpan.FromMinutes(30));
        await Assert.That(capturedFailure).IsEqualTo(TimeSpan.FromSeconds(10));
    }
}
