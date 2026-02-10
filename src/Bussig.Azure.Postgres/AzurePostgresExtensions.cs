using Azure.Core;
using Bussig.Abstractions;

namespace Bussig.Azure.Postgres;

public static class AzurePostgresExtensions
{
    private const string AzurePostgresScope = "https://ossrdbms-aad.database.windows.net/.default";

    public static void UseAzureEntraAuthentication(
        this IBussigRegistrationConfigurator configurator,
        Action<AzurePostgresOptions, IServiceProvider>? configure = null
    )
    {
        if (configurator is BussigRegistrationConfigurator impl)
        {
            impl.ConfigureDataSource = (builder, serviceProvider) =>
            {
                var options = new AzurePostgresOptions();
                configure?.Invoke(options, serviceProvider);

                builder.UsePeriodicPasswordProvider(
                    async (_, ct) =>
                    {
                        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                        cts.CancelAfter(options.RefreshTimeoutInterval);

                        var token = await options.TokenCredential.GetTokenAsync(
                            new TokenRequestContext([AzurePostgresScope]),
                            cts.Token
                        );

                        return token.Token;
                    },
                    options.SuccessRefreshInterval,
                    options.FailureRefreshInterval
                );
            };
        }
    }
}
