using Microsoft.Extensions.Options;

namespace Bussig.Configuration;

internal sealed class PostgresSettingsPostConfigure : IPostConfigureOptions<PostgresSettings>
{
    public void PostConfigure(string? name, PostgresSettings options) => options.Apply();
}
