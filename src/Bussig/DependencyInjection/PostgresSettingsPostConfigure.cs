using Bussig.Configuration;
using Microsoft.Extensions.Options;

namespace Bussig;

internal sealed class PostgresSettingsPostConfigure : IPostConfigureOptions<PostgresSettings>
{
    public void PostConfigure(string? name, PostgresSettings options) => options.Apply();
}
