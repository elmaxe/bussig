using Npgsql;

namespace Bussig.Tests.Integration;

public static class QueryHelpers
{
    public static async IAsyncEnumerable<string> ReadAsListAsync(this NpgsqlCommand command)
    {
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            yield return reader.GetString(0);
        }
    }
}
