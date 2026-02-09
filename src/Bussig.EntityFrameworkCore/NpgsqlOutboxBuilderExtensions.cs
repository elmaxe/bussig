using Bussig.EntityFrameworkCore.StatementProviders;
using Microsoft.Extensions.DependencyInjection;

namespace Bussig.EntityFrameworkCore;

public static class NpgsqlOutboxBuilderExtensions
{
    public static OutboxBuilder UseNpgsql(this OutboxBuilder builder)
    {
        builder.Services.AddSingleton<
            OutboxSqlStatementProvider,
            PostgresOutboxSqlStatementProvider
        >();
        return builder;
    }
}
