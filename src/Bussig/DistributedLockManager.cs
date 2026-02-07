using System.Globalization;
using Bussig.Abstractions;
using Bussig.Configuration;
using Bussig.Constants;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Bussig;

public sealed class DistributedLockManager : IDistributedLockManager
{
    private readonly NpgsqlDataSource _npgsqlDataSource;
    private readonly string _acquireLockSql;
    private readonly string _releaseLockSql;
    private readonly string _renewLockSql;

    public DistributedLockManager(
        [FromKeyedServices(ServiceKeys.BussigNpgsql)] NpgsqlDataSource npgsqlDataSource,
        IOptions<PostgresSettings> settings
    )
    {
        _npgsqlDataSource = npgsqlDataSource;
        var schema = settings.Value.Schema;

        _acquireLockSql = string.Format(
            CultureInfo.InvariantCulture,
            PsqlStatements.AcquireLock,
            schema
        );
        _releaseLockSql = string.Format(
            CultureInfo.InvariantCulture,
            PsqlStatements.ReleaseLock,
            schema
        );
        _renewLockSql = string.Format(
            CultureInfo.InvariantCulture,
            PsqlStatements.RenewLock,
            schema
        );
    }

    public async Task<bool> TryLockAsync(
        string lockId,
        Guid ownerToken,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    )
    {
        await using var conn = await _npgsqlDataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(_acquireLockSql, conn);

        // acquire_lock($1=lockId, $2=duration, $3=ownerToken)
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = lockId });
        cmd.Parameters.Add(new NpgsqlParameter<TimeSpan> { TypedValue = duration });
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = ownerToken });

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is true;
    }

    public async Task<bool> TryRenewAsync(
        string lockId,
        Guid ownerToken,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    )
    {
        await using var conn = await _npgsqlDataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(_renewLockSql, conn);

        // renew_lock($1=lockId, $2=ownerToken, $3=duration)
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = lockId });
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = ownerToken });
        cmd.Parameters.Add(new NpgsqlParameter<TimeSpan> { TypedValue = duration });

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is true;
    }

    public async Task<bool> TryReleaseAsync(
        string lockId,
        Guid ownerToken,
        CancellationToken cancellationToken = default
    )
    {
        await using var conn = await _npgsqlDataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(_releaseLockSql, conn);

        // release_lock($1=lockId, $2=ownerToken)
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = lockId });
        cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = ownerToken });

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is true;
    }
}
