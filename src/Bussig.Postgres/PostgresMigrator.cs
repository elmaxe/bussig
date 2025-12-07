using Microsoft.Extensions.Logging;
using Npgsql;

namespace Bussig.Postgres;

public static class TransportConstants
{
    public const string DefaultSchemaName = "bussig";
}

public class TransportOptions
{
    public string SchemaName { get; set; } = TransportConstants.DefaultSchemaName;
}

public class PostgresMigrator(NpgsqlDataSource npgsqlDataSource, ILogger<PostgresMigrator> logger)
{
    public async Task CreateSchema(TransportOptions options, CancellationToken cancellationToken)
    {
        await using var connection = await npgsqlDataSource.OpenConnectionAsync(cancellationToken);

        await using var command = new NpgsqlCommand(
            string.Format(CreateSchemaSqlCommand, options.SchemaName),
            connection
        );
        await command.ExecuteScalarAsync(cancellationToken);

        logger.LogDebug("Schema {Schema} created", options.SchemaName);
    }

    public async Task CreateInfrastructure(
        TransportOptions options,
        CancellationToken cancellationToken
    )
    {
        await using var connection = await npgsqlDataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var formattedCmdStr = string.Format(CreateInfrastructureSqlCommand, options.SchemaName);
        await using var command = new NpgsqlCommand(formattedCmdStr, connection, transaction);
        await command.ExecuteScalarAsync(cancellationToken);

        await transaction.CommitAsync(CancellationToken.None);

        logger.LogDebug("Transport infrastructure in schema {Schema} upserted", options.SchemaName);
    }

    private const string CreateSchemaSqlCommand =
        //lang=postgresql
        """
            CREATE SCHEMA IF NOT EXISTS "{0}";
            """;

    private const string CreateInfrastructureSqlCommand =
        //lang=postgresql
        """
            CREATE SEQUENCE IF NOT EXISTS {0}.topology_seq AS BIGINT;        

            CREATE TABLE IF NOT EXISTS {0}.queues(
                    queue_id                BIGINT          NOT NULL PRIMARY KEY DEFAULT nextval('"{0}".topology_seq')
                ,   name                    TEXT            NOT NULL
                ,   type                    INTEGER         NOT NULL -- 1=Main, 2=DLQ
                ,   max_delivery_count      INTEGER         NOT NULL DEFAULT 3
                ,   updated_at              TIMESTAMPTZ     NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
            );

            CREATE UNIQUE INDEX IF NOT EXISTS queues_idx_name_type ON {0}.queues (name, type);

            CREATE TABLE IF NOT EXISTS {0}.message (
                    message_id              UUID            NOT NULL PRIMARY KEY
                ,   body                    BYTEA           NULL
                ,   headers                 JSONB           NOT NULL DEFAULT '{{}}'::JSONB
                ,   message_version         INTEGER         NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS {0}.message_delivery (
                    message_delivery_id     BIGINT          PRIMARY KEY GENERATED ALWAYS AS IDENTITY
                ,   message_id              UUID            REFERENCES {0}.message(message_id)
                ,   queue_id                BIGINT          REFERENCES {0}.queues(queue_id)
                ,   priority                SMALLINT        NOT NULL --low is high prio
                ,   visible_at              TIMESTAMPTZ     NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
                ,   enqueued_at             TIMESTAMPTZ     NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc') -- info
                ,   delivery_count          INTEGER         NOT NULL DEFAULT 0
                -- ,   max_delivery_count      INTEGER         NOT NULL 
                ,   expiration_time         TIMESTAMPTZ     NULL
                ,   lock_id                 UUID            NULL
                ,   lock_until              TIMESTAMPTZ     NULL
            );

            CREATE INDEX IF NOT EXISTS message_delivery_idx_fetch ON {0}.message_delivery (queue_id, priority ASC, visible_at, message_delivery_id);
            CREATE INDEX IF NOT EXISTS message_delivery_idx_message_id ON {0}.message_delivery (message_id);
            """;
}
