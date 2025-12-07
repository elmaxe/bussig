using Microsoft.Extensions.Logging;
using Npgsql;

namespace Bussig.Postgres;

public static class TransportConstants
{
    public const string DefaultSchemaName = "bussig";
}

public enum PostgresVersion
{
    Pg15 = 15,
    Pg16 = 16,
    Pg17 = 17,
    Pg18 = 18,
}

public class TransportOptions
{
    public string SchemaName { get; set; } = TransportConstants.DefaultSchemaName;
    public PostgresVersion PostgresVersion { get; set; }
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

        await using (
            var command = new NpgsqlCommand(
                CreateUuidFunctionSqlCommand(options),
                connection,
                transaction
            )
        )
        {
            await command.ExecuteScalarAsync(cancellationToken);
        }
        var formattedCmdStr = string.Format(CreateInfrastructureSqlCommand, options.SchemaName);
        await using (var command = new NpgsqlCommand(formattedCmdStr, connection, transaction))
        {
            await command.ExecuteScalarAsync(cancellationToken);
        }

        await transaction.CommitAsync(CancellationToken.None);

        logger.LogDebug("Transport infrastructure in schema {Schema} upserted", options.SchemaName);
    }

    private const string CreateSchemaSqlCommand =
        //lang=postgresql
        """
            CREATE SCHEMA IF NOT EXISTS "{0}";
            """;

    private static string CreateUuidFunctionSqlCommand(TransportOptions options) =>
        //lang=postgresql
        $"""
            CREATE OR REPLACE FUNCTION "{options.SchemaName}".genuuid() RETURNS UUID AS
            $$
            BEGIN
                {(
                options.PostgresVersion >= PostgresVersion.Pg18
                    ? "SELECT pg_catalog.uuidv7()"
                    : "SELECT pg_catalog.gen_random_uuid()"
            )};
            END;
            $$ LANGUAGE plpgsql;
            """;

    private const string CreateInfrastructureSqlCommand =
        //lang=postgresql
        """
            CREATE SEQUENCE IF NOT EXISTS "{0}".topology_seq AS BIGINT;        

            CREATE TABLE IF NOT EXISTS "{0}".queues(
                    queue_id                BIGINT          NOT NULL PRIMARY KEY DEFAULT nextval('"{0}".topology_seq')
                ,   name                    TEXT            NOT NULL
                ,   type                    INTEGER         NOT NULL -- 1=Main, 2=DLQ
                ,   max_delivery_count      INTEGER         NOT NULL DEFAULT 3
                ,   updated_at              TIMESTAMPTZ     NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
            );

            CREATE UNIQUE INDEX IF NOT EXISTS queues_idx_name_type ON "{0}".queues (name, type);

            CREATE TABLE IF NOT EXISTS "{0}".message (
                    message_id              UUID            NOT NULL PRIMARY KEY
                ,   body                    BYTEA           NULL
                ,   headers                 JSONB           NULL
                ,   message_version         INTEGER         NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS "{0}".message_delivery (
                    message_delivery_id     BIGINT          PRIMARY KEY GENERATED ALWAYS AS IDENTITY
                ,   message_id              UUID            REFERENCES "{0}".message(message_id)
                ,   queue_id                BIGINT          REFERENCES "{0}".queues(queue_id)
                ,   priority                SMALLINT        NOT NULL --low is high prio
                ,   visible_at              TIMESTAMPTZ     NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
                ,   enqueued_at             TIMESTAMPTZ     NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc') -- info
                ,   delivery_count          INTEGER         NOT NULL DEFAULT 0
                ,   max_delivery_count      INTEGER         NOT NULL 
                ,   expiration_time         TIMESTAMPTZ     NULL
                ,   lock_id                 UUID            NULL
                ,   lock_until              TIMESTAMPTZ     NULL
            );

            CREATE INDEX IF NOT EXISTS message_delivery_idx_fetch ON "{0}".message_delivery (queue_id, priority ASC, visible_at, message_delivery_id);
            CREATE INDEX IF NOT EXISTS message_delivery_idx_message_id ON "{0}".message_delivery (message_id);

            CREATE OR REPLACE FUNCTION "{0}".send_message(
                    queue_name      TEXT
                ,   message_id      UUID        DEFAULT "{0}".genuuid()
                ,   priority        INTEGER     DEFAULT NULL
                ,   body            BYTEA       DEFAULT NULL
                ,   delay           INTERVAL    DEFAULT INTERVAL '0 seconds'
                ,   headers         JSONB       DEFAULT NULL
                ,   message_version INTEGER     DEFAULT 0
                ,   expiration_time TIMESTAMPTZ DEFAULT NULL
            )
                RETURNS BIGINT AS
            $$
            DECLARE
                v_queue_id              BIGINT;
                v_max_delivery_count    INTEGER;
                v_visible_at            TIMESTAMPTZ;
                v_enqueued_at           TIMESTAMPTZ;
            BEGIN
                SELECT INTO v_queue_id, v_max_delivery_count q.queue_id, q.max_delivery_count FROM "{0}".queues q WHERE name = queue_name AND type = 1;
                
                IF v_queue_id IS NULL THEN
                    RAISE EXCEPTION 'Queue not found';
                END IF;
                
                v_visible_at := (NOW() AT TIME ZONE 'utc');
                v_enqueued_at := v_visible_at;
                IF delay > INTERVAL '0 seconds' THEN
                    v_visible_at = v_visible_at + delay;
                END IF;
                
                INSERT INTO "{0}".message(message_id, body, headers, message_version)
                VALUES (send_message.message_id, send_message.body, send_message.headers, send_message.message_version);
                
                INSERT INTO "{0}".message_delivery(message_id, queue_id, priority, visible_at, enqueued_at, delivery_count, max_delivery_count, expiration_time)
                VALUES (send_message.message_id, v_queue_id, send_message.priority, v_visible_at, v_enqueued_at, 0, v_max_delivery_count, send_message.expiration_time);

                RETURN 1;
            END;
            $$ LANGUAGE plpgsql;
            """;
}
