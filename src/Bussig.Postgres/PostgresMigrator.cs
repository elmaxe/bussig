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
                    ? "RETURN pg_catalog.uuidv7()"
                    : "RETURN pg_catalog.gen_random_uuid()"
            )};
            END;
            $$ LANGUAGE plpgsql;
            """;

    // TODO: Move this to a .sql file instead
    private const string CreateInfrastructureSqlCommand =
        //lang=postgresql
        """
            CREATE SEQUENCE IF NOT EXISTS "{0}".topology_seq AS BIGINT;        

            CREATE TABLE IF NOT EXISTS "{0}".queues(
                    queue_id                BIGINT          NOT NULL PRIMARY KEY DEFAULT nextval('"{0}".topology_seq')
                ,   name                    TEXT            NOT NULL
                ,   type                    SMALLINT        NOT NULL -- 1=Main, 2=DLQ
                ,   max_delivery_count      INTEGER         NOT NULL DEFAULT 3
                ,   updated_at              TIMESTAMPTZ     NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
            );
            CREATE UNIQUE INDEX IF NOT EXISTS queues_idx_name_type ON "{0}".queues (name, type) INCLUDE (queue_id);
            ALTER TABLE "{0}".queues ADD CONSTRAINT unique_queue UNIQUE USING INDEX queues_idx_name_type;

            CREATE TABLE IF NOT EXISTS "{0}".messages (
                    message_id              UUID            NOT NULL PRIMARY KEY
                ,   body                    BYTEA           NULL
                ,   headers                 JSONB           NULL
                ,   message_version         INTEGER         NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS "{0}".message_delivery (
                    message_delivery_id     BIGINT          PRIMARY KEY GENERATED ALWAYS AS IDENTITY
                ,   message_id              UUID            REFERENCES "{0}".messages(message_id)
                ,   queue_id                BIGINT          REFERENCES "{0}".queues(queue_id)
                ,   priority                SMALLINT        NOT NULL                                    --low is high prio
                ,   visible_at              TIMESTAMPTZ     NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc') -- when message is available for processing (scheduled or lock is expired)
                ,   enqueued_at             TIMESTAMPTZ     NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc') -- info
                ,   last_delivered_at       TIMESTAMPTZ     NULL                                        -- when was message last delivered (processed)
                ,   delivery_count          INTEGER         NOT NULL DEFAULT 0
                ,   max_delivery_count      INTEGER         NOT NULL 
                ,   expiration_time         TIMESTAMPTZ     NULL                                        -- auto delete date
                ,   lock_id                 UUID            NULL
                -- todo: add consumer id (purely informational), either session based or something you can set in config. set it when fetching messages
            );

            CREATE INDEX IF NOT EXISTS message_delivery_idx_fetch ON "{0}".message_delivery (queue_id, priority ASC, visible_at, message_delivery_id);
            CREATE INDEX IF NOT EXISTS message_delivery_idx_message_id ON "{0}".message_delivery (message_id);

            CREATE OR REPLACE FUNCTION "{0}".get_messages(
                    a_queue_name          TEXT
                ,   a_lock_id             UUID
                ,   a_lock_duration       INTERVAL
                ,   a_count               INTEGER DEFAULT 1
            ) RETURNS TABLE (
                    message_id          UUID
                ,   message_delivery_id BIGINT
                ,   priority            SMALLINT
                ,   queue_id            BIGINT
                ,   visible_at          TIMESTAMPTZ
                ,   body                BYTEA
                ,   headers             JSONB
                ,   message_version     INTEGER
                ,   lock_id             UUID
                ,   enqueued_at         TIMESTAMPTZ
                ,   last_delivered_at   TIMESTAMPTZ
                ,   delivery_count      INTEGER
                ,   max_delivery_count  INTEGER
                ,   expiration_time     TIMESTAMPTZ
            ) AS
            $$
            DECLARE
                v_queue_id      BIGINT;
                v_now           TIMESTAMPTZ;
                v_visible_at    TIMESTAMPTZ;
            BEGIN
                SELECT q.queue_id INTO v_queue_id FROM "{0}".queues q WHERE q.name = a_queue_name AND q.type = 1;
                
                IF v_queue_id IS NULL THEN
                    RAISE EXCEPTION 'Queue not found: %s', a_queue_name;
                END IF;
                
                v_now := (NOW() AT TIME ZONE 'utc');
                v_visible_at := v_now + a_lock_duration;
                
                RETURN QUERY
                WITH msgs AS (
                    SELECT md.* FROM "{0}".message_delivery md
                    WHERE queue_id = v_queue_id
                    AND md.visible_at <= v_now
                    AND md.delivery_count < md.max_delivery_count
                    ORDER BY md.priority, md.visible_at, md.message_delivery_id
                    LIMIT a_count FOR UPDATE OF md SKIP LOCKED
                )
                UPDATE "{0}".message_delivery umd
                SET delivery_count = umd.delivery_count + 1,
                    lock_id = a_lock_id,
                    visible_at = v_visible_at,
                    last_delivered_at = v_now
                FROM msgs
                JOIN "{0}".messages m ON msgs.message_id = m.message_id
                WHERE umd.message_delivery_id = msgs.message_delivery_id
                RETURNING
                    m.message_id,
                    umd.message_delivery_id,
                    umd.priority,
                    umd.queue_id,
                    umd.visible_at,
                    m.body,
                    m.headers,
                    m.message_version,
                    umd.lock_id,
                    umd.enqueued_at,
                    umd.last_delivered_at,
                    umd.delivery_count,
                    umd.max_delivery_count,
                    umd.expiration_time;
            END;
            $$ LANGUAGE plpgsql;

            -- abandon message (with delay)
            -- complete message

            CREATE OR REPLACE FUNCTION "{0}".create_queue(
                    a_name                TEXT
                ,   a_max_delivery_count  INTEGER     DEFAULT NULL
            )
                RETURNS BIGINT AS
            $$
            DECLARE
                v_queue_id    BIGINT;
            BEGIN
                IF a_name IS NULL OR LENGTH(a_name) < 1 THEN
                    RAISE EXCEPTION 'Queue names must not be null or empty';
                END IF;
                
                INSERT INTO "{0}".queues (name, type, max_delivery_count) VALUES (a_name, 1, COALESCE(a_max_delivery_count, 3))
                ON CONFLICT ON CONSTRAINT unique_queue DO
                UPDATE SET
                            updated_at = (NOW() AT TIME ZONE 'utc'),
                            max_delivery_count = COALESCE(a_max_delivery_count, EXCLUDED.max_delivery_count, 3)
                RETURNING queues.queue_id INTO v_queue_id;
                
                INSERT INTO "{0}".queues (name, type, max_delivery_count) VALUES (a_name, 2, COALESCE(a_max_delivery_count, 3))
                ON CONFLICT ON CONSTRAINT unique_queue DO
                UPDATE SET
                            updated_at = (NOW() AT TIME ZONE 'utc'),
                            max_delivery_count = COALESCE(a_max_delivery_count, EXCLUDED.max_delivery_count, 3);
                
                RETURN v_queue_id;
            END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION "{0}".send_message(
                    a_queue_name        TEXT
                ,   a_message_id        UUID        DEFAULT "{0}".genuuid() -- todo remove this, generate on client instead
                ,   a_priority          INTEGER     DEFAULT NULL
                ,   a_body              BYTEA       DEFAULT NULL
                ,   a_delay             INTERVAL    DEFAULT INTERVAL '0 seconds'
                ,   a_headers           JSONB       DEFAULT NULL
                ,   a_message_version   INTEGER     DEFAULT 0
                ,   a_expiration_time   TIMESTAMPTZ DEFAULT NULL
            )
                RETURNS BIGINT AS
            $$
            DECLARE
                v_queue_id              BIGINT;
                v_max_delivery_count    INTEGER;
                v_visible_at            TIMESTAMPTZ;
                v_enqueued_at           TIMESTAMPTZ;
            BEGIN
                SELECT q.queue_id, q.max_delivery_count INTO v_queue_id, v_max_delivery_count FROM "{0}".queues q WHERE q.name = a_queue_name AND q.type = 1;
                
                IF v_queue_id IS NULL THEN
                    RAISE EXCEPTION 'Queue not found';
                END IF;
                
                v_visible_at := (NOW() AT TIME ZONE 'utc');
                v_enqueued_at := v_visible_at;
                IF a_delay > INTERVAL '0 seconds' THEN
                    v_visible_at = v_visible_at + a_delay;
                END IF;
                
                INSERT INTO "{0}".messages(message_id, body, headers, message_version)
                VALUES (a_message_id, a_body, a_headers, a_message_version);
                
                INSERT INTO "{0}".message_delivery(message_id, queue_id, priority, visible_at, enqueued_at, delivery_count, max_delivery_count, expiration_time)
                VALUES (a_message_id, v_queue_id, a_priority, v_visible_at, v_enqueued_at, 0, v_max_delivery_count, a_expiration_time);

                RETURN 1;
            END;
            $$ LANGUAGE plpgsql;
            """;
}
