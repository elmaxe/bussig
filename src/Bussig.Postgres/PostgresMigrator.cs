using Bussig.Constants;
using Bussig.Postgres.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Bussig.Postgres;

public class TransportOptions
{
    public string SchemaName { get; set; } = TransportConstants.DefaultSchemaName;
}

public class PostgresMigrator(
    [FromKeyedServices(ServiceKeys.BussigNpgsql)] NpgsqlDataSource npgsqlDataSource,
    ILogger<PostgresMigrator> logger
)
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

    // TODO: Move this to a .sql file instead
    private const string CreateInfrastructureSqlCommand =
        //lang=postgresql
        """
            CREATE OR REPLACE FUNCTION "{0}".add_constraint_if_not_exists(
                    a_table_name            TEXT
                ,   a_constraint_name       TEXT
                ,   a_constraint_sql         TEXT
            ) RETURNS VOID AS
            $$
            BEGIN
                IF NOT EXISTS (
                    SELECT constraint_name FROM information_schema.constraint_column_usage ccu
                    WHERE ccu.table_name = a_table_name AND ccu.table_schema = '{0}'
                    AND ccu.constraint_name = a_constraint_name AND ccu.constraint_schema = '{0}'
                ) THEN EXECUTE a_constraint_sql;
                END IF;
            END;
            $$ LANGUAGE plpgsql;

            CREATE SEQUENCE IF NOT EXISTS "{0}".topology_seq AS BIGINT;        

            CREATE TABLE IF NOT EXISTS "{0}".queues(
                    queue_id                BIGINT          NOT NULL PRIMARY KEY DEFAULT nextval('"{0}".topology_seq')
                ,   name                    TEXT            NOT NULL
                ,   type                    SMALLINT        NOT NULL -- 1=Main, 2=DLQ
                ,   max_delivery_count      INTEGER         NOT NULL DEFAULT 3
                ,   updated_at              TIMESTAMPTZ     NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
            );
            CREATE UNIQUE INDEX IF NOT EXISTS queues_idx_name_type ON "{0}".queues (name, type) INCLUDE (queue_id);
            SELECT "{0}".add_constraint_if_not_exists('queues', 'unique_queue', 'ALTER TABLE "{0}".queues ADD CONSTRAINT unique_queue UNIQUE USING INDEX queues_idx_name_type;');

            CREATE TABLE IF NOT EXISTS "{0}".messages (
                    message_id              UUID            NOT NULL PRIMARY KEY
                ,   body                    BYTEA           NULL
                ,   headers                 JSONB           NULL
                ,   message_version         INTEGER         NOT NULL DEFAULT 0
                ,   scheduling_token_id     UUID            NULL                    -- used when scheduling. Needs to be used when cancelling a scheduled message
            );

            CREATE INDEX IF NOT EXISTS messages_idx_schedule_token ON "{0}".messages (scheduling_token_id) WHERE messages.scheduling_token_id IS NOT NULL;         

            CREATE TABLE IF NOT EXISTS "{0}".message_delivery (
                    message_delivery_id         BIGINT          PRIMARY KEY GENERATED ALWAYS AS IDENTITY
                ,   message_id                  UUID            NOT NULL REFERENCES "{0}".messages(message_id) ON DELETE CASCADE
                ,   queue_id                    BIGINT          NOT NULL REFERENCES "{0}".queues(queue_id)
                ,   lock_id                     UUID            NULL
                ,   priority                    SMALLINT        NOT NULL                                    --low is high prio
                ,   visible_at                  TIMESTAMPTZ     NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc') -- when message is available for processing (scheduled or lock is expired)
                ,   enqueued_at                 TIMESTAMPTZ     NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc') -- info
                ,   last_delivered_at           TIMESTAMPTZ     NULL                                        -- when was message last delivered (processed)
                ,   delivery_count              INTEGER         NOT NULL DEFAULT 0
                ,   max_delivery_count          INTEGER         NOT NULL 
                ,   expiration_time             TIMESTAMPTZ     NULL                                        -- auto delete date
                ,   message_delivery_headers    JSONB           NULL
                -- todo: add consumer id (purely informational), either session based or something you can set in config. set it when fetching messages
            );

            CREATE INDEX IF NOT EXISTS message_delivery_idx_fetch ON "{0}".message_delivery (queue_id, priority ASC, visible_at, message_delivery_id);
            CREATE INDEX IF NOT EXISTS message_delivery_idx_message_id ON "{0}".message_delivery (message_id);

            CREATE TABLE IF NOT EXISTS "{0}".distributed_locks (
                    lock_id             TEXT            PRIMARY KEY NOT NULL
                ,   expires_at          TIMESTAMPTZ     NOT NULL
                ,   acquired_at         TIMESTAMPTZ     NOT NULL
                ,   owner_token         UUID            NOT NULL
                ,   extended_times      INTEGER         NOT NULL DEFAULT 0
            );

            CREATE OR REPLACE FUNCTION "{0}".get_messages(
                    a_queue_name          TEXT
                ,   a_lock_id             UUID
                ,   a_lock_duration       INTERVAL
                ,   a_count               INTEGER DEFAULT 1
            ) RETURNS TABLE (
                    message_id                  UUID
                ,   message_delivery_id         BIGINT
                ,   priority                    SMALLINT
                ,   queue_id                    BIGINT
                ,   visible_at                  TIMESTAMPTZ
                ,   body                        BYTEA
                ,   headers                     JSONB
                ,   message_delivery_headers    JSONB
                ,   message_version             INTEGER
                ,   scheduling_token_id         UUID
                ,   lock_id                     UUID
                ,   enqueued_at                 TIMESTAMPTZ
                ,   last_delivered_at           TIMESTAMPTZ
                ,   delivery_count              INTEGER
                ,   max_delivery_count          INTEGER
                ,   expiration_time             TIMESTAMPTZ
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
                    WHERE md.queue_id = v_queue_id
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
                    umd.message_delivery_headers,
                    m.message_version,
                    m.scheduling_token_id,
                    umd.lock_id,
                    umd.enqueued_at,
                    umd.last_delivered_at,
                    umd.delivery_count,
                    umd.max_delivery_count,
                    umd.expiration_time;
            END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION "{0}".complete_message(
                    a_message_delivery_id   BIGINT
                ,   a_lock_id               UUID
            ) RETURNS BIGINT AS
            $$
            DECLARE
                v_message_delivery_id   BIGINT;
                v_message_id            UUID;
                v_queue_id              BIGINT;
            BEGIN
                
                DELETE FROM "{0}".message_delivery md WHERE md.message_delivery_id = a_message_delivery_id AND md.lock_id = a_lock_id
                RETURNING md.message_delivery_id, md.message_id, md.queue_id INTO v_message_delivery_id, v_message_id, v_queue_id;
                
                IF v_message_id IS NOT NULL THEN
                    DELETE FROM "{0}".messages m WHERE m.message_id = v_message_id
                    AND NOT EXISTS(SELECT FROM "{0}".message_delivery md WHERE md.message_id = v_message_id);
                END IF;
                
                RETURN v_message_delivery_id;
            END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION "{0}".abandon_message(
                    a_message_delivery_id   BIGINT
                ,   a_lock_id               UUID
                ,   a_headers               JSONB
                ,   a_delay                 INTERVAL DEFAULT INTERVAL '0 seconds'
            ) RETURNS BIGINT AS
            $$
            DECLARE
                v_visible_at            TIMESTAMPTZ;
                v_message_delivery_id   BIGINT;
            BEGIN
                v_visible_at := (NOW() AT TIME ZONE 'utc');
                IF a_delay > INTERVAL '0 seconds' THEN
                    v_visible_at := v_visible_at + a_delay;
                END IF;
                
                UPDATE "{0}".message_delivery md
                SET lock_id = NULL, visible_at = v_visible_at, message_delivery_headers = a_headers
                WHERE md.message_delivery_id = a_message_delivery_id AND md.lock_id = a_lock_id
                RETURNING md.message_delivery_id
                INTO v_message_delivery_id;

                RETURN v_message_delivery_id;
            END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION "{0}".move_message(
                    a_message_delivery_id       BIGINT
                ,   a_lock_id                   UUID
                ,   a_queue_type                SMALLINT
                ,   a_queue_name                TEXT
                ,   a_headers                   JSONB
                ,   a_delay                     INTERVAL DEFAULT INTERVAL '0 seconds'
            ) RETURNS BIGINT AS
            $$
            DECLARE
                vl_dest_queue_id        BIGINT;
                v_visible_at            TIMESTAMPTZ;
                v_message_delivery_id   BIGINT;
            BEGIN
                SELECT q.queue_id INTO vl_dest_queue_id FROM "{0}".queues q WHERE q.name = a_queue_name AND q.type = a_queue_type;
                IF vl_dest_queue_id IS NULL THEN
                    RAISE EXCEPTION 'Queue with name %s of type %s not found', a_queue_name, a_queue_type;
                END IF;
                
                v_visible_at := (NOW() AT TIME ZONE  'utc');
                IF a_delay > INTERVAL '0 seconds' THEN
                    v_visible_at = v_visible_at + a_delay;
                END IF;

                UPDATE "{0}".message_delivery md
                SET queue_id = vl_dest_queue_id, visible_at = v_visible_at, lock_id = NULL, message_delivery_headers = a_headers
                WHERE md.message_delivery_id = a_message_delivery_id AND md.lock_id = a_lock_id
                RETURNING md.message_delivery_id
                INTO v_message_delivery_id;
                
                RETURN v_message_delivery_id;
            END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION "{0}".deadletter_message(
                    a_message_delivery_id       BIGINT
                ,   a_lock_id                   UUID
                ,   a_queue_name                TEXT
                ,   a_headers                   JSONB
            ) RETURNS BIGINT AS
            $$
            BEGIN
                RETURN "{0}".move_message(a_message_delivery_id, a_lock_id, 2::SMALLINT, a_queue_name, a_headers);
            END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION "{0}".renew_message_lock(
                    a_message_delivery_id   BIGINT
                ,   a_lock_id               UUID
                ,   a_duration              INTERVAL
            ) RETURNS BIGINT AS
            $$
            DECLARE 
                v_visible_at            TIMESTAMPTZ;
                v_message_delivery_id   BIGINT;
            BEGIN
                IF a_duration IS NULL OR a_duration < INTERVAL '1 seconds' THEN
                    RAISE EXCEPTION 'Lock duration is invalid';
                END IF;
                
                v_visible_at := (NOW() AT TIME ZONE 'utc') + a_duration;
                
                UPDATE "{0}".message_delivery md
                SET visible_at = v_visible_at
                WHERE md.message_delivery_id = a_message_delivery_id AND md.lock_id = a_lock_id
                RETURNING md.message_delivery_id
                INTO v_message_delivery_id;
                
                RETURN v_message_delivery_id;
            END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION "{0}".cancel_scheduled_message(
                    a_scheduling_token_id     UUID
            ) RETURNS UUID AS
            $$
            DECLARE
                v_message_id    UUID;
            BEGIN
                DELETE FROM "{0}".messages m
                    USING "{0}".messages mm
                    LEFT JOIN "{0}".message_delivery md ON md.message_id = mm.message_id
                WHERE m.scheduling_token_id = a_scheduling_token_id
                AND mm.message_id = m.message_id
                AND md.delivery_count = 0
                AND md.lock_id IS NULL
                RETURNING m.message_id
                INTO v_message_id;
                
                RETURN v_message_id;
            END;
            $$ LANGUAGE plpgsql;

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
                    a_queue_name            TEXT
                ,   a_message_id            UUID
                ,   a_priority              SMALLINT    DEFAULT NULL
                ,   a_body                  BYTEA       DEFAULT NULL
                ,   a_delay                 INTERVAL    DEFAULT INTERVAL '0 seconds'
                ,   a_headers               JSONB       DEFAULT NULL
                ,   a_message_version       INTEGER     DEFAULT 0
                ,   a_expiration_time       TIMESTAMPTZ DEFAULT NULL
                ,   a_scheduling_token_id   UUID        DEFAULT NULL
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
                
                INSERT INTO "{0}".messages(message_id, body, headers, message_version, scheduling_token_id)
                VALUES (a_message_id, a_body, a_headers, a_message_version, a_scheduling_token_id);
                
                INSERT INTO "{0}".message_delivery(message_id, queue_id, priority, visible_at, enqueued_at, delivery_count, max_delivery_count, expiration_time)
                VALUES (a_message_id, v_queue_id, COALESCE(a_priority, 16384), v_visible_at, v_enqueued_at, 0, v_max_delivery_count, a_expiration_time);

                RETURN 1;
            END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION "{0}".acquire_lock(
                    a_lock_id               TEXT
                ,   a_duration              INTERVAL
                ,   a_owner_token           UUID
            ) RETURNS BOOLEAN AS
            $$
            DECLARE
                    v_now           TIMESTAMPTZ;
                    v_expires_at    TIMESTAMPTZ;
                    v_upserted      INTEGER;
            BEGIN
                IF a_duration < INTERVAL '1 second' THEN
                    RAISE EXCEPTION 'Duration must be greater than 0 seconds';
                END IF;
                
                v_now := (NOW() AT TIME ZONE 'utc');
                v_expires_at := v_now + a_duration;
                
                INSERT INTO "{0}".distributed_locks AS dl (lock_id, expires_at, acquired_at, owner_token)
                VALUES (a_lock_id, v_expires_at, v_now, a_owner_token)
                ON CONFLICT (lock_id) DO UPDATE SET
                    expires_at = EXCLUDED.expires_at,
                    acquired_at = EXCLUDED.acquired_at,
                    owner_token = EXCLUDED.owner_token,
                    extended_times = 0
                WHERE dl.expires_at <= v_now;
                
                GET DIAGNOSTICS v_upserted = ROW_COUNT;
                RETURN v_upserted = 1;
            END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION "{0}".release_lock(
                    a_lock_id           TEXT
                ,   a_owner_token       UUID
            ) RETURNS BOOLEAN AS
            $$
            DECLARE
                v_deleted   INTEGER;
            BEGIN
                DELETE FROM "{0}".distributed_locks
                WHERE lock_id = a_lock_id AND owner_token = a_owner_token;
                
                GET DIAGNOSTICS v_deleted = ROW_COUNT;
                RETURN v_deleted = 1;
            END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION "{0}".renew_lock(
                    a_lock_id           TEXT
                ,   a_owner_token       UUID
                ,   a_duration          INTERVAL
            ) RETURNS BOOLEAN AS
            $$
            DECLARE
                v_now           TIMESTAMPTZ := NOW() AT TIME ZONE 'utc';
                v_expires_at    TIMESTAMPTZ;
                v_updated       INTEGER;
            BEGIN
                IF a_duration < INTERVAL '1 second' THEN
                    RAISE EXCEPTION 'Duration must be greater than 0 seconds';
                END IF;
                
                v_expires_at := v_now + a_duration;
                
                UPDATE "{0}".distributed_locks dl
                SET expires_at = v_expires_at, extended_times = dl.extended_times + 1
                WHERE lock_id = a_lock_id AND owner_token = a_owner_token AND dl.expires_at > v_now;
                
                GET DIAGNOSTICS v_updated = ROW_COUNT;
                RETURN v_updated = 1;
            END;
            $$ LANGUAGE plpgsql;

            -- Management

            CREATE OR REPLACE FUNCTION "{0}".peek_deadletters(
                    a_queue_name        TEXT
                ,   a_count             BIGINT  DEFAULT NULL
            ) RETURNS TABLE (
                    queue_id                    BIGINT
                ,   queue_name                  TEXT
                ,   message_delivery_id         BIGINT
                ,   priority                    SMALLINT
                ,   enqueued_at                 TIMESTAMPTZ
                ,   last_delivered_at           TIMESTAMPTZ
                ,   delivery_count              INTEGER
                ,   max_delivery_count          INTEGER
                ,   expiration_time             TIMESTAMPTZ
                ,   message_delivery_headers    JSONB
                ,   visible_at                  TIMESTAMPTZ
                ,   message_id                  UUID
                ,   body                        BYTEA
                ,   headers                     JSONB
                ,   message_version             INTEGER
                ,   scheduling_token_id         UUID
            ) AS
            $$
            DECLARE
                v_queue_id      BIGINT;
                v_queue_name    TEXT;
            BEGIN
                SELECT q.queue_id, q.name INTO v_queue_id, v_queue_name FROM "{0}".queues q WHERE q.name = a_queue_name AND q.type = 2;
                IF v_queue_id IS NULL THEN
                    RAISE EXCEPTION 'Queue not found: %s', a_queue_name;
                END IF;
                
                RETURN QUERY
                SELECT
                        v_queue_id
                    ,   v_queue_name
                    ,   md.message_delivery_id
                    ,   md.priority
                    ,   md.enqueued_at
                    ,   md.last_delivered_at
                    ,   md.delivery_count
                    ,   md.max_delivery_count
                    ,   md.expiration_time
                    ,   md.message_delivery_headers
                    ,   md.visible_at
                    ,   m.message_id
                    ,   m.body
                    ,   m.headers
                    ,   m.message_version
                    ,   m.scheduling_token_id
                FROM "{0}".message_delivery md
                JOIN "{0}".messages m ON m.message_id = md.message_id
                WHERE md.queue_id = v_queue_id
                ORDER BY md.visible_at, md.message_delivery_id
                LIMIT COALESCE(a_count, 10);
            END;
            $$ LANGUAGE plpgsql;
            """;
}
