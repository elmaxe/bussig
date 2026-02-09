using System.Globalization;
using System.Text;

namespace Bussig.Outbox.Npgsql;

internal static class OutboxSqlStatements
{
    private static readonly CompositeFormat InsertFormat = CompositeFormat.Parse(
        """
        INSERT INTO "{0}".outbox_messages
            (message_id, queue_name, body, headers_json, priority, delay, message_version, expiration_time, scheduling_token_id)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9);
        """
    );

    private static readonly CompositeFormat SelectPendingFormat = CompositeFormat.Parse(
        """
        SELECT id, message_id, queue_name, body, headers_json, priority, delay, message_version, expiration_time, scheduling_token_id
        FROM "{0}".outbox_messages
        WHERE published_at IS NULL
        ORDER BY id
        LIMIT $1
        FOR UPDATE SKIP LOCKED;
        """
    );

    private static readonly CompositeFormat MarkPublishedFormat = CompositeFormat.Parse(
        """
        UPDATE "{0}".outbox_messages
        SET published_at = (NOW() AT TIME ZONE 'utc')
        WHERE id = $1;
        """
    );

    private static readonly CompositeFormat CancelBySchedulingTokenFormat = CompositeFormat.Parse(
        """
        DELETE FROM "{0}".outbox_messages
        WHERE scheduling_token_id = $1 AND published_at IS NULL
        RETURNING id;
        """
    );

    private static readonly CompositeFormat CleanupPublishedFormat = CompositeFormat.Parse(
        """
        DELETE FROM "{0}".outbox_messages
        WHERE published_at IS NOT NULL AND published_at < (NOW() AT TIME ZONE 'utc') - $1;
        """
    );

    public static string Insert(string schema) =>
        string.Format(CultureInfo.InvariantCulture, InsertFormat, schema);

    public static string SelectPending(string schema) =>
        string.Format(CultureInfo.InvariantCulture, SelectPendingFormat, schema);

    public static string MarkPublished(string schema) =>
        string.Format(CultureInfo.InvariantCulture, MarkPublishedFormat, schema);

    public static string CancelBySchedulingToken(string schema) =>
        string.Format(CultureInfo.InvariantCulture, CancelBySchedulingTokenFormat, schema);

    public static string CleanupPublished(string schema) =>
        string.Format(CultureInfo.InvariantCulture, CleanupPublishedFormat, schema);
}
