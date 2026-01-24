using System.Text;

namespace Bussig.Postgres;

public static class PsqlStatements
{
    public static readonly CompositeFormat SendMessage = CompositeFormat.Parse(
        """SELECT * FROM "{0}".send_message($1, $2, $3, $4, $5, $6, $7, $8, $9);"""
    );

    public static readonly CompositeFormat CreateQueue = CompositeFormat.Parse(
        """SELECT * FROM "{0}".create_queue($1, $2);"""
    );

    public static readonly CompositeFormat GetMessages = CompositeFormat.Parse(
        """SELECT * FROM "{0}".get_messages($1, $2, $3, $4);"""
    );
    public static readonly CompositeFormat CompleteMessage = CompositeFormat.Parse(
        """SELECT * FROM "{0}".complete_message($1, $2);"""
    );
    public static readonly CompositeFormat AbandonMessage = CompositeFormat.Parse(
        """SELECT * FROM "{0}".abandon_message($1, $2, $3, $4);"""
    );
    public static readonly CompositeFormat DeadLetterMessage = CompositeFormat.Parse(
        """SELECT * FROM "{0}".deadletter_message($1, $2, $3, $4);"""
    );

    public static readonly CompositeFormat RenewMessageLock = CompositeFormat.Parse(
        """SELECT * FROM "{0}".renew_message_lock($1, $2, $3);"""
    );

    public static readonly CompositeFormat CancelScheduledMessage = CompositeFormat.Parse(
        """SELECT * FROM "{0}".cancel_scheduled_message($1);"""
    );

    public static readonly CompositeFormat AcquireLock = CompositeFormat.Parse(
        """SELECT * FROM "{0}".acquire_lock($1, $2, $3)"""
    );
    public static readonly CompositeFormat ReleaseLock = CompositeFormat.Parse(
        """SELECT * FROM "{0}".acquire_lock($1, $2)"""
    );
    public static readonly CompositeFormat RenewLock = CompositeFormat.Parse(
        """SELECT * FROM "{0}".acquire_lock($1, $2, $3)"""
    );
}
