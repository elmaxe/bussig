namespace Bussig.Postgres;

public class PsqlStatements
{
    public const string SendMessage =
        """SELECT * FROM "{0}".send_message($1, $2, $3, $4, $5, $6, $7, $8, $9);""";

    public const string CreateQueue = """SELECT * FROM "{0}".create_queue($1, $2);""";

    public const string GetMessages = """SELECT * FROM "{0}".get_messages($1, $2, $3, $4);""";
    public const string CompleteMessage = """SELECT * FROM "{0}".complete_message($1, $2);""";
    public const string AbandonMessage = """SELECT * FROM "{0}".abandon_message($1, $2, $3, $4);""";
    public const string DeadLetterMessage =
        """SELECT * FROM "{0}".deadletter_message($1, $2, $3, $4);""";

    public const string RenewMessageLock =
        """SELECT * FROM "{0}".renew_message_lock($1, $2, $3);""";

    public const string CancelScheduledMessage =
        """SELECT * FROM "{0}".cancel_scheduled_message($1);""";
}
