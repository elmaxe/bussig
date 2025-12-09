namespace Bussig.Postgres;

public class PsqlStatements
{
    public const string SendMessage =
        """SELECT "{0}".send_message($1, $2, $3, $4, $5, $6, $8, $9);""";

    public const string CreateQueue = """SELECT "{0}".create_queue($1, $2);""";

    public const string GetMessages = """SELECT "{0}".get_messages($1, $2, $3, $4);""";
}
