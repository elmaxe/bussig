namespace Bussig.Exceptions;

public class MessageDataException(string message, Exception? e = null)
    : BussigException(message, e);
