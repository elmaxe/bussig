namespace Bussig.Exceptions;

public class MessageDataException(string message, Exception? e = null) : Exception(message, e);
