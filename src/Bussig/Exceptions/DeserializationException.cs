namespace Bussig.Exceptions;

public sealed class DeserializationException(string message, Exception? innerException = null)
    : BussigException(message, innerException);
