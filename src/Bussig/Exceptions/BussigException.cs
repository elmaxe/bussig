namespace Bussig.Exceptions;

public abstract class BussigException(string message, Exception? innerException = null)
    : Exception(message, innerException);
