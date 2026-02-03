namespace Bussig.Exceptions;

public sealed class BussigConfigurationException(string message, Exception? exception = null)
    : BussigException(message, exception);
