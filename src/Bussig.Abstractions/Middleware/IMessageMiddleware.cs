namespace Bussig.Abstractions.Middleware;

/// <summary>
/// Delegate representing the next middleware in the pipeline.
/// </summary>
public delegate Task MessageMiddlewareDelegate(MessageContext context);

/// <summary>
/// Interface for message processing middleware.
/// Middleware can inspect, modify, or short-circuit message processing.
/// </summary>
public interface IMessageMiddleware
{
    /// <summary>
    /// Processes a message in the pipeline.
    /// </summary>
    /// <param name="context">The message context containing message data and state.</param>
    /// <param name="nextMiddleware">The delegate to invoke the next middleware in the pipeline.</param>
    Task InvokeAsync(MessageContext context, MessageMiddlewareDelegate nextMiddleware);
}
