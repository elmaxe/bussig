namespace Bussig.Abstractions.Middleware;

/// <summary>
/// Delegate representing the next middleware in the outgoing message pipeline.
/// </summary>
public delegate Task OutgoingMessageMiddlewareDelegate(OutgoingMessageContext context);

/// <summary>
/// Interface for outgoing message middleware.
/// Middleware can inspect, modify, or short-circuit message sending.
/// </summary>
public interface IOutgoingMessageMiddleware
{
    /// <summary>
    /// Processes an outgoing message in the pipeline.
    /// </summary>
    /// <param name="context">The outgoing message context containing message data and state.</param>
    /// <param name="nextMiddleware">The delegate to invoke the next middleware in the pipeline.</param>
    Task InvokeAsync(
        OutgoingMessageContext context,
        OutgoingMessageMiddlewareDelegate nextMiddleware
    );
}
