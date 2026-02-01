using Bussig.Abstractions.Middleware;

namespace Bussig.Abstractions.Options;

/// <summary>
/// Configuration options for processor middleware.
/// Unified middleware works for both single-message and batch processors.
/// </summary>
public sealed class MiddlewareOptions
{
    private readonly List<Type> _middlewareTypes = [];

    /// <summary>
    /// Gets the registered middleware types in order.
    /// </summary>
    public IReadOnlyList<Type> MiddlewareTypes => _middlewareTypes;

    /// <summary>
    /// Adds a middleware type to the processing pipeline.
    /// Middleware is executed in the order it is added.
    /// Works for both single-message and batch processors.
    /// </summary>
    /// <typeparam name="TMiddleware">The middleware type implementing IMessageMiddleware.</typeparam>
    /// <returns>This options instance for chaining.</returns>
    public MiddlewareOptions Use<TMiddleware>()
        where TMiddleware : class, IMessageMiddleware
    {
        _middlewareTypes.Add(typeof(TMiddleware));
        return this;
    }

    /// <summary>
    /// Adds a middleware type to the processing pipeline.
    /// Middleware is executed in the order it is added.
    /// Works for both single-message and batch processors.
    /// </summary>
    /// <param name="middlewareType">The middleware type implementing IMessageMiddleware.</param>
    /// <returns>This options instance for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown if the type does not implement IMessageMiddleware.</exception>
    public MiddlewareOptions Use(Type middlewareType)
    {
        if (!typeof(IMessageMiddleware).IsAssignableFrom(middlewareType))
        {
            throw new ArgumentException(
                $"Type {middlewareType.Name} must implement {nameof(IMessageMiddleware)}",
                nameof(middlewareType)
            );
        }

        _middlewareTypes.Add(middlewareType);
        return this;
    }
}
