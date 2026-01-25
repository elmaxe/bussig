using Bussig.Processing.Internal;
using Microsoft.Extensions.Logging;

namespace Bussig.Processing;

/// <summary>
/// Orchestrates message consumption for a single queue using a processing strategy.
/// </summary>
public sealed class QueueConsumer : IAsyncDisposable
{
    private readonly IMessageProcessingStrategy _strategy;
    private readonly string _queueName;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _concurrencySemaphore;
    private readonly CancellationTokenSource _stoppingCts = new();
    private Task? _pollingTask;

    internal QueueConsumer(
        IMessageProcessingStrategy strategy,
        string queueName,
        ILogger logger,
        SemaphoreSlim concurrencySemaphore
    )
    {
        _strategy = strategy;
        _queueName = queueName;
        _logger = logger;
        _concurrencySemaphore = concurrencySemaphore;
    }

    public void Start()
    {
        _pollingTask = _strategy.PollAsync(_stoppingCts.Token);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stoppingCts.CancelAsync();

        if (_pollingTask is not null)
        {
            try
            {
                await _pollingTask.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Expected when stopping
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None);
        _stoppingCts.Dispose();
        _concurrencySemaphore.Dispose();
    }
}
