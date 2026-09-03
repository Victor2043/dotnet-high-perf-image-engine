using HighPerfImageEngine.Core.Benchmark;
namespace HighPerfImageEngine.Core.Messaging;

/// <summary>
/// Signals when the whole batch is truly done: the completion marker has
/// been received AND every message that was ever enqueued has finished
/// processing (acked or nacked). Also supports being forced to complete for
/// graceful shutdown (Ctrl+C) even if the batch never finished naturally.
///
/// Rendering the final report is triggered from here (via BenchmarkReporter,
/// which guards against double-rendering) so every completion path —
/// natural or forced — is guaranteed to report exactly once.
/// </summary>
public sealed class BatchCompletionSignal
{
    private readonly TaskCompletionSource _tcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly BenchmarkMetrics _metrics;
    private int _batchMarkerReceived;

    public BatchCompletionSignal(BenchmarkMetrics metrics)
    {
        _metrics = metrics;
    }

    public Task Completion => _tcs.Task;

    public void MarkBatchMarkerReceived()
    {
        Interlocked.Exchange(ref _batchMarkerReceived, 1);
        TryComplete();
    }

    /// <summary>Call once a message has been fully handled (Ack or Nack sent).</summary>
    public void NotifyMessageFinished()
    {
        _metrics.RecordMessageFinished();
        TryComplete();
    }

    public void ForceComplete()
    {
        BenchmarkReporter.RenderOnce(_metrics);
        _tcs.TrySetResult();
    }

    private void TryComplete()
    {
        if (Volatile.Read(ref _batchMarkerReceived) == 1 &&
            _metrics.ActiveMessageCount == 0)
        {
            BenchmarkReporter.RenderOnce(_metrics);
            _tcs.TrySetResult();
        }
    }
}