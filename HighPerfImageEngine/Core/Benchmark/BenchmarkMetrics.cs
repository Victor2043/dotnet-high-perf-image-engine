using System.Diagnostics;

namespace HighPerfImageEngine.Core.Benchmark;

/// <summary>
/// Central, thread-safe counters for a single run. All mutation happens via
/// Interlocked/Volatile so any worker/consumer thread can call into this
/// safely without its own locking.
/// </summary>
public sealed class BenchmarkMetrics
{
    private readonly Stopwatch _processingTimer = new();
    private int _timerStarted;

    private long _totalAllocatedBytes;
    private long _processedMessageCount;
    private long _failedMessageCount;
    private long _activeMessageCount;
    private long _expectedMessageCount;
    private long _sampleWriteCounter;
    private long _receivedDeliveryCount;

    private long _totalAckPathTicks;
    private long _ackPathSamples;
    private long _totalChannelWaitTicks;
    private long _channelWaitSamples;
    private long _totalPipelineTicks;

    private volatile string? _batchId;

    public TimeSpan Elapsed => _processingTimer.Elapsed;

    public long ActiveMessageCount => Volatile.Read(ref _activeMessageCount);
    public long ProcessedCount => Volatile.Read(ref _processedMessageCount);
    public long FailedCount => Volatile.Read(ref _failedMessageCount);
    public long ExpectedCount => Volatile.Read(ref _expectedMessageCount);
    public long ReceivedDeliveryCount => Volatile.Read(ref _receivedDeliveryCount);
    public string? BatchId => _batchId;

    /// <summary>Counts every delivery RabbitMQ.Client handed to ReceivedAsync, regardless of outcome. Diagnostic-only.</summary>
    public void RecordDeliveryReceived() => Interlocked.Increment(ref _receivedDeliveryCount);

    /// <summary>Starts the benchmark clock exactly once, on whichever thread gets there first.</summary>
    public void StartTimerIfNeeded()
    {
        if (Interlocked.Exchange(ref _timerStarted, 1) == 0)
        {
            _processingTimer.Start();
        }
    }

    public void StopTimer()
    {
        if (_processingTimer.IsRunning)
        {
            _processingTimer.Stop();
        }
    }

    public void SetBatchInfo(long expectedMessages, string? batchId)
    {
        Volatile.Write(ref _expectedMessageCount, expectedMessages);
        _batchId = batchId;
    }

    public void RecordMessageStarted() => Interlocked.Increment(ref _activeMessageCount);

    public void RecordMessageFinished() => Interlocked.Decrement(ref _activeMessageCount);

    /// <summary>Records a fully processed message. Returns the running total (useful for "every N messages" logging).</summary>
    public long RecordProcessed(long allocatedBytes, double pipelineMs)
    {
        Interlocked.Add(ref _totalAllocatedBytes, Math.Max(0, allocatedBytes));
        Interlocked.Add(ref _totalPipelineTicks, (long)(pipelineMs * Stopwatch.Frequency / 1000.0));
        return Interlocked.Increment(ref _processedMessageCount);
    }

    public void RecordFailed() => Interlocked.Increment(ref _failedMessageCount);

    public void RecordAckPath(long ticks)
    {
        Interlocked.Add(ref _totalAckPathTicks, ticks);
        Interlocked.Increment(ref _ackPathSamples);
    }

    public void RecordChannelWait(long ticks)
    {
        Interlocked.Add(ref _totalChannelWaitTicks, ticks);
        Interlocked.Increment(ref _channelWaitSamples);
    }

    /// <summary>Shared, monotonically increasing counter used to decide which messages get sampled to disk.</summary>
    public long NextSampleIndex() => Interlocked.Increment(ref _sampleWriteCounter);

    public BenchmarkSnapshot Snapshot()
    {
        long processed = ProcessedCount;
        long ackSamples = Volatile.Read(ref _ackPathSamples);
        long waitSamples = Volatile.Read(ref _channelWaitSamples);

        double avgAckMs =
            ackSamples > 0
                ? Volatile.Read(ref _totalAckPathTicks) * 1000.0 / Stopwatch.Frequency / ackSamples
                : 0;

        double avgChannelWaitMs =
            waitSamples > 0
                ? Volatile.Read(ref _totalChannelWaitTicks) * 1000.0 / Stopwatch.Frequency / waitSamples
                : 0;

        double avgPipelineMs =
            processed > 0
                ? Volatile.Read(ref _totalPipelineTicks) * 1000.0 / Stopwatch.Frequency / processed
                : 0;

        return new BenchmarkSnapshot(
            Processed: processed,
            Failed: FailedCount,
            Expected: ExpectedCount,
            ReceivedDeliveries: ReceivedDeliveryCount,
            BatchId: BatchId,
            ElapsedSeconds: Elapsed.TotalSeconds,
            TotalAllocatedBytes: Volatile.Read(ref _totalAllocatedBytes),
            AvgPipelineMs: avgPipelineMs,
            AvgAckMs: avgAckMs,
            AvgChannelWaitMs: avgChannelWaitMs);
    }
}

public sealed record BenchmarkSnapshot(
    long Processed,
    long Failed,
    long Expected,
    long ReceivedDeliveries,
    string? BatchId,
    double ElapsedSeconds,
    long TotalAllocatedBytes,
    double AvgPipelineMs,
    double AvgAckMs,
    double AvgChannelWaitMs);