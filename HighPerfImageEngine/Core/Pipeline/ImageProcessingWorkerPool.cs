using System.Diagnostics;
using System.Threading.Channels;
using HighPerfImageEngine.Config;
using HighPerfImageEngine.Core.Benchmark;
using HighPerfImageEngine.Core.Messaging;
using HighPerfImageEngine.Core.Ui;

namespace HighPerfImageEngine.Core.Pipeline;

/// <summary>Everything a worker needs to process and ack/nack one message.</summary>
public sealed record ImageWorkItem(ulong DeliveryTag, int DeathCount, ParsedImageMessage Payload);

/// <summary>
/// Decouples RabbitMQ message receipt (single-threaded, driven by
/// RabbitMQ.Client) from the CPU-bound image pipeline, which runs
/// concurrently across N worker tasks pulling from an internal bounded
/// Channel&lt;T&gt;. With degreeOfParallelism = 1 this behaves exactly like a
/// fully-serial pipeline.
/// </summary>
public sealed class ImageProcessingWorkerPool
{
    private readonly Channel<ImageWorkItem> _workChannel;
    private readonly Task[] _workers;
    private readonly BenchmarkMetrics _metrics;

    public ImageProcessingWorkerPool(
        int degreeOfParallelism,
        int channelCapacity,
        ImagePipelineService pipelineService,
        AckNackDispatcher ackDispatcher,
        BenchmarkMetrics metrics,
        EngineSettings settings,
        string outputDirectory,
        Action onMessageFinished)
    {
        _metrics = metrics;

        // Bounding the channel (rather than using an unbounded one) preserves
        // backpressure: once it's full, EnqueueAsync suspends, which naturally
        // slows down how fast the RabbitMQ consumer pulls further deliveries.
        _workChannel = Channel.CreateBounded<ImageWorkItem>(
            new BoundedChannelOptions(channelCapacity)
            {
                SingleWriter = true,
                SingleReader = false,
                FullMode = BoundedChannelFullMode.Wait
            });

        _workers = new Task[degreeOfParallelism];

        for (int w = 0; w < degreeOfParallelism; w++)
        {
            _workers[w] = Task.Run(() => RunWorkerAsync(
                pipelineService, ackDispatcher, settings, outputDirectory, onMessageFinished));
        }
    }

    /// <summary>
    /// Enqueues an item for processing, suspending (backpressure) if the
    /// internal channel is already full.
    /// </summary>
    public async Task EnqueueAsync(ImageWorkItem item)
    {
        long start = Stopwatch.GetTimestamp();

        await _workChannel.Writer.WriteAsync(item);

        _metrics.RecordChannelWait(Stopwatch.GetTimestamp() - start);
    }

    /// <summary>Signals that no more items will ever be enqueued; workers drain what's left and exit.</summary>
    public void CompleteEnqueueing() => _workChannel.Writer.TryComplete();

    /// <summary>Waits for every worker to finish draining the channel.</summary>
    public Task WaitForDrainAsync() => Task.WhenAll(_workers);

    private async Task RunWorkerAsync(
        ImagePipelineService pipelineService,
        AckNackDispatcher ackDispatcher,
        EngineSettings settings,
        string outputDirectory,
        Action onMessageFinished)
    {
        await foreach (ImageWorkItem item in _workChannel.Reader.ReadAllAsync())
        {
            using ParsedImageMessage payload = item.Payload;

            try
            {
                // Start the benchmark timer when the first image message
                // begins processing.
                _metrics.StartTimerIfNeeded();

                string webpFileName =
                    $"{Path.GetFileNameWithoutExtension(payload.FileName)}.webp";

                string outputPath = Path.Combine(outputDirectory, webpFileName);

                // Every message is fully processed regardless; only the
                // physical disk write is sampled, purely for inspection.
                long sampleIndex = _metrics.NextSampleIndex();

                bool writeToDisk =
                    settings.Persistence.SaveSampleToDisk &&
                    sampleIndex % Math.Max(1, settings.Persistence.SampleEveryNthMessage) == 0;

                long allocBefore = GC.GetAllocatedBytesForCurrentThread();

                bool success = pipelineService.ProcessImageFromBytes(
                    payload.ImageBytes,
                    payload.FileName,
                    outputPath,
                    payload.BrightnessOffset,
                    writeToDisk,
                    out ProcessResult? result);

                if (!success || result == null)
                {
                    throw new InvalidOperationException(
                        $"Failed to process image '{payload.FileName}'.");
                }

                long allocAfter = GC.GetAllocatedBytesForCurrentThread();

                long currentCount = _metrics.RecordProcessed(
                    allocAfter - allocBefore, result.TotalMilliseconds);

                if (settings.Logging.EnableUiLogs &&
                    currentCount % settings.Logging.LogIntervalMessages == 0)
                {
                    double elapsedSeconds = _metrics.Elapsed.TotalSeconds;
                    double throughput = elapsedSeconds > 0 ? currentCount / elapsedSeconds : 0;

                    ConsoleUiService.LogSuccess(
                        $"Processed {currentCount:N0} msgs | Speed: {throughput:N1} msgs/sec");
                }

                await ackDispatcher.AckAsync(item.DeliveryTag);
            }
            catch (Exception ex)
            {
                if (settings.Logging.EnableUiLogs)
                {
                    ConsoleUiService.LogError($"Processing failed: {ex.Message}");
                }

                if (item.DeathCount < settings.RabbitMq.MaxRetryCount - 1)
                {
                    if (settings.Logging.EnableUiLogs)
                    {
                        ConsoleUiService.LogInfo("Requeuing message...");
                    }

                    await ackDispatcher.NackAsync(item.DeliveryTag, requeue: true);
                }
                else
                {
                    _metrics.RecordFailed();

                    if (settings.Logging.EnableUiLogs)
                    {
                        ConsoleUiService.LogInfo(
                            $"Retry limit reached ({settings.RabbitMq.MaxRetryCount}). " +
                            "Forwarding message to DLQ.");
                    }

                    await ackDispatcher.NackAsync(item.DeliveryTag, requeue: false);
                }
            }
            finally
            {
                onMessageFinished();
            }
        }
    }
}