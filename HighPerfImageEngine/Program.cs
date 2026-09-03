using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using HighPerfImageEngine.Config;
using HighPerfImageEngine.Core.Messaging;
using HighPerfImageEngine.Core.Pipeline;
using HighPerfImageEngine.Core.Ui;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace HighPerfImageEngine;

public class Program
{
    private const string BatchCompletedMessageType = "batch_completed";

    private static readonly Stopwatch ProcessingTimer = new();

    private static long _totalAllocatedBytes;
    private static long _processedMessageCount;
    private static long _failedMessageCount;
    private static long _activeMessageCount;
    private static long _expectedMessageCount;
    private static long _sampleWriteCounter;

    // Diagnostic-only accumulators (in Stopwatch ticks) to find out exactly
    // where per-message time is going. Removed once the bottleneck is found.
    private static long _totalAckPathTicks;
    private static long _totalChannelWaitTicks;
    private static long _totalPipelineTicks;
    private static long _ackPathSamples;
    private static long _channelWaitSamples;

    private static int _timerStarted;
    private static int _batchCompleted;
    private static int _finalResultsRendered;

    private static string? _batchId;

    /// <summary>Everything a worker needs to process and ack/nack one message.</summary>
    private sealed record WorkItem(ulong DeliveryTag, int DeathCount, ParsedImageMessage Payload);

    /// <summary>A single Ack or Nack instruction for the dedicated ack loop.</summary>
    private sealed record AckRequest(ulong DeliveryTag, bool Ack, bool Requeue);

    private static IConfiguration BuildConfiguration()
    {
        string environment =
            Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT")
            ?? "Development";

        return new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile(
                "appsettings.json",
                optional: false,
                reloadOnChange: false)
            .AddJsonFile(
                $"appsettings.{environment}.json",
                optional: true,
                reloadOnChange: false)
            .AddEnvironmentVariables()
            .Build();
    }

    public static async Task Main(string[] args)
    {
        var configuration = BuildConfiguration();
        var settings = configuration.Get<EngineSettings>() ?? new EngineSettings();

        ConsoleUiService.RenderBanner();

        var pipelineService = new ImagePipelineService();

        string outputDirectory = Environment.GetEnvironmentVariable("OUTPUT_DIR") ?? "/app/output_files";
        Directory.CreateDirectory(outputDirectory);

        var resources = await RabbitMqConnectionFactory.CreateConnectionAndChannelAsync(settings);
        if (resources == null) return;

        using var connection = resources.Value.Connection;
        using var channel = resources.Value.Channel;

        await RabbitMqTopologyBuilder.DeclareTopologyAsync(channel, settings.RabbitMq);

        // 0 means "auto": use every logical processor available to the container.
        int degreeOfParallelism =
            settings.Processing.DegreeOfParallelism > 0
                ? settings.Processing.DegreeOfParallelism
                : Environment.ProcessorCount;

        int channelCapacity =
            settings.Processing.ChannelCapacity > 0
                ? settings.Processing.ChannelCapacity
                : Math.Max(1, (int)settings.RabbitMq.PrefetchCount);

        ConsoleUiService.LogInfo(
            $"Consumer started. Prefetch: {settings.RabbitMq.PrefetchCount}. " +
            $"Workers: {degreeOfParallelism}. Channel capacity: {channelCapacity}. " +
            $"Raw Environment.ProcessorCount: {Environment.ProcessorCount} " +
            "(if this doesn't match your cgroup CPU limit, the runtime " +
            "isn't detecting the container's real budget). " +
            "Waiting for batch...");

        // Internal bounded channel: decouples the single-threaded RabbitMQ
        // delivery callback from the CPU-bound image pipeline. Bounding it
        // (rather than using an unbounded channel) preserves backpressure —
        // once it's full, ReceivedAsync's WriteAsync suspends, which naturally
        // slows down how fast we ack/consume further deliveries.
        var workChannel = Channel.CreateBounded<WorkItem>(
            new BoundedChannelOptions(channelCapacity)
            {
                SingleWriter = true,
                SingleReader = false,
                FullMode = BoundedChannelFullMode.Wait
            });

        // Ack/Nack requests go through a dedicated single-consumer channel
        // instead of a SemaphoreSlim gate. Workers only ever *enqueue* here
        // (a fast, effectively lock-free MPSC write); exactly one background
        // task drains it and is the ONLY thing that ever touches `channel`
        // for Ack/Nack, so there's no contention, no WaitAsync queueing, and
        // no risk of a lock-convoy forming at some intermediate worker count.
        var ackChannel = Channel.CreateUnbounded<AckRequest>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });

        var batchCompletionTcs =
            new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously);

        Task AckAsync(ulong deliveryTag) =>
            ackChannel.Writer.WriteAsync(
                new AckRequest(deliveryTag, Ack: true, Requeue: false)).AsTask();

        Task NackAsync(ulong deliveryTag, bool requeue) =>
            ackChannel.Writer.WriteAsync(
                new AckRequest(deliveryTag, Ack: false, Requeue: requeue)).AsTask();

        // The single dedicated Ack/Nack consumer.
        var ackLoop = Task.Run(async () =>
        {
            await foreach (AckRequest req in ackChannel.Reader.ReadAllAsync())
            {
                long start = Stopwatch.GetTimestamp();

                if (req.Ack)
                {
                    await channel.BasicAckAsync(
                        deliveryTag: req.DeliveryTag,
                        multiple: false);
                }
                else
                {
                    await channel.BasicNackAsync(
                        deliveryTag: req.DeliveryTag,
                        multiple: false,
                        requeue: req.Requeue);
                }

                Interlocked.Add(ref _totalAckPathTicks, Stopwatch.GetTimestamp() - start);
                Interlocked.Increment(ref _ackPathSamples);
            }
        });

        void MarkMessageDone()
        {
            Interlocked.Decrement(ref _activeMessageCount);
            TryCompleteBatch(batchCompletionTcs);
        }

        // Worker pool: each task pulls parsed image payloads off the channel
        // and runs decode -> SIMD brightness -> WebP encode. This is where the
        // actual CPU parallelism comes from — with 1 worker this behaves
        // exactly like the old fully-serial pipeline.
        var workers = new Task[degreeOfParallelism];

        for (int w = 0; w < degreeOfParallelism; w++)
        {
            workers[w] = Task.Run(async () =>
            {
                await foreach (WorkItem item in workChannel.Reader.ReadAllAsync())
                {
                    using ParsedImageMessage payload = item.Payload;

                    try
                    {
                        // Start the benchmark timer when the first image
                        // message begins processing (mirrors original semantics).
                        if (Interlocked.Exchange(ref _timerStarted, 1) == 0)
                        {
                            ProcessingTimer.Start();
                        }

                        long allocBefore =
                            GC.GetAllocatedBytesForCurrentThread();

                        string webpFileName =
                            $"{Path.GetFileNameWithoutExtension(payload.FileName)}.webp";

                        string outputPath =
                            Path.Combine(outputDirectory, webpFileName);

                        // Every message is fully processed regardless; only the
                        // physical disk write is sampled, purely for inspection.
                        long sampleIndex =
                            Interlocked.Increment(ref _sampleWriteCounter);

                        bool writeToDisk =
                            settings.Persistence.SaveSampleToDisk &&
                            sampleIndex %
                                Math.Max(1, settings.Persistence.SampleEveryNthMessage) == 0;

                        bool success =
                            pipelineService.ProcessImageFromBytes(
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

                        long allocAfter =
                            GC.GetAllocatedBytesForCurrentThread();

                        Interlocked.Add(
                            ref _totalAllocatedBytes,
                            Math.Max(0, allocAfter - allocBefore));

                        Interlocked.Add(
                            ref _totalPipelineTicks,
                            (long)(result.TotalMilliseconds * Stopwatch.Frequency / 1000.0));

                        long currentCount =
                            Interlocked.Increment(ref _processedMessageCount);

                        double elapsedSeconds =
                            ProcessingTimer.Elapsed.TotalSeconds;

                        if (settings.Logging.EnableUiLogs &&
                            currentCount % settings.Logging.LogIntervalMessages == 0)
                        {
                            double throughput =
                                elapsedSeconds > 0
                                    ? currentCount / elapsedSeconds
                                    : 0;

                            ConsoleUiService.LogSuccess(
                                $"Processed {currentCount:N0} msgs | " +
                                $"Speed: {throughput:N1} msgs/sec");
                        }

                        await AckAsync(item.DeliveryTag);
                    }
                    catch (Exception ex)
                    {
                        if (settings.Logging.EnableUiLogs)
                        {
                            ConsoleUiService.LogError(
                                $"Processing failed: {ex.Message}");
                        }

                        if (item.DeathCount < settings.RabbitMq.MaxRetryCount - 1)
                        {
                            if (settings.Logging.EnableUiLogs)
                            {
                                ConsoleUiService.LogInfo(
                                    "Requeuing message...");
                            }

                            await NackAsync(item.DeliveryTag, requeue: true);
                        }
                        else
                        {
                            Interlocked.Increment(ref _failedMessageCount);

                            if (settings.Logging.EnableUiLogs)
                            {
                                ConsoleUiService.LogInfo(
                                    $"Retry limit reached " +
                                    $"({settings.RabbitMq.MaxRetryCount}). " +
                                    "Forwarding message to DLQ.");
                            }

                            await NackAsync(item.DeliveryTag, requeue: false);
                        }
                    }
                    finally
                    {
                        MarkMessageDone();
                    }
                }
            });
        }

        var consumer =
            new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (model, ea) =>
        {
            try
            {
                // Check whether this is the final batch marker.
                if (TryReadBatchCompletionMessage(
                        ea.Body,
                        out long expectedMessages,
                        out string? batchId))
                {
                    _expectedMessageCount =
                        expectedMessages;

                    _batchId = batchId;

                    Interlocked.Exchange(
                        ref _batchCompleted,
                        1);

                    await AckAsync(ea.DeliveryTag);

                    if (settings.Logging.EnableUiLogs)
                    {
                        ConsoleUiService.LogSuccess(
                            $"Batch completion marker received. " +
                            $"Batch ID: {_batchId}. " +
                            $"Expected messages: {_expectedMessageCount:N0}.");
                    }

                    // No more image messages are coming — completing the
                    // writer lets every worker drain what's already queued
                    // and then exit its loop cleanly.
                    workChannel.Writer.TryComplete();

                    TryCompleteBatch(batchCompletionTcs);

                    return;
                }

                int deathCount =
                    GetRetryCountFromHeaders(
                        ea.BasicProperties.Headers);

                // Cheap, synchronous parse (JSON scan + Base64 decode into a
                // pooled buffer) happens right here, on RabbitMQ.Client's
                // single dispatch thread. The expensive CPU work (Skia
                // decode/SIMD/encode) is handed off to the worker pool.
                if (!ImageMessageParser.TryParse(
                        ea.Body.Span,
                        out ParsedImageMessage payload))
                {
                    if (settings.Logging.EnableUiLogs)
                    {
                        ConsoleUiService.LogError(
                            "Malformed message payload. Sending to DLQ.");
                    }

                    await NackAsync(ea.DeliveryTag, requeue: false);
                    return;
                }

                Interlocked.Increment(
                    ref _activeMessageCount);

                long writeStart = Stopwatch.GetTimestamp();

                await workChannel.Writer.WriteAsync(
                    new WorkItem(ea.DeliveryTag, deathCount, payload));

                Interlocked.Add(ref _totalChannelWaitTicks, Stopwatch.GetTimestamp() - writeStart);
                Interlocked.Increment(ref _channelWaitSamples);
            }
            catch (Exception ex)
            {
                ConsoleUiService.LogError(
                    $"Unhandled consumer error: {ex.Message}");
            }
        };

        await channel.BasicConsumeAsync(
            queue: settings.RabbitMq.MainQueue,
            autoAck: false,
            consumer: consumer);

        // Graceful shutdown support.
        void ShutdownHandler()
        {
            workChannel.Writer.TryComplete();

            RenderFinalResults();

            batchCompletionTcs.TrySetResult();
        }

        AppDomain.CurrentDomain.ProcessExit +=
            (sender, eventArgs) =>
            {
                ShutdownHandler();
            };

        Console.CancelKeyPress +=
            (sender, eventArgs) =>
            {
                eventArgs.Cancel = true;

                ShutdownHandler();
            };

        // Wait until the producer sends the batch completion marker,
        // or until the application receives a graceful shutdown signal.
        await batchCompletionTcs.Task;

        // Let any in-flight workers finish enqueueing their ack/nack requests...
        await Task.WhenAll(workers);

        // ...then let the dedicated ack loop actually flush every queued
        // Ack/Nack to the broker before we close the channel/connection.
        // Without this, messages could still be "in flight" to RabbitMQ.Client
        // when the connection closes, and the broker would requeue them.
        ackChannel.Writer.TryComplete();
        await ackLoop;

        await Task.Delay(50);

        if (channel.IsOpen)
        {
            await channel.CloseAsync();
        }

        if (connection.IsOpen)
        {
            await connection.CloseAsync();
        }
    }

    private static void TryCompleteBatch(
        TaskCompletionSource batchCompletionTcs)
    {
        bool batchCompleted =
            Volatile.Read(ref _batchCompleted) == 1;

        long activeMessages =
            Volatile.Read(ref _activeMessageCount);

        if (batchCompleted &&
            activeMessages == 0)
        {
            RenderFinalResults();

            batchCompletionTcs.TrySetResult();
        }
    }

    private static void RenderFinalResults()
    {
        if (Interlocked.Exchange(
                ref _finalResultsRendered,
                1) != 0)
        {
            return;
        }

        if (ProcessingTimer.IsRunning)
        {
            ProcessingTimer.Stop();
        }

        double elapsedSeconds =
            ProcessingTimer.Elapsed.TotalSeconds;

        long processed =
            Volatile.Read(ref _processedMessageCount);

        long expected =
            Volatile.Read(ref _expectedMessageCount);

        long failed =
            Volatile.Read(ref _failedMessageCount);

        if (expected > 0)
        {
            ConsoleUiService.LogInfo(
                $"Batch completed. " +
                $"Expected: {expected:N0} | " +
                $"Processed: {processed:N0} | " +
                $"Failed: {failed:N0}");
        }

        if (!string.IsNullOrWhiteSpace(_batchId))
        {
            ConsoleUiService.LogInfo(
                $"Batch ID: {_batchId}");
        }

        if (expected > 0 &&
            processed + failed != expected)
        {
            ConsoleUiService.LogWarning(
                $"Batch count mismatch! " +
                $"Expected {expected:N0}, but processed " +
                $"{processed:N0} and failed {failed:N0}.");
        }

        ConsoleUiService.RenderResultTable(
            processed,
            elapsedSeconds,
            Volatile.Read(
                ref _totalAllocatedBytes));

        // --- Diagnostic breakdown (remove once bottleneck is confirmed) ---
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

        ConsoleUiService.LogInfo(
            $"[DIAGNOSTIC] Avg pipeline (decode+SIMD+encode): {avgPipelineMs:N2} ms | " +
            $"Avg ack/nack round-trip (incl. semaphore wait): {avgAckMs:N2} ms | " +
            $"Avg channel enqueue wait (backpressure): {avgChannelWaitMs:N2} ms");
    }

    private static bool TryReadBatchCompletionMessage(
        ReadOnlyMemory<byte> body,
        out long expectedMessages,
        out string? batchId)
    {
        expectedMessages = 0;
        batchId = null;

        try
        {
            using JsonDocument document =
                JsonDocument.Parse(body);

            JsonElement root =
                document.RootElement;

            if (!root.TryGetProperty(
                    "message_type",
                    out JsonElement messageType))
            {
                return false;
            }

            if (!string.Equals(
                    messageType.GetString(),
                    BatchCompletedMessageType,
                    StringComparison.Ordinal))
            {
                return false;
            }

            if (root.TryGetProperty(
                    "expected_messages",
                    out JsonElement expectedElement))
            {
                expectedMessages =
                    expectedElement.GetInt64();
            }

            if (root.TryGetProperty(
                    "batch_id",
                    out JsonElement batchIdElement))
            {
                batchId =
                    batchIdElement.GetString();
            }

            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static int GetRetryCountFromHeaders(
        IDictionary<string, object?>? headers)
    {
        if (headers == null ||
            !headers.TryGetValue(
                "x-death",
                out var xDeathObj))
        {
            return 0;
        }

        if (xDeathObj is IList<object> xDeathList &&
            xDeathList.Count > 0)
        {
            if (xDeathList[0]
                    is IDictionary<string, object?> deathEntry &&
                deathEntry.TryGetValue(
                    "count",
                    out var countObj))
            {
                return Convert.ToInt32(countObj);
            }
        }

        return 0;
    }
}