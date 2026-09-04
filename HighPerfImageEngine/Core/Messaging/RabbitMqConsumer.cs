using System.Text.Json;
using HighPerfImageEngine.Config;
using HighPerfImageEngine.Core.Benchmark;
using HighPerfImageEngine.Core.Pipeline;
using HighPerfImageEngine.Core.Ui;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace HighPerfImageEngine.Core.Messaging;

/// <summary>
/// Wires up the RabbitMQ consumer: detects the batch-completion marker,
/// parses regular image messages, and forwards parsed payloads into the
/// worker pool for processing. This class owns "receive + route" only — the
/// actual image processing lives in ImageProcessingWorkerPool.
/// </summary>
public sealed class RabbitMqConsumer
{
    private const string BatchCompletedMessageType = "batch_completed";
    private const string ReadinessProbeMessageType = "readiness_probe";

    private readonly IChannel _channel;
    private readonly EngineSettings _settings;
    private readonly ImageProcessingWorkerPool _workerPool;
    private readonly AckNackDispatcher _ackDispatcher;
    private readonly BenchmarkMetrics _metrics;
    private readonly BatchCompletionSignal _batchSignal;

    public RabbitMqConsumer(
        IChannel channel,
        EngineSettings settings,
        ImageProcessingWorkerPool workerPool,
        AckNackDispatcher ackDispatcher,
        BenchmarkMetrics metrics,
        BatchCompletionSignal batchSignal)
    {
        _channel = channel;
        _settings = settings;
        _workerPool = workerPool;
        _ackDispatcher = ackDispatcher;
        _metrics = metrics;
        _batchSignal = batchSignal;
    }

    public async Task StartAsync()
    {
        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += OnReceivedAsync;

        await _channel.BasicConsumeAsync(
            queue: _settings.RabbitMq.MainQueue,
            autoAck: false,
            consumer: consumer);
    }

    private async Task OnReceivedAsync(object model, BasicDeliverEventArgs ea)
    {
        _metrics.RecordDeliveryReceived();

        try
        {
            // Check whether this is the final batch marker.
            if (TryReadBatchCompletionMessage(
                    ea.Body, out long expectedMessages, out string? batchId))
            {
                _metrics.SetBatchInfo(expectedMessages, batchId);

                await _ackDispatcher.AckAsync(ea.DeliveryTag);

                if (_settings.Logging.EnableUiLogs)
                {
                    ConsoleUiService.LogSuccess(
                        $"Batch completion marker received. " +
                        $"Batch ID: {batchId}. " +
                        $"Expected messages: {expectedMessages:N0}.");
                }

                // No more image messages are coming — completing enqueueing
                // lets every worker drain what's already queued and then
                // exit its loop cleanly.
                _workerPool.CompleteEnqueueing();

                _batchSignal.MarkBatchMarkerReceived();

                return;
            }

            // The producer sends one throwaway probe message before it
            // starts the real benchmark, purely to confirm this queue's
            // binding already exists (avoiding a startup race). It's
            // infrastructure, not data: ack it and move on without touching
            // any of the Processed/Failed/Expected accounting.
            if (IsReadinessProbe(ea.Body))
            {
                await _ackDispatcher.AckAsync(ea.DeliveryTag);
                return;
            }

            int deathCount = GetRetryCountFromHeaders(ea.BasicProperties.Headers);

            // Cheap, synchronous parse (JSON scan + Base64 decode into a
            // pooled buffer) happens right here, on RabbitMQ.Client's single
            // dispatch thread. The expensive CPU work (Skia decode/SIMD/
            // encode) is handed off to the worker pool.
            if (!ImageMessageParser.TryParse(ea.Body.Span, out ParsedImageMessage payload))
            {
                if (_settings.Logging.EnableUiLogs)
                {
                    ConsoleUiService.LogError("Malformed message payload. Sending to DLQ.");
                }

                // Count this so Processed + Failed always adds up to Expected
                // in the final report — a malformed payload is a legitimate
                // terminal outcome, not a message that silently vanishes.
                _metrics.RecordFailed();

                await _ackDispatcher.NackAsync(ea.DeliveryTag, requeue: false);
                return;
            }

            _metrics.RecordMessageStarted();

            await _workerPool.EnqueueAsync(
                new ImageWorkItem(ea.DeliveryTag, deathCount, payload));
        }
        catch (Exception ex)
        {
            ConsoleUiService.LogError($"Unhandled consumer error: {ex.Message}");
        }
    }

    private static bool IsReadinessProbe(ReadOnlyMemory<byte> body)
    {
        try
        {
            using JsonDocument document = JsonDocument.Parse(body);
            JsonElement root = document.RootElement;

            return root.TryGetProperty("message_type", out JsonElement messageType) &&
                   string.Equals(
                       messageType.GetString(),
                       ReadinessProbeMessageType,
                       StringComparison.Ordinal);
        }
        catch (JsonException)
        {
            return false;
        }
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
            using JsonDocument document = JsonDocument.Parse(body);
            JsonElement root = document.RootElement;

            if (!root.TryGetProperty("message_type", out JsonElement messageType))
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

            if (root.TryGetProperty("expected_messages", out JsonElement expectedElement))
            {
                expectedMessages = expectedElement.GetInt64();
            }

            if (root.TryGetProperty("batch_id", out JsonElement batchIdElement))
            {
                batchId = batchIdElement.GetString();
            }

            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static int GetRetryCountFromHeaders(IDictionary<string, object?>? headers)
    {
        if (headers == null || !headers.TryGetValue("x-death", out var xDeathObj))
        {
            return 0;
        }

        if (xDeathObj is IList<object> xDeathList && xDeathList.Count > 0)
        {
            if (xDeathList[0] is IDictionary<string, object?> deathEntry &&
                deathEntry.TryGetValue("count", out var countObj))
            {
                return Convert.ToInt32(countObj);
            }
        }

        return 0;
    }
}