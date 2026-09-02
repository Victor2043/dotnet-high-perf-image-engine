using System.Diagnostics;
using System.Text;
using System.Text.Json;
using HighPerfImageEngine.Core.Pipeline;
using HighPerfImageEngine.Core.Ui;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace HighPerfImageEngine;

public class Program
{
    private const string MainExchange = "image.events";
    private const string MainQueue = "image.processing.queue";
    private const string MainRoutingKey = "image.process";

    private const string DlxExchange = "image.events.dlx";
    private const string DlqQueue = "image.processing.dlq";
    private const string DlqRoutingKey = "image.process.dlq";

    private const int MaxRetryCount = 3;
    private const ushort PrefetchCount = 10;

    private static readonly Stopwatch GlobalTimer = Stopwatch.StartNew();
    private static long _totalAllocatedBytes;

    public static async Task Main(string[] args)
    {
        // Instantiate the pipeline service and resolve output directory
        var pipelineService = new ImagePipelineService();
        string outputDirectory = Environment.GetEnvironmentVariable("OUTPUT_DIR") ?? "/app/output_files";
        Directory.CreateDirectory(outputDirectory);

        var factory = new ConnectionFactory
        {
            HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
            UserName = "guest",
            Password = "guest"
        };

        IConnection? connection = null;
        IChannel? channel = null;

        for (int attempt = 1; attempt <= 10; attempt++)
        {
            try
            {
                ConsoleUiService.LogInfo($"Attempting to connect to RabbitMQ (attempt {attempt}/10)...");
                connection = await factory.CreateConnectionAsync();
                channel = await connection.CreateChannelAsync();
                ConsoleUiService.LogSuccess("Successfully connected to RabbitMQ!");
                break;
            }
            catch (Exception ex)
            {
                ConsoleUiService.LogWarning($"Broker still unreachable: {ex.Message}. Waiting 3s...");
                await Task.Delay(3000);
            }
        }

        if (connection == null || channel == null)
        {
            ConsoleUiService.LogError("Could not establish connection to RabbitMQ. Terminating.");
            return;
        }

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: PrefetchCount, global: false);

        // Declare Dead Letter Exchange and DLQ
        await channel.ExchangeDeclareAsync(exchange: DlxExchange, type: ExchangeType.Direct, durable: true);
        await channel.QueueDeclareAsync(queue: DlqQueue, durable: true, exclusive: false, autoDelete: false);
        await channel.QueueBindAsync(queue: DlqQueue, exchange: DlxExchange, routingKey: DlqRoutingKey);

        // Declare Main Queue bound to DLX
        var mainQueueArgs = new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", DlxExchange },
            { "x-dead-letter-routing-key", DlqRoutingKey }
        };

        await channel.ExchangeDeclareAsync(exchange: MainExchange, type: ExchangeType.Direct, durable: true);
        await channel.QueueDeclareAsync(queue: MainQueue, durable: true, exclusive: false, autoDelete: false, arguments: mainQueueArgs);
        await channel.QueueBindAsync(queue: MainQueue, exchange: MainExchange, routingKey: MainRoutingKey);

        ConsoleUiService.LogInfo($"Consumer started. Prefetch: {PrefetchCount}. Waiting for messages...");

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (model, ea) =>
        {
            int deathCount = GetRetryCountFromHeaders(ea.BasicProperties.Headers);

            try
            {
                ConsoleUiService.LogInfo($"Attempt {deathCount + 1}/{MaxRetryCount} | Msg Tag: {ea.DeliveryTag}");

                long allocBefore = GC.GetAllocatedBytesForCurrentThread();

                // 1. Deserialize incoming JSON from Python without string allocations
                var payload = JsonSerializer.Deserialize<ImageMessagePayload>(ea.Body.Span);
                if (payload == null || string.IsNullOrEmpty(payload.ContentBase64))
                {
                    throw new InvalidDataException("Event payload is null or missing valid Base64 content.");
                }

                // 2. Convert received Base64 string to byte array
                byte[] imageBytes = Convert.FromBase64String(payload.ContentBase64);

                // 3. Define output file name and path in mapped volume
                string webpFileName = $"{Path.GetFileNameWithoutExtension(payload.FileName)}.webp";
                string outputPath = Path.Combine(outputDirectory, webpFileName);

                // 4. Invoke pipeline with format detection, SIMD kernel, and WebP encoding
                bool success = pipelineService.ProcessImageFromBytes(
                    imageBytes,
                    payload.FileName,
                    outputPath,
                    (byte)payload.BrightnessOffset,
                    out ProcessResult? result
                );

                if (!success || result == null)
                {
                    throw new InvalidOperationException($"Failed to process image '{payload.FileName}'. Unknown format or invalid buffer.");
                }

                long allocAfter = GC.GetAllocatedBytesForCurrentThread();
                long currentAllocated = Math.Max(0, allocAfter - allocBefore);

                ConsoleUiService.LogSuccess($"File saved: {result.FileName} -> {webpFileName} | Res: {result.Width}x{result.Height} | SIMD: {result.SimdMicroseconds:F2}µs | Total: {result.TotalMilliseconds:F2}ms");

                // Thread-safe accumulation of total allocated bytes
                long globalTotalAllocated = Interlocked.Add(ref _totalAllocatedBytes, currentAllocated);
                double globalTotalSeconds = GlobalTimer.Elapsed.TotalSeconds;

                ConsoleUiService.RenderResultTable(result, currentAllocated, globalTotalSeconds, globalTotalAllocated);

                await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                ConsoleUiService.LogError($"Processing failed: {ex.Message}");

                if (deathCount < MaxRetryCount - 1)
                {
                    ConsoleUiService.LogInfo("Requeuing message for retry...");
                    await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                }
                else
                {
                    ConsoleUiService.LogInfo($"Retry limit of {MaxRetryCount} reached. Forwarding message to DLQ.");
                    await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
                }
            }
        };

        await channel.BasicConsumeAsync(queue: MainQueue, autoAck: false, consumer: consumer);

        var tcs = new TaskCompletionSource();
        AppDomain.CurrentDomain.ProcessExit += (s, e) => tcs.SetResult();
        await tcs.Task;
    }

    private static int GetRetryCountFromHeaders(IDictionary<string, object?>? headers)
    {
        if (headers == null || !headers.TryGetValue("x-death", out var xDeathObj))
            return 0;

        if (xDeathObj is IList<object> xDeathList && xDeathList.Count > 0)
        {
            if (xDeathList[0] is IDictionary<string, object?> deathEntry && deathEntry.TryGetValue("count", out var countObj))
            {
                return Convert.ToInt32(countObj);
            }
        }

        return 0;
    }
}