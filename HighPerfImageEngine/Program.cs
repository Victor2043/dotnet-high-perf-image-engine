using System.Diagnostics;
using System.Text.Json;
using HighPerfImageEngine.Config;
using HighPerfImageEngine.Core.Pipeline;
using HighPerfImageEngine.Core.Ui;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace HighPerfImageEngine;

public class Program
{
    private static readonly Stopwatch GlobalTimer = Stopwatch.StartNew();
    private static long _totalAllocatedBytes;
    private static long _processedMessageCount;

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
                if (settings.Logging.EnableUiLogs)
                    ConsoleUiService.LogInfo($"Attempting to connect to RabbitMQ (attempt {attempt}/10)...");

                connection = await factory.CreateConnectionAsync();
                channel = await connection.CreateChannelAsync();

                if (settings.Logging.EnableUiLogs)
                    ConsoleUiService.LogSuccess("Successfully connected to RabbitMQ!");

                break;
            }
            catch (Exception ex)
            {
                if (settings.Logging.EnableUiLogs)
                    ConsoleUiService.LogWarning($"Broker unreachable: {ex.Message}. Waiting 3s...");

                await Task.Delay(3000);
            }
        }

        if (connection == null || channel == null)
        {
            ConsoleUiService.LogError("Could not establish connection to RabbitMQ. Terminating.");
            return;
        }

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: settings.RabbitMq.PrefetchCount, global: false);

        // Topologia DLX / DLQ
        await channel.ExchangeDeclareAsync(exchange: settings.RabbitMq.DlxExchange, type: ExchangeType.Direct, durable: true);
        await channel.QueueDeclareAsync(queue: settings.RabbitMq.DlqQueue, durable: true, exclusive: false, autoDelete: false);
        await channel.QueueBindAsync(queue: settings.RabbitMq.DlqQueue, exchange: settings.RabbitMq.DlxExchange, routingKey: settings.RabbitMq.DlqRoutingKey);

        // Topologia Fila Principal
        var mainQueueArgs = new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", settings.RabbitMq.DlxExchange },
            { "x-dead-letter-routing-key", settings.RabbitMq.DlqRoutingKey }
        };

        await channel.ExchangeDeclareAsync(exchange: settings.RabbitMq.MainExchange, type: ExchangeType.Direct, durable: true);
        await channel.QueueDeclareAsync(queue: settings.RabbitMq.MainQueue, durable: true, exclusive: false, autoDelete: false, arguments: mainQueueArgs);
        await channel.QueueBindAsync(queue: settings.RabbitMq.MainQueue, exchange: settings.RabbitMq.MainExchange, routingKey: settings.RabbitMq.MainRoutingKey);

        if (settings.Logging.EnableUiLogs)
            ConsoleUiService.LogInfo($"Consumer started. Prefetch: {settings.RabbitMq.PrefetchCount}. Awaiting messages...");

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (model, ea) =>
        {
            int deathCount = GetRetryCountFromHeaders(ea.BasicProperties.Headers);

            try
            {
                long allocBefore = GC.GetAllocatedBytesForCurrentThread();

                var payload = JsonSerializer.Deserialize<ImageMessagePayload>(ea.Body.Span);
                if (payload == null || string.IsNullOrEmpty(payload.ContentBase64))
                {
                    throw new InvalidDataException("Event payload is null or missing valid Base64 content.");
                }

                byte[] imageBytes = Convert.FromBase64String(payload.ContentBase64);
                string webpFileName = $"{Path.GetFileNameWithoutExtension(payload.FileName)}.webp";
                string outputPath = Path.Combine(outputDirectory, webpFileName);

                bool success = pipelineService.ProcessImageFromBytes(
                    imageBytes,
                    payload.FileName,
                    outputPath,
                    (byte)payload.BrightnessOffset,
                    out ProcessResult? result
                );

                if (!success || result == null)
                {
                    throw new InvalidOperationException($"Failed to process image '{payload.FileName}'.");
                }

                long allocAfter = GC.GetAllocatedBytesForCurrentThread();
                long currentAllocated = Math.Max(0, allocAfter - allocBefore);

                // Incrementa contadores thread-safe
                Interlocked.Add(ref _totalAllocatedBytes, currentAllocated);
                long currentCount = Interlocked.Increment(ref _processedMessageCount);
                double globalTotalSeconds = GlobalTimer.Elapsed.TotalSeconds;

                if (settings.Logging.EnableResultTable)
                {
                    ConsoleUiService.RenderResultTableByImage(result, currentAllocated, globalTotalSeconds);
                }
                else if (settings.Logging.EnableUiLogs && (currentCount % settings.Logging.LogIntervalMessages == 0))
                {
                    double throughput = currentCount / globalTotalSeconds;
                    ConsoleUiService.LogSuccess($"[BATCH LOG] Processed {currentCount} msgs | Speed: {throughput:N1} msgs/sec");
                }

                await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                if (settings.Logging.EnableUiLogs)
                    ConsoleUiService.LogError($"Processing failed: {ex.Message}");

                if (deathCount < settings.RabbitMq.MaxRetryCount - 1)
                {
                    if (settings.Logging.EnableUiLogs)
                        ConsoleUiService.LogInfo("Requeuing message...");

                    await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                }
                else
                {
                    if (settings.Logging.EnableUiLogs)
                        ConsoleUiService.LogInfo($"Retry limit reached ({settings.RabbitMq.MaxRetryCount}). Forwarding to DLQ.");

                    await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
                }
            }
        };

        await channel.BasicConsumeAsync(queue: settings.RabbitMq.MainQueue, autoAck: false, consumer: consumer);

        // Captura o encerramento gracioso (Ctrl+C ou SIGTERM do Docker/Kubernetes)
        var tcs = new TaskCompletionSource();

        Action shutdownHandler = () =>
        {
            GlobalTimer.Stop();
            ConsoleUiService.RenderResultTable(_processedMessageCount, GlobalTimer.Elapsed.TotalSeconds, _totalAllocatedBytes);
            tcs.TrySetResult();
        };

        AppDomain.CurrentDomain.ProcessExit += (s, e) => shutdownHandler();
        Console.CancelKeyPress += (s, e) =>
        {
            e.Cancel = true; // Impede interrupção abrupta para garantir renderização da tabela
            shutdownHandler();
        };

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