using HighPerfImageEngine.Config;
using HighPerfImageEngine.Core.Benchmark;
using HighPerfImageEngine.Core.Messaging;
using HighPerfImageEngine.Core.Pipeline;
using HighPerfImageEngine.Core.Ui;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;

namespace HighPerfImageEngine;

public class Program
{
    private static IConfiguration BuildConfiguration()
    {
        string environment =
            Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT")
            ?? "Development";

        return new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
            .AddJsonFile($"appsettings.{environment}.json", optional: true, reloadOnChange: false)
            .AddEnvironmentVariables()
            .Build();
    }

    public static async Task Main(string[] args)
    {        
        var configuration = BuildConfiguration();
        var settings = configuration.Get<EngineSettings>() ?? new EngineSettings();

        ConsoleUiService.RenderBanner();

        var pipelineService = new ImagePipelineService();

        string outputDirectory =
            Environment.GetEnvironmentVariable("OUTPUT_DIR") ?? "/app/output_files";

        Directory.CreateDirectory(outputDirectory);

        var resources = await RabbitMqConnectionFactory.CreateConnectionAndChannelAsync(settings);
        if (resources == null)
        {
            return;
        }

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

        var metrics = new BenchmarkMetrics();
        var ackDispatcher = new AckNackDispatcher(channel, metrics);
        var batchSignal = new BatchCompletionSignal(metrics);

        var workerPool = new ImageProcessingWorkerPool(
            degreeOfParallelism,
            channelCapacity,
            pipelineService,
            ackDispatcher,
            metrics,
            settings,
            outputDirectory,
            onMessageFinished: batchSignal.NotifyMessageFinished);

        var consumer = new RabbitMqConsumer(
            channel, settings, workerPool, ackDispatcher, metrics, batchSignal);

        await consumer.StartAsync();

        // Graceful shutdown support.
        void ShutdownHandler()
        {
            workerPool.CompleteEnqueueing();
            batchSignal.ForceComplete();
        }

        AppDomain.CurrentDomain.ProcessExit += (_, _) => ShutdownHandler();

        Console.CancelKeyPress += (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            ShutdownHandler();
        };

        // Wait until the producer sends the batch completion marker, or
        // until the application receives a graceful shutdown signal.
        await batchSignal.Completion;

        // Let any in-flight workers finish enqueueing their ack/nack requests,
        // then let the dedicated ack loop actually flush every queued Ack/Nack
        // to the broker before closing the channel/connection. Without this,
        // messages could still be "in flight" to RabbitMQ.Client when the
        // connection closes, and the broker would requeue them.
        await workerPool.WaitForDrainAsync();
        await ackDispatcher.CompleteAndDrainAsync();

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
}