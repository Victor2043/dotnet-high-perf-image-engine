namespace HighPerfImageEngine.Config;

public class EngineSettings
{
    public LoggingSettings Logging { get; set; } = new();
    public RabbitMqSettings RabbitMq { get; set; } = new();
    public ProcessingSettings Processing { get; set; } = new();
    public PersistenceSettings Persistence { get; set; } = new();
}

public class LoggingSettings
{
    public bool EnableUiLogs { get; set; } = true;
    public bool EnableResultTable { get; set; } = true;
    public int LogIntervalMessages { get; set; } = 100;
}

public class RabbitMqSettings
{
    public string MainExchange { get; set; } = "image.events";
    public string MainQueue { get; set; } = "image.processing.queue";
    public string MainRoutingKey { get; set; } = "image.process";

    public string DlxExchange { get; set; } = "image.events.dlx";
    public string DlqQueue { get; set; } = "image.processing.dlq";
    public string DlqRoutingKey { get; set; } = "image.process.dlq";

    public int MaxRetryCount { get; set; } = 3;
    public ushort PrefetchCount { get; set; } = 10;
}

public class ProcessingSettings
{
    /// <summary>
    /// Number of worker tasks pulling parsed messages off the internal Channel
    /// and running the CPU-bound decode/SIMD/encode pipeline concurrently.
    /// Defaults to the number of logical processors available to the container.
    /// </summary>
    public int DegreeOfParallelism { get; set; } = Environment.ProcessorCount;

    /// <summary>
    /// Capacity of the internal bounded Channel that decouples message receipt
    /// (single-threaded, driven by RabbitMQ.Client) from image processing
    /// (parallel workers). 0 means "use RabbitMq.PrefetchCount", which keeps
    /// backpressure consistent with what the broker is already enforcing.
    /// </summary>
    public int ChannelCapacity { get; set; } = 0;
}

public class PersistenceSettings
{
    /// <summary>
    /// If true, periodically writes a processed WebP image to disk so a human
    /// can inspect real output. Every message is still fully decoded,
    /// SIMD-filtered and WebP-encoded regardless of this flag — only the
    /// physical disk write is sampled, so benchmark numbers always reflect the
    /// true cost of processing every single message.
    /// </summary>
    public bool SaveSampleToDisk { get; set; } = true;

    public int SampleEveryNthMessage { get; set; } = 500;
}