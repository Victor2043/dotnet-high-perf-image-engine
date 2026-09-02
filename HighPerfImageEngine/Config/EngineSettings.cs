namespace HighPerfImageEngine.Config;

public class EngineSettings
{
    public LoggingSettings Logging { get; set; } = new();
    public RabbitMqSettings RabbitMq { get; set; } = new();
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