using HighPerfImageEngine.Config;
using HighPerfImageEngine.Core.Ui;
using RabbitMQ.Client;

namespace HighPerfImageEngine.Core.Messaging;

public static class RabbitMqConnectionFactory
{
    public static async Task<(IConnection Connection, IChannel Channel)?> CreateConnectionAndChannelAsync(EngineSettings settings)
    {
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
                {
                    ConsoleUiService.LogInfo($"Attempting to connect to RabbitMQ (attempt {attempt}/10)...");
                }

                connection = await factory.CreateConnectionAsync();
                channel = await connection.CreateChannelAsync();

                if (settings.Logging.EnableUiLogs)
                {
                    ConsoleUiService.LogSuccess("Successfully connected to RabbitMQ!");
                }

                return (connection, channel);
            }
            catch (Exception ex)
            {
                if (settings.Logging.EnableUiLogs)
                {
                    ConsoleUiService.LogWarning($"Broker unreachable: {ex.Message}. Waiting 3s...");
                }

                await Task.Delay(3000);
            }
        }

        ConsoleUiService.LogError("Could not establish connection to RabbitMQ. Terminating.");
        return null;
    }
}