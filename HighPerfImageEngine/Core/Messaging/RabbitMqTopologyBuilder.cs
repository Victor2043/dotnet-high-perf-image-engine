using HighPerfImageEngine.Config;
using RabbitMQ.Client;

namespace HighPerfImageEngine.Core.Messaging;

public static class RabbitMqTopologyBuilder
{
    public static async Task DeclareTopologyAsync(IChannel channel, RabbitMqSettings settings)
    {
        await channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: settings.PrefetchCount,
            global: false);

        // Dead-letter exchange e queue
        await channel.ExchangeDeclareAsync(
            exchange: settings.DlxExchange,
            type: ExchangeType.Direct,
            durable: true);

        await channel.QueueDeclareAsync(
            queue: settings.DlqQueue,
            durable: true,
            exclusive: false,
            autoDelete: false);

        await channel.QueueBindAsync(
            queue: settings.DlqQueue,
            exchange: settings.DlxExchange,
            routingKey: settings.DlqRoutingKey);

        // Main queue topology
        var mainQueueArgs = new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", settings.DlxExchange },
            { "x-dead-letter-routing-key", settings.DlqRoutingKey }
        };

        await channel.ExchangeDeclareAsync(
            exchange: settings.MainExchange,
            type: ExchangeType.Direct,
            durable: true);

        await channel.QueueDeclareAsync(
            queue: settings.MainQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: mainQueueArgs);

        await channel.QueueBindAsync(
            queue: settings.MainQueue,
            exchange: settings.MainExchange,
            routingKey: settings.MainRoutingKey);
    }
}