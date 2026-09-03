using System.Diagnostics;
using System.Threading.Channels;
using HighPerfImageEngine.Core.Benchmark;
using RabbitMQ.Client;

namespace HighPerfImageEngine.Core.Messaging;

/// <summary>
/// Serializes every Ack/Nack call onto a single dedicated background task
/// instead of gating multiple worker threads behind a SemaphoreSlim. Callers
/// only ever *enqueue* here (a fast, effectively lock-free MPSC write); this
/// class is the ONLY thing that ever touches the underlying IChannel for
/// Ack/Nack, so there's no contention and no risk of a lock-convoy forming
/// at some intermediate concurrency level.
/// </summary>
public sealed class AckNackDispatcher
{
    private sealed record AckRequest(ulong DeliveryTag, bool Ack, bool Requeue);

    private readonly IChannel _channel;
    private readonly BenchmarkMetrics _metrics;
    private readonly Channel<AckRequest> _ackChannel;
    private readonly Task _loop;

    public AckNackDispatcher(IChannel channel, BenchmarkMetrics metrics)
    {
        _channel = channel;
        _metrics = metrics;

        _ackChannel = Channel.CreateUnbounded<AckRequest>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

        _loop = Task.Run(RunLoopAsync);
    }

    public Task AckAsync(ulong deliveryTag) =>
        _ackChannel.Writer.WriteAsync(new AckRequest(deliveryTag, Ack: true, Requeue: false)).AsTask();

    public Task NackAsync(ulong deliveryTag, bool requeue) =>
        _ackChannel.Writer.WriteAsync(new AckRequest(deliveryTag, Ack: false, requeue)).AsTask();

    /// <summary>
    /// Stops accepting new requests and waits for every already-queued
    /// Ack/Nack to actually reach the broker. Call this before closing the
    /// connection, or messages could still be "in flight" when it closes,
    /// and the broker would requeue them.
    /// </summary>
    public async Task CompleteAndDrainAsync()
    {
        _ackChannel.Writer.TryComplete();
        await _loop;
    }

    private async Task RunLoopAsync()
    {
        await foreach (AckRequest req in _ackChannel.Reader.ReadAllAsync())
        {
            long start = Stopwatch.GetTimestamp();

            if (req.Ack)
            {
                await _channel.BasicAckAsync(deliveryTag: req.DeliveryTag, multiple: false);
            }
            else
            {
                await _channel.BasicNackAsync(deliveryTag: req.DeliveryTag, multiple: false, requeue: req.Requeue);
            }

            _metrics.RecordAckPath(Stopwatch.GetTimestamp() - start);
        }
    }
}