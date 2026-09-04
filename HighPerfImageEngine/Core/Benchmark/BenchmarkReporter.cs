using HighPerfImageEngine.Core.Ui;

namespace HighPerfImageEngine.Core.Benchmark;

/// <summary>
/// Renders the final benchmark report exactly once per run, no matter how
/// many code paths (natural batch completion, Ctrl+C, ProcessExit) try to
/// trigger it.
/// </summary>
public static class BenchmarkReporter
{
    private static int _rendered;

    public static void RenderOnce(BenchmarkMetrics metrics)
    {
        if (Interlocked.Exchange(ref _rendered, 1) != 0)
        {
            return;
        }

        metrics.StopTimer();

        BenchmarkSnapshot snapshot = metrics.Snapshot();

        if (snapshot.Expected > 0)
        {
            ConsoleUiService.LogInfo(
                $"Batch completed. " +
                $"Expected: {snapshot.Expected:N0} | " +
                $"Processed: {snapshot.Processed:N0} | " +
                $"Failed: {snapshot.Failed:N0}");
        }

        if (!string.IsNullOrWhiteSpace(snapshot.BatchId))
        {
            ConsoleUiService.LogInfo($"Batch ID: {snapshot.BatchId}");
        }

        if (snapshot.Expected > 0 &&
            snapshot.Processed + snapshot.Failed != snapshot.Expected)
        {
            ConsoleUiService.LogWarning(
                $"Batch count mismatch! " +
                $"Expected {snapshot.Expected:N0}, but processed " +
                $"{snapshot.Processed:N0} and failed {snapshot.Failed:N0}.");

            ConsoleUiService.LogWarning(
                $"Diagnostic: RabbitMQ.Client actually delivered " +
                $"{snapshot.ReceivedDeliveries:N0} messages to ReceivedAsync " +
                $"(including the batch marker itself). If this is close to " +
                $"Expected, the bug is in OUR completion logic. If it's much " +
                $"lower, messages are being lost before they even reach us.");
        }

        ConsoleUiService.RenderResultTable(
            snapshot.Processed,
            snapshot.ElapsedSeconds,
            snapshot.TotalAllocatedBytes,
            snapshot.AvgPipelineMs,
            snapshot.AvgAckMs,
            snapshot.AvgChannelWaitMs);
    }
}