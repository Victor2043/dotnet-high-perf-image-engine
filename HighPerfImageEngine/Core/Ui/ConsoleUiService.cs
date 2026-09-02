using System.Runtime.Intrinsics.X86;
using HighPerfImageEngine.Core.Pipeline;
using Spectre.Console;

namespace HighPerfImageEngine.Core.Ui
{
    static class ConsoleUiService
    {
        public static void RenderBanner()
        {
            AnsiConsole.Write(
                new FigletText(".NET High-Perf Engine")
                    .LeftJustified()
                    .Color(Color.Cyan1));

            AnsiConsole.MarkupLine("[bold yellow]Iniciando Worker Consumidor (RabbitMQ + SkiaSharp + SIMD)...[/]\n");
            AnsiConsole.MarkupLine($"[bold white]Suporte a Hardware SIMD (AVX2):[/] {(Avx2.IsSupported ? "[bold green]SIM (256-bit Vectorization)[/]" : "[bold red]NÃO (Fallback Escalar)[/]")}\n");
        }

        public static void LogInfo(string message) => AnsiConsole.MarkupLine($"[grey]{Markup.Escape(message)}[/]");
        public static void LogSuccess(string message) => AnsiConsole.MarkupLine($"[bold green]{Markup.Escape(message)}[/]");
        public static void LogWarning(string message) => AnsiConsole.MarkupLine($"[bold yellow]{Markup.Escape(message)}[/]");
        public static void LogError(string message) => AnsiConsole.MarkupLine($"[bold red]{Markup.Escape(message)}[/]");

        internal static void RenderResultTableByImage(
            ProcessResult result,
            long currentAllocatedBytes,
            double globalTotalSeconds)
        
            {
            var table = new Table()
                .Border(TableBorder.Rounded)
                .BorderColor(Color.Grey)
                .Title($"[bold green]PROCESSED: {result.FileName} -> WebP[/]")
                .AddColumn(new TableColumn("[bold cyan]Metric[/]").LeftAligned())
                .AddColumn(new TableColumn("[bold cyan]Value[/]").RightAligned());

            table.AddRow("Original File", result.FileName);
            table.AddRow("Detected Format", result.DetectedFormat.ToString());
            table.AddRow("Resolution", $"{result.Width} x {result.Height} px");
            table.AddRow("Final Size (WebP)", $"{result.OutputSizeBytes / 1024.0:N1} KB");
            table.AddRow("SIMD Kernel Time", $"[bold green]{result.SimdMicroseconds:N2} µs[/]");
            table.AddRow("Total Pipeline Time", $"{result.TotalMilliseconds:N2} ms");
            table.AddRow("GC Allocation", $"{result.AllocatedBytes:N0} Bytes");
            table.AddRow("[bold yellow]Global Elapsed Time[/]", $"[bold yellow]{globalTotalSeconds:N2} s[/]");

            AnsiConsole.Write(table);
            AnsiConsole.WriteLine();
        }

        public static void RenderResultTable(
            long totalProcessedMessages,
            double globalTotalSeconds,
            long globalTotalAllocatedBytes)
        {
            double msgsPerSecond = globalTotalSeconds > 0 ? totalProcessedMessages / globalTotalSeconds : 0;
            double msgsPerMinute = msgsPerSecond * 60;
            double avgTimePerMsgMs = totalProcessedMessages > 0 ? (globalTotalSeconds * 1000.0) / totalProcessedMessages : 0;
            double avgAllocPerMsgKb = totalProcessedMessages > 0 ? (globalTotalAllocatedBytes / 1024.0) / totalProcessedMessages : 0;

            // GC metrics
            int gen0Collections = GC.CollectionCount(0);
            int gen1Collections = GC.CollectionCount(1);
            int gen2Collections = GC.CollectionCount(2);
            long totalMemoryHeapMB = GC.GetTotalMemory(forceFullCollection: false) / (1024 * 1024);

            var table = new Table()
                .Border(TableBorder.DoubleEdge)
                .BorderColor(Color.Cyan1)
                .Title("[bold green]=== EXECUTION SUMMARY & BENCHMARK REPORT ===[/]")
                .AddColumn(new TableColumn("[bold cyan]Category[/]").LeftAligned())
                .AddColumn(new TableColumn("[bold cyan]Metric[/]").LeftAligned())
                .AddColumn(new TableColumn("[bold cyan]Value[/]").RightAligned());

            // Throughput & Processing
            table.AddRow("[bold yellow]Throughput[/]", "Total Messages Processed", $"[bold green]{totalProcessedMessages:N0}[/]");
            table.AddRow("[bold yellow]Throughput[/]", "Messages / Second", $"[bold green]{msgsPerSecond:N2} msg/s[/]");
            table.AddRow("[bold yellow]Throughput[/]", "Messages / Minute", $"[bold green]{msgsPerMinute:N0} msg/min[/]");
            table.AddRow("[bold yellow]Performance[/]", "Global Elapsed Time", $"{globalTotalSeconds:N2} s");
            table.AddRow("[bold yellow]Performance[/]", "Avg Latency per Msg", $"{avgTimePerMsgMs:N2} ms");

            table.AddEmptyRow();

            // Memory & GC
            table.AddRow("[bold magenta]Memory (GC)[/]", "Total Allocated Bytes", $"{globalTotalAllocatedBytes / (1024.0 * 1024.0):N2} MB");
            table.AddRow("[bold magenta]Memory (GC)[/]", "Avg Allocation per Msg", $"{avgAllocPerMsgKb:N2} KB");
            table.AddRow("[bold magenta]Memory (GC)[/]", "Gen 0 Collections", $"{gen0Collections}");
            table.AddRow("[bold magenta]Memory (GC)[/]", "Gen 1 Collections", $"{gen1Collections}");
            table.AddRow("[bold magenta]Memory (GC)[/]", "Gen 2 Collections", $"{gen2Collections}");
            table.AddRow("[bold magenta]Memory (GC)[/]", "Current Live Heap", $"{totalMemoryHeapMB:N2} MB");

            AnsiConsole.WriteLine();
            AnsiConsole.Write(table);
            AnsiConsole.WriteLine();
        }
    }
}
