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

        internal static void RenderResultTable(
            ProcessResult result,
            long currentAllocatedBytes,
            double globalTotalSeconds,
            long globalTotalAllocatedBytes)
        
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
            table.AddRow("[bold yellow]Total Allocated (GC)[/]", $"[bold yellow]{globalTotalAllocatedBytes / (1024.0 * 1024.0):N2} MB[/]");

            AnsiConsole.Write(table);
            AnsiConsole.WriteLine();
        }
    }
}
