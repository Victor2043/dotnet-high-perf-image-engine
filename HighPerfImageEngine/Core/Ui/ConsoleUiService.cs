using System.Diagnostics;
using System.Runtime.Intrinsics.X86;
using HighPerfImageEngine.Domain.Enums;
using SkiaSharp;
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

        public static void LogInfo(string message)
        {
            AnsiConsole.MarkupLine($"[grey] {message}");
        }

        public static void LogSuccess(string message)
        {
            AnsiConsole.MarkupLine($"[bold green] {message}");
        }

        public static void LogWarning(string message)
        {
            AnsiConsole.MarkupLine($"[bold yellow] {message}");
        }

        public static void LogError(string message)
        {
            AnsiConsole.MarkupLine($"[bold red] {message}");
        }

        internal static void RenderResultTable(string fileName, string filePath, ImageFormat detectedFormat, SKBitmap bitmap, FileInfo outputInfo, Stopwatch swSimd, Stopwatch swTotal, long bytesAllocated)
        {
            var table = new Table()
                       .Border(TableBorder.Rounded)
                       .BorderColor(Color.Grey)
                       .Title($"[bold green]RESULT: {fileName} -> WebP[/]")
                       .AddColumn(new TableColumn("[bold cyan]Metric[/]").LeftAligned())
                       .AddColumn(new TableColumn("[bold cyan]Value[/]").RightAligned());

            table.AddRow("Input File", Path.GetFileName(filePath));
            table.AddRow("Detected Format", detectedFormat.ToString());
            table.AddRow("Resolution", $"{bitmap.Width} x {bitmap.Height} px");
            table.AddRow("Final Size (WebP)", $"{outputInfo.Length / 1024.0:N1} KB");
            table.AddRow("SIMD Kernel Time", $"[bold green]{swSimd.Elapsed.TotalMicroseconds:N2} µs[/]");
            table.AddRow("Total Pipeline Time", $"{swTotal.Elapsed.TotalMilliseconds:N2} ms");
            table.AddRow("GC Allocation", bytesAllocated < 2000 ? "[bold green]Near Zero[/]" : $"{bytesAllocated:N0} Bytes");

            AnsiConsole.Write(table);
            AnsiConsole.WriteLine();
        }
    }
}
