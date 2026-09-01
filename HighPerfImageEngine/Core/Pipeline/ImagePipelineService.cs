using System.Diagnostics;
using System.Runtime.Intrinsics.X86;
using System.Runtime.Intrinsics;
using SkiaSharp;
using Spectre.Console;
using HighPerfImageEngine.Domain.Enums;
using HighPerfImageEngine.Core.Processing;

namespace HighPerfImageEngine.Core.Pipeline
{
    public class ImagePipelineService
    {
        public void ProcessImage(string inputDir, string outputDir)
        {                       

          
            string[] imageExtensions = [".jpg", ".jpeg", ".png", ".webp"];
            string[] inputFiles = Directory.GetFiles(inputDir)
                .Where(f => imageExtensions.Contains(Path.GetExtension(f).ToLowerInvariant()))
                .ToArray();

            if (inputFiles.Length == 0)
            {
                AnsiConsole.MarkupLine($"[bold red]No images found in directory:[/] {inputDir}");
                AnsiConsole.MarkupLine("[grey]Add images (.jpg, .png, .webp) to this directory and run again.[/]");
                return;
            }

            AnsiConsole.MarkupLine($"[bold white]Files to process:[/] [green]{inputFiles.Length}[/]");
            AnsiConsole.MarkupLine($"[bold white]SIMD Hardware Support (AVX2):[/] {(Avx2.IsSupported ? "[bold green]YES (256-bit Vectorization)[/]" : "[bold red]NO (Scalar Fallback)[/]")}\n");

            // ============================================================================
            // 2. FILE-BY-FILE PROCESSING
            // ============================================================================
            foreach (string filePath in inputFiles)
            {
                string fileName = Path.GetFileNameWithoutExtension(filePath);
                string outputPath = Path.Combine(outputDir, $"processed_{fileName}.webp");

                long initialMemory = GC.GetAllocatedBytesForCurrentThread();

                var swTotal = Stopwatch.StartNew();
                var swSimd = new Stopwatch();

                try
                {
                    // 1. SAFETY VALIDATION AND SANITIZATION (MAGIC NUMBERS VIA SPAN)
                    Span<byte> headerBuffer = stackalloc byte[12];
                    using (var fs = File.OpenRead(filePath))
                    {
                        int bytesRead = fs.Read(headerBuffer);
                        if (bytesRead < 12)
                        {
                            AnsiConsole.MarkupLine($"[bold red]File corrupted or too small:[/] {fileName}");
                            continue;
                        }
                    }

                    ImageFormat detectedFormat = ImageFormatDetector.DetectImageFormat(headerBuffer);
                    if (detectedFormat == ImageFormat.Unknown)
                    {
                        AnsiConsole.MarkupLine($"[bold red]Invalid signature (Unrecognized Magic Numbers):[/] {fileName}");
                        continue;
                    }

                    // 2. LOAD AND DECODE IMAGE
                    using var originalBitmap = SKBitmap.Decode(filePath);
                    if (originalBitmap == null)
                    {
                        AnsiConsole.MarkupLine($"[bold red]Failed to decode image:[/] {fileName}");
                        continue;
                    }

                    // Standardize color layout to BGRA8888 (4 bytes per pixel)
                    using var bitmap = originalBitmap.ColorType == SKColorType.Bgra8888
                        ? originalBitmap
                        : originalBitmap.Copy(SKColorType.Bgra8888);

                    // 3. GET DIRECT SPAN FROM SKIA MEMORY
                    Span<byte> pixelSpan;
                    unsafe
                    {
                        // Get native pixel pointer and create a writable Span without Heap allocation
                        byte* ptr = (byte*)bitmap.GetPixels().ToPointer();
                        int byteCount = bitmap.ByteCount;
                        pixelSpan = new Span<byte>(ptr, byteCount);
                    }

                    // 4. SIMD AVX2 KERNEL
                    swSimd.Start();
                    SimdBrightnessEngine.ApplyBrightnessSimdRgbOnly(pixelSpan, brightnessOffset: 50);
                    swSimd.Stop();

                    // 5. ENCODE AND SAVE TO WEBP (STORAGE OPTIMIZATION AND METADATA STRIPPING)
                    using var image = SKImage.FromBitmap(bitmap);
                    using var data = image.Encode(SKEncodedImageFormat.Webp, 80);
                    using var outputStream = File.Create(outputPath);
                    data.SaveTo(outputStream);

                    swTotal.Stop();

                    long finalMemory = GC.GetAllocatedBytesForCurrentThread();
                    long bytesAllocated = finalMemory - initialMemory;
                    FileInfo outputInfo = new FileInfo(outputPath);

                    // Dashboard
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
                catch (Exception ex)
                {
                    AnsiConsole.MarkupLine($"[bold red]Error processing {fileName}:[/] {ex.Message}");
                }
            }                     
        }
    }
}