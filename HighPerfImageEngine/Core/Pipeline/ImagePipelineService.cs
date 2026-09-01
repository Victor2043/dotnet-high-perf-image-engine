using System.Diagnostics;
using System.Runtime.Intrinsics.X86;
using System.Runtime.Intrinsics;
using SkiaSharp;
using Spectre.Console;

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

                    ImageFormat detectedFormat = DetectImageFormat(headerBuffer);
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
                    ApplyBrightnessSimdRgbOnly(pixelSpan, brightnessOffset: 50);
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

            // ============================================================================
            // 3. LOW-LEVEL METHODS (CORE LOGIC)
            // ============================================================================

            static ImageFormat DetectImageFormat(ReadOnlySpan<byte> header)
            {
                if (header.Length < 12) return ImageFormat.Unknown;

                ReadOnlySpan<byte> jpeg = [0xFF, 0xD8, 0xFF];
                ReadOnlySpan<byte> png = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
                ReadOnlySpan<byte> riff = [0x52, 0x49, 0x46, 0x46];
                ReadOnlySpan<byte> webp = [0x57, 0x45, 0x42, 0x50];

                if (header.StartsWith(png)) return ImageFormat.Png;
                if (header.StartsWith(jpeg)) return ImageFormat.Jpeg;
                if (header.Slice(0, 4).SequenceEqual(riff) && header.Slice(8, 4).SequenceEqual(webp)) return ImageFormat.Webp;

                return ImageFormat.Unknown;
            }

            /// <summary>
            /// Applies brightness only to RGB channels of an RGBA/BGRA pixel buffer (4 bytes per pixel)
            /// </summary>
            static void ApplyBrightnessSimdRgbOnly(Span<byte> data, byte brightnessOffset)
            {
                int i = 0;

                if (Avx2.IsSupported && data.Length >= Vector256<byte>.Count)
                {
                    // 1. Fill mask on the stack (32 bytes)
                    Span<byte> maskSpan = stackalloc byte[32];
                    for (int b = 0; b < 32; b++)
                    {
                        maskSpan[b] = (b % 4 == 3) ? (byte)0 : brightnessOffset;
                    }

                    // 2. Explicit cast to ReadOnlySpan<byte> prevents compiler ambiguity
                    ReadOnlySpan<byte> maskReadOnly = maskSpan;
                    Vector256<byte> brightnessVector = Vector256.Create(maskReadOnly);

                    int vectorSize = Vector256<byte>.Count;
                    int loopLimit = data.Length - (data.Length % vectorSize);

                    for (; i < loopLimit; i += vectorSize)
                    {
                        ReadOnlySpan<byte> readBlock = data.Slice(i, vectorSize);
                        Vector256<byte> pixels = Vector256.Create(readBlock);

                        Vector256<byte> result = Avx2.AddSaturate(pixels, brightnessVector);

                        Span<byte> writeBlock = data.Slice(i, vectorSize);
                        result.CopyTo(writeBlock);
                    }
                }

                // Scalar fallback processing for remaining buffer
                for (; i < data.Length; i++)
                {
                    if (i % 4 != 3) // Ignore Alpha channel
                    {
                        int sum = data[i] + brightnessOffset;
                        data[i] = sum > 255 ? (byte)255 : (byte)sum;
                    }
                }
            }
        }
    }
}

enum ImageFormat
{
    Unknown,
    Jpeg,
    Png,
    Webp
}