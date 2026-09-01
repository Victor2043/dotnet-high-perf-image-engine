using System.Diagnostics;
using System.Runtime.Intrinsics.X86;
using SkiaSharp;
using HighPerfImageEngine.Domain.Enums;
using HighPerfImageEngine.Core.Processing;
using HighPerfImageEngine.Core.Ui;

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
                ConsoleUiService.LogError($"No images found in directory:[/] {inputDir}");
                ConsoleUiService.LogError("Add images (.jpg, .png, .webp) to this directory and run again.[/]");
                return;
            }

            ConsoleUiService.LogInfo($"Files to process: {inputFiles.Length}[/]");
            ConsoleUiService.LogInfo($"SIMD Hardware Support (AVX2):[/] {(Avx2.IsSupported ? "[bold green]YES (256-bit Vectorization)[/]" : "NO (Scalar Fallback)[/]")}\n");

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
                            ConsoleUiService.LogError($"File corrupted or too small:[/] {fileName}");
                            continue;
                        }
                    }

                    ImageFormat detectedFormat = ImageFormatDetector.DetectImageFormat(headerBuffer);
                    if (detectedFormat == ImageFormat.Unknown)
                    {
                        ConsoleUiService.LogError($"Invalid signature (Unrecognized Magic Numbers):[/] {fileName}");
                        continue;
                    }

                    // 2. LOAD AND DECODE IMAGE
                    using var originalBitmap = SKBitmap.Decode(filePath);
                    if (originalBitmap == null)
                    {
                        ConsoleUiService.LogError($"Failed to decode image:[/] {fileName}");
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
                    ConsoleUiService.RenderResultTable(fileName, filePath, detectedFormat, bitmap, outputInfo, swSimd, swTotal, bytesAllocated);
                }
                catch (Exception ex)
                {
                    ConsoleUiService.LogError($"Error processing {fileName}:[/] {ex.Message}");
                }
            }                     
        }
    }
}