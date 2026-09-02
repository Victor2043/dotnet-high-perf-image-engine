using System.Diagnostics;
using HighPerfImageEngine.Core.Processing;
using HighPerfImageEngine.Domain.Enums;
using SkiaSharp;

namespace HighPerfImageEngine.Core.Pipeline;

public record ProcessResult(
    string FileName,
    ImageFormat DetectedFormat,
    int Width,
    int Height,
    long OutputSizeBytes,
    double SimdMicroseconds,
    double TotalMilliseconds,
    long AllocatedBytes,
    bool SkippedDiskWrite // Flag for metric transparency
);

public class ImagePipelineService
{
    public bool ProcessImageFromBytes(byte[] imageBytes, string fileName, string outputPath, byte brightnessOffset, out ProcessResult? result)
    {
        result = null;
        long initialMemory = GC.GetAllocatedBytesForCurrentThread();

        var swTotal = Stopwatch.StartNew();
        var swSimd = new Stopwatch();

        // 1. Payload Sanity Check (Magic Numbers)
        if (imageBytes == null || imageBytes.Length < 12) return false;

        ReadOnlySpan<byte> headerBuffer = imageBytes.AsSpan(0, 12);
        ImageFormat detectedFormat = ImageFormatDetector.DetectImageFormat(headerBuffer);
        if (detectedFormat == ImageFormat.Unknown) return false;

        // 2. In-memory decoding via SkiaSharp (ALWAYS EXECUTES)
        using var originalBitmap = SKBitmap.Decode(imageBytes);
        if (originalBitmap == null) return false;

        SKBitmap bitmapToProcess = originalBitmap;
        bool isCopy = false;

        if (originalBitmap.ColorType != SKColorType.Bgra8888)
        {
            bitmapToProcess = originalBitmap.Copy(SKColorType.Bgra8888);
            if (bitmapToProcess == null) return false;
            isCopy = true;
        }

        try
        {
            // 3. Execution of the SIMD Kernel on the pixel Span (ALWAYS EXECUTES)
            Span<byte> pixelSpan;
            unsafe
            {
                byte* ptr = (byte*)bitmapToProcess.GetPixels().ToPointer();
                int byteCount = bitmapToProcess.ByteCount;
                pixelSpan = new Span<byte>(ptr, byteCount);
            }

            swSimd.Start();
            SimdBrightnessEngine.ApplyBrightnessSimdRgbOnly(pixelSpan, brightnessOffset);
            swSimd.Stop();

            // 4. Persistence Guard (Processes WebP, but skips writing to disk if it already exists)
            bool fileAlreadyExists = File.Exists(outputPath);
            long outputSize = 0;

            if (!fileAlreadyExists)
            {
                using var image = SKImage.FromBitmap(bitmapToProcess);
                using var data = image.Encode(SKEncodedImageFormat.Webp, 80);

                using (var outputStream = File.Create(outputPath))
                {
                    data.SaveTo(outputStream);
                }
                outputSize = data.Size;
            }
            else
            {
                outputSize = new FileInfo(outputPath).Length;
            }

            swTotal.Stop();

            long finalMemory = GC.GetAllocatedBytesForCurrentThread();

            result = new ProcessResult(
                FileName: fileName,
                DetectedFormat: detectedFormat,
                Width: bitmapToProcess.Width,
                Height: bitmapToProcess.Height,
                OutputSizeBytes: outputSize,
                SimdMicroseconds: swSimd.Elapsed.TotalMicroseconds,
                TotalMilliseconds: swTotal.Elapsed.TotalMilliseconds,
                AllocatedBytes: Math.Max(0, finalMemory - initialMemory),
                SkippedDiskWrite: fileAlreadyExists
            );

            return true;
        }
        finally
        {
            if (isCopy)
            {
                bitmapToProcess.Dispose();
            }
        }
    }
}