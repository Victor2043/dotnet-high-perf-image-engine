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
    bool WrittenToDisk // Whether this particular result was sampled to disk
);

public class ImagePipelineService
{
    /// <summary>
    /// Decodes, brightness-filters (SIMD) and WebP-encodes an image.
    ///
    /// `writeToDisk` controls ONLY whether the encoded bytes are persisted —
    /// decode + SIMD + encode ALWAYS run in full for every message. This is
    /// what makes the benchmark numbers honest: previously, once a given
    /// filename had been written once, every subsequent message for that same
    /// filename skipped the entire WebP encode step (the actual workload being
    /// measured), silently inflating throughput over the course of a run.
    /// </summary>
    public bool ProcessImageFromBytes(
        ReadOnlySpan<byte> imageBytes,
        string fileName,
        string outputPath,
        byte brightnessOffset,
        bool writeToDisk,
        out ProcessResult? result)
    {
        result = null;
        long initialMemory = GC.GetAllocatedBytesForCurrentThread();

        var swTotal = Stopwatch.StartNew();
        var swSimd = new Stopwatch();

        // 1. Payload Sanity Check (Magic Numbers)
        if (imageBytes.Length < 12) return false;

        ImageFormat detectedFormat = ImageFormatDetector.DetectImageFormat(imageBytes[..12]);
        if (detectedFormat == ImageFormat.Unknown) return false;

        // 2. In-memory decoding via SkiaSharp.
        // SKBitmap.Decode(ReadOnlySpan<byte>) copies the span straight into
        // memory Skia owns natively — that copy does not land on the managed
        // .NET heap, so it does not add to GC pressure the way a managed
        // byte[] copy would, and we skip the extra SKData wrapping step.
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

            // 4. Encoding ALWAYS executes — this is the real workload.
            using var image = SKImage.FromBitmap(bitmapToProcess);
            using var data = image.Encode(SKEncodedImageFormat.Webp, 80);

            long outputSize = data.Size;

            if (writeToDisk)
            {              
                using var outputStream = new FileStream(
                    outputPath,
                    FileMode.Create,
                    FileAccess.Write,
                    FileShare.None,
                    bufferSize: 4096,
                    useAsync: false);

                data.SaveTo(outputStream);
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
                WrittenToDisk: writeToDisk
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